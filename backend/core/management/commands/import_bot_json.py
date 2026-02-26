import json
from pathlib import Path

from django.core.management.base import BaseCommand
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from core.models import AdItem, TelegramUser, UserAction


def _parse_dt(value):
    if not value:
        return None
    parsed = parse_datetime(str(value))
    if not parsed:
        return None
    if timezone.is_naive(parsed):
        return timezone.make_aware(parsed, timezone.get_current_timezone())
    return parsed


def _normalize_role(role: str | None) -> str:
    normalized = str(role or TelegramUser.ROLE_USER).strip().lower()
    if normalized in {"leasing", "leasing_company", "лизинговая", "лизинговая компания", "лизинговая_компания"}:
        return TelegramUser.ROLE_LEASING_COMPANY
    if normalized in {"admin", "админ", "администратор"}:
        return TelegramUser.ROLE_ADMIN
    return TelegramUser.ROLE_USER


class Command(BaseCommand):
    help = "Импортирует auth_users.json, users_log.json и ads_feed.json в Django БД"

    def add_arguments(self, parser):
        parser.add_argument(
            "--project-root",
            default="..",
            help="Путь к папке, где лежат bot.py и JSON-файлы (по умолчанию: ../)",
        )

    def handle(self, *args, **options):
        project_root = Path(options["project_root"]).resolve()

        auth_file = project_root / "auth_users.json"
        logs_file = project_root / "users_log.json"
        ads_file = project_root / "ads_feed.json"

        self.stdout.write(f"Project root: {project_root}")

        users_count = self._import_users(auth_file)
        actions_count = self._import_actions(logs_file)
        ads_count = self._import_ads(ads_file)

        self.stdout.write(self.style.SUCCESS(f"Импорт завершен: users={users_count}, actions={actions_count}, ads={ads_count}"))

    def _import_users(self, path: Path) -> int:
        if not path.exists():
            self.stdout.write(self.style.WARNING(f"Пропущено: {path.name} не найден"))
            return 0

        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            self.stdout.write(self.style.WARNING(f"Пропущено: {path.name} имеет неверный формат"))
            return 0

        count = 0
        for raw_user in data.values():
            if not isinstance(raw_user, dict):
                continue
            telegram_id = raw_user.get("telegram_id")
            if not telegram_id:
                continue
            try:
                telegram_id = int(telegram_id)
            except Exception:
                continue

            TelegramUser.objects.update_or_create(
                telegram_id=telegram_id,
                defaults={
                    "username": str(raw_user.get("username") or ""),
                    "first_name": str(raw_user.get("first_name") or ""),
                    "last_name": str(raw_user.get("last_name") or ""),
                    "language_code": str(raw_user.get("language_code") or ""),
                    "phone_number": str(raw_user.get("phone_number") or ""),
                    "avatar_file_id": str(raw_user.get("avatar_file_id") or ""),
                    "role": _normalize_role(raw_user.get("role")),
                    "is_authenticated": bool(raw_user.get("is_authenticated", False)),
                    "authenticated_at": _parse_dt(raw_user.get("authenticated_at")),
                },
            )
            count += 1

        self.stdout.write(self.style.SUCCESS(f"Импортировано пользователей: {count}"))
        return count

    def _import_actions(self, path: Path) -> int:
        if not path.exists():
            self.stdout.write(self.style.WARNING(f"Пропущено: {path.name} не найден"))
            return 0

        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            self.stdout.write(self.style.WARNING(f"Пропущено: {path.name} имеет неверный формат"))
            return 0

        count = 0
        for event in data:
            if not isinstance(event, dict):
                continue
            telegram_id = event.get("user_id")
            action = str(event.get("action") or "").strip()
            if not telegram_id or not action:
                continue

            try:
                telegram_id = int(telegram_id)
            except Exception:
                continue

            user = TelegramUser.objects.filter(telegram_id=telegram_id).first()
            UserAction.objects.create(
                user=user,
                telegram_id=telegram_id,
                username=str(event.get("username") or ""),
                first_name=str(event.get("first_name") or ""),
                last_name=str(event.get("last_name") or ""),
                action=action,
                details=str(event.get("details") or ""),
                raw_payload=event,
            )
            count += 1

        self.stdout.write(self.style.SUCCESS(f"Импортировано действий: {count}"))
        return count

    def _import_ads(self, path: Path) -> int:
        if not path.exists():
            self.stdout.write(self.style.WARNING(f"Пропущено: {path.name} не найден"))
            return 0

        root = json.loads(path.read_text(encoding="utf-8"))
        items = root.get("items") if isinstance(root, dict) else root
        if not isinstance(items, list):
            self.stdout.write(self.style.WARNING(f"Пропущено: {path.name} имеет неверный формат"))
            return 0

        count = 0
        for item in items:
            if not isinstance(item, dict):
                continue
            ad_id = str(item.get("id") or "").strip()
            title = str(item.get("title") or "").strip()
            if not ad_id or not title:
                continue

            author = item.get("author") if isinstance(item.get("author"), dict) else {}
            source_type = str(item.get("source_type") or "manual").strip().lower()
            if source_type not in {AdItem.SOURCE_EXCEL, AdItem.SOURCE_MANUAL}:
                source_type = AdItem.SOURCE_MANUAL

            status = str(item.get("status") or AdItem.STATUS_ACTIVE).strip().lower()
            if status not in {AdItem.STATUS_ACTIVE, AdItem.STATUS_INACTIVE, AdItem.STATUS_ARCHIVED}:
                status = AdItem.STATUS_ACTIVE

            try:
                price = int(item.get("price") or 0)
            except Exception:
                price = 0

            year = item.get("year")
            try:
                year = int(year) if year is not None and str(year).strip() else None
            except Exception:
                year = None

            author_id = author.get("id")
            try:
                author_id = int(author_id) if author_id is not None and str(author_id).strip() else None
            except Exception:
                author_id = None

            AdItem.objects.update_or_create(
                ad_id=ad_id,
                defaults={
                    "source_type": source_type,
                    "external_id": str(item.get("external_id") or ""),
                    "title": title,
                    "category": str(item.get("category") or ""),
                    "price": price,
                    "year": year,
                    "details": str(item.get("details") or ""),
                    "location": str(item.get("location") or ""),
                    "image": str(item.get("image") or ""),
                    "status": status,
                    "author_telegram_id": author_id,
                    "author_username": str(author.get("username") or ""),
                    "author_first_name": str(author.get("first_name") or ""),
                    "author_last_name": str(author.get("last_name") or ""),
                    "created_at_remote": _parse_dt(item.get("createdAt")),
                    "raw_payload": item,
                },
            )
            count += 1

        self.stdout.write(self.style.SUCCESS(f"Импортировано объявлений: {count}"))
        return count
