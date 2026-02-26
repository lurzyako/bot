import json
from functools import wraps

from django.conf import settings
from django.db import transaction
from django.http import HttpRequest, JsonResponse
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.views.decorators.csrf import csrf_exempt

from .models import AdItem, TelegramUser, UserAction


def normalize_role(role: str | None) -> str:
    normalized = str(role or TelegramUser.ROLE_USER).strip().lower()
    if normalized in {
        "leasing",
        "leasing_company",
        "лизинговая",
        "лизинговая компания",
        "лизинговая_компания",
    }:
        return TelegramUser.ROLE_LEASING_COMPANY
    if normalized in {"admin", "админ", "администратор"}:
        return TelegramUser.ROLE_ADMIN
    return TelegramUser.ROLE_USER


def parse_iso_datetime(value):
    if not value:
        return None
    parsed = parse_datetime(str(value))
    if not parsed:
        return None
    if timezone.is_naive(parsed):
        parsed = timezone.make_aware(parsed, timezone.get_current_timezone())
    return parsed


def parse_json_request(request: HttpRequest) -> dict:
    try:
        return json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return {}


def require_bot_api_key(view_func):
    @wraps(view_func)
    def wrapper(request: HttpRequest, *args, **kwargs):
        expected_key = settings.BOT_API_KEY
        if not expected_key:
            return JsonResponse(
                {"ok": False, "detail": "DJANGO_BOT_API_KEY is not configured"},
                status=500,
            )

        provided_key = request.headers.get("X-API-Key") or request.META.get("HTTP_X_API_KEY")
        if provided_key != expected_key:
            return JsonResponse({"ok": False, "detail": "Unauthorized"}, status=401)

        return view_func(request, *args, **kwargs)

    return wrapper


def health(_: HttpRequest) -> JsonResponse:
    return JsonResponse({"ok": True, "service": "django-bot-backend"})


@csrf_exempt
@require_bot_api_key
def upsert_user(request: HttpRequest) -> JsonResponse:
    if request.method != "POST":
        return JsonResponse({"ok": False, "detail": "Method not allowed"}, status=405)

    payload = parse_json_request(request)
    telegram_id = payload.get("telegram_id")
    if not telegram_id:
        return JsonResponse({"ok": False, "detail": "telegram_id is required"}, status=400)

    try:
        telegram_id = int(telegram_id)
    except Exception:
        return JsonResponse({"ok": False, "detail": "telegram_id must be int"}, status=400)

    defaults = {
        "username": str(payload.get("username") or ""),
        "first_name": str(payload.get("first_name") or ""),
        "last_name": str(payload.get("last_name") or ""),
        "language_code": str(payload.get("language_code") or ""),
        "phone_number": str(payload.get("phone_number") or ""),
        "avatar_file_id": str(payload.get("avatar_file_id") or ""),
        "role": normalize_role(payload.get("role")),
        "is_authenticated": bool(payload.get("is_authenticated", False)),
        "authenticated_at": parse_iso_datetime(payload.get("authenticated_at")),
    }

    obj, created = TelegramUser.objects.update_or_create(
        telegram_id=telegram_id,
        defaults=defaults,
    )

    return JsonResponse(
        {
            "ok": True,
            "created": created,
            "telegram_id": obj.telegram_id,
            "role": obj.role,
        }
    )


@csrf_exempt
@require_bot_api_key
def create_action(request: HttpRequest) -> JsonResponse:
    if request.method != "POST":
        return JsonResponse({"ok": False, "detail": "Method not allowed"}, status=405)

    payload = parse_json_request(request)
    telegram_id = payload.get("telegram_id")
    action = str(payload.get("action") or "").strip()

    if not telegram_id:
        return JsonResponse({"ok": False, "detail": "telegram_id is required"}, status=400)
    if not action:
        return JsonResponse({"ok": False, "detail": "action is required"}, status=400)

    try:
        telegram_id = int(telegram_id)
    except Exception:
        return JsonResponse({"ok": False, "detail": "telegram_id must be int"}, status=400)

    user = TelegramUser.objects.filter(telegram_id=telegram_id).first()

    event_time = parse_iso_datetime(payload.get("timestamp")) or timezone.now()

    action_obj = UserAction.objects.create(
        user=user,
        telegram_id=telegram_id,
        username=str(payload.get("username") or ""),
        first_name=str(payload.get("first_name") or ""),
        last_name=str(payload.get("last_name") or ""),
        action=action,
        details=str(payload.get("details") or ""),
        created_at=event_time,
        raw_payload=payload,
    )

    return JsonResponse({"ok": True, "id": action_obj.id})


def _prepare_ad_defaults(payload: dict) -> dict:
    author = payload.get("author") if isinstance(payload.get("author"), dict) else {}
    status = str(payload.get("status") or AdItem.STATUS_ACTIVE).strip().lower()
    if status not in {AdItem.STATUS_ACTIVE, AdItem.STATUS_INACTIVE, AdItem.STATUS_ARCHIVED}:
        status = AdItem.STATUS_ACTIVE

    source_type = str(payload.get("source_type") or AdItem.SOURCE_MANUAL).strip().lower()
    if source_type not in {AdItem.SOURCE_EXCEL, AdItem.SOURCE_MANUAL}:
        source_type = AdItem.SOURCE_MANUAL

    year = payload.get("year")
    try:
        year = int(year) if year is not None and str(year).strip() else None
    except Exception:
        year = None

    price = payload.get("price", 0)
    try:
        price = int(price)
    except Exception:
        price = 0

    author_id = author.get("id")
    try:
        author_id = int(author_id) if author_id is not None and str(author_id).strip() else None
    except Exception:
        author_id = None

    return {
        "source_type": source_type,
        "external_id": str(payload.get("external_id") or ""),
        "title": str(payload.get("title") or "").strip(),
        "category": str(payload.get("category") or "").strip(),
        "price": price,
        "year": year,
        "details": str(payload.get("details") or ""),
        "location": str(payload.get("location") or ""),
        "image": str(payload.get("image") or ""),
        "status": status,
        "author_telegram_id": author_id,
        "author_username": str(author.get("username") or ""),
        "author_first_name": str(author.get("first_name") or ""),
        "author_last_name": str(author.get("last_name") or ""),
        "created_at_remote": parse_iso_datetime(payload.get("createdAt") or payload.get("created_at")),
        "raw_payload": payload,
    }


def _upsert_ad_item(payload: dict):
    ad_id = str(payload.get("id") or payload.get("ad_id") or "").strip()
    if not ad_id:
        raise ValueError("ad_id is required")

    defaults = _prepare_ad_defaults(payload)
    if not defaults["title"]:
        raise ValueError("title is required")

    return AdItem.objects.update_or_create(ad_id=ad_id, defaults=defaults)


def _normalize_actor_role(role: str | None) -> str:
    return normalize_role(role)


def _can_actor_manage_ad(actor_role: str, actor_telegram_id: int, ad_item: AdItem) -> tuple[bool, str]:
    if actor_role == TelegramUser.ROLE_ADMIN:
        return True, ""

    if actor_role == TelegramUser.ROLE_LEASING_COMPANY:
        if ad_item.author_telegram_id != actor_telegram_id:
            return False, "leasing_company can modify only own ads"
        return True, ""

    return False, "insufficient permissions"


def _parse_actor(payload: dict) -> tuple[int, str]:
    actor_telegram_id = payload.get("actor_telegram_id")
    if actor_telegram_id is None:
        raise ValueError("actor_telegram_id is required")
    try:
        actor_telegram_id = int(actor_telegram_id)
    except Exception as exc:
        raise ValueError("actor_telegram_id must be int") from exc

    actor_role = _normalize_actor_role(payload.get("actor_role"))
    return actor_telegram_id, actor_role


def _extract_ad_update_fields(updates: dict) -> tuple[dict, str | None]:
    cleaned: dict = {}

    if "title" in updates:
        title = str(updates.get("title") or "").strip()
        if not title:
            return {}, "title must not be empty"
        cleaned["title"] = title

    if "category" in updates:
        cleaned["category"] = str(updates.get("category") or "").strip()

    if "price" in updates:
        try:
            cleaned["price"] = int(updates.get("price") or 0)
        except Exception:
            cleaned["price"] = 0

    if "year" in updates:
        year = updates.get("year")
        try:
            cleaned["year"] = int(year) if year is not None and str(year).strip() else None
        except Exception:
            cleaned["year"] = None

    if "details" in updates:
        cleaned["details"] = str(updates.get("details") or "")

    if "location" in updates:
        cleaned["location"] = str(updates.get("location") or "")

    if "image" in updates:
        cleaned["image"] = str(updates.get("image") or "")

    if "status" in updates:
        status = str(updates.get("status") or "").strip().lower()
        if status in {AdItem.STATUS_ACTIVE, AdItem.STATUS_INACTIVE, AdItem.STATUS_ARCHIVED}:
            cleaned["status"] = status

    if "external_id" in updates:
        cleaned["external_id"] = str(updates.get("external_id") or "")

    if "source_type" in updates:
        source_type = str(updates.get("source_type") or "").strip().lower()
        if source_type in {AdItem.SOURCE_EXCEL, AdItem.SOURCE_MANUAL}:
            cleaned["source_type"] = source_type

    created_at_remote = updates.get("createdAt") if "createdAt" in updates else updates.get("created_at")
    if "createdAt" in updates or "created_at" in updates:
        cleaned["created_at_remote"] = parse_iso_datetime(created_at_remote)

    if "author" in updates and isinstance(updates.get("author"), dict):
        author = updates.get("author", {})
        author_id = author.get("id")
        try:
            author_id = int(author_id) if author_id is not None and str(author_id).strip() else None
        except Exception:
            author_id = None
        cleaned["author_telegram_id"] = author_id
        cleaned["author_username"] = str(author.get("username") or "")
        cleaned["author_first_name"] = str(author.get("first_name") or "")
        cleaned["author_last_name"] = str(author.get("last_name") or "")

    return cleaned, None


@csrf_exempt
@require_bot_api_key
def upsert_ad(request: HttpRequest) -> JsonResponse:
    if request.method != "POST":
        return JsonResponse({"ok": False, "detail": "Method not allowed"}, status=405)

    payload = parse_json_request(request)

    try:
        obj, created = _upsert_ad_item(payload)
    except ValueError as exc:
        return JsonResponse({"ok": False, "detail": str(exc)}, status=400)

    return JsonResponse({"ok": True, "created": created, "ad_id": obj.ad_id})


@csrf_exempt
@require_bot_api_key
def bulk_upsert_ads(request: HttpRequest) -> JsonResponse:
    if request.method != "POST":
        return JsonResponse({"ok": False, "detail": "Method not allowed"}, status=405)

    payload = parse_json_request(request)
    items = payload.get("items")

    if not isinstance(items, list):
        return JsonResponse({"ok": False, "detail": "items must be a list"}, status=400)

    created_count = 0
    updated_count = 0
    errors: list[dict] = []

    with transaction.atomic():
        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                errors.append({"index": idx, "error": "item must be object"})
                continue
            try:
                _, created = _upsert_ad_item(item)
            except ValueError as exc:
                errors.append({"index": idx, "error": str(exc)})
                continue

            if created:
                created_count += 1
            else:
                updated_count += 1

    return JsonResponse(
        {
            "ok": True,
            "created": created_count,
            "updated": updated_count,
            "errors": errors,
        }
    )


@csrf_exempt
@require_bot_api_key
def update_ad_with_permissions(request: HttpRequest) -> JsonResponse:
    if request.method != "POST":
        return JsonResponse({"ok": False, "detail": "Method not allowed"}, status=405)

    payload = parse_json_request(request)
    ad_id = str(payload.get("ad_id") or payload.get("id") or "").strip()
    if not ad_id:
        return JsonResponse({"ok": False, "detail": "ad_id is required"}, status=400)

    updates = payload.get("updates")
    if not isinstance(updates, dict):
        return JsonResponse({"ok": False, "detail": "updates must be object"}, status=400)

    try:
        actor_telegram_id, actor_role = _parse_actor(payload)
    except ValueError as exc:
        return JsonResponse({"ok": False, "detail": str(exc)}, status=400)

    ad_item = AdItem.objects.filter(ad_id=ad_id).first()
    if not ad_item:
        return JsonResponse({"ok": False, "detail": "ad not found"}, status=404)

    allowed, reason = _can_actor_manage_ad(actor_role, actor_telegram_id, ad_item)
    if not allowed:
        return JsonResponse({"ok": False, "detail": reason}, status=403)

    cleaned_updates, error_message = _extract_ad_update_fields(updates)
    if error_message:
        return JsonResponse({"ok": False, "detail": error_message}, status=400)
    if not cleaned_updates:
        return JsonResponse({"ok": False, "detail": "no updatable fields"}, status=400)

    existing_payload = ad_item.raw_payload if isinstance(ad_item.raw_payload, dict) else {}
    existing_payload["last_update"] = updates
    cleaned_updates["raw_payload"] = existing_payload
    for key, value in cleaned_updates.items():
        setattr(ad_item, key, value)
    ad_item.save(update_fields=list(cleaned_updates.keys()) + ["updated_at"])

    return JsonResponse({"ok": True, "ad_id": ad_item.ad_id})


@csrf_exempt
@require_bot_api_key
def delete_ad_with_permissions(request: HttpRequest) -> JsonResponse:
    if request.method != "POST":
        return JsonResponse({"ok": False, "detail": "Method not allowed"}, status=405)

    payload = parse_json_request(request)
    ad_id = str(payload.get("ad_id") or payload.get("id") or "").strip()
    if not ad_id:
        return JsonResponse({"ok": False, "detail": "ad_id is required"}, status=400)

    try:
        actor_telegram_id, actor_role = _parse_actor(payload)
    except ValueError as exc:
        return JsonResponse({"ok": False, "detail": str(exc)}, status=400)

    ad_item = AdItem.objects.filter(ad_id=ad_id).first()
    if not ad_item:
        return JsonResponse({"ok": False, "detail": "ad not found"}, status=404)

    allowed, reason = _can_actor_manage_ad(actor_role, actor_telegram_id, ad_item)
    if not allowed:
        return JsonResponse({"ok": False, "detail": reason}, status=403)

    ad_item.delete()
    return JsonResponse({"ok": True, "ad_id": ad_id})


@require_bot_api_key
def user_role(request: HttpRequest, telegram_id: int) -> JsonResponse:
    if request.method != "GET":
        return JsonResponse({"ok": False, "detail": "Method not allowed"}, status=405)

    user = TelegramUser.objects.filter(telegram_id=telegram_id).first()
    if not user:
        return JsonResponse({"ok": False, "detail": "User not found"}, status=404)

    return JsonResponse({"ok": True, "telegram_id": telegram_id, "role": user.role})
