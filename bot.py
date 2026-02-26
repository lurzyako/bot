import logging
import json
import os
import html
import re
from datetime import datetime
from pathlib import Path
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    WebAppInfo,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
)
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
import pandas as pd

# Импорт ядра парсера (без GUI) из корня проекта
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in os.sys.path:
    os.sys.path.insert(0, str(ROOT_DIR))

PARSER_AVAILABLE = False
PARSER_IMPORT_ERROR = ""
try:
    from parser import read_flexible, prepare_cards, generate_site
    import config_manager
    import column_mapper

    PARSER_AVAILABLE = True
except Exception as parser_import_exc:
    read_flexible = None
    prepare_cards = None
    generate_site = None
    config_manager = None
    column_mapper = None
    PARSER_IMPORT_ERROR = str(parser_import_exc)

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Ваши данные
TOKEN = os.getenv("BOT_TOKEN", "")
HTML_FILE_URL = os.getenv("HTML_FILE_URL", "https://lurzyako.github.io/bot/")
DJANGO_BACKEND_URL = os.getenv("DJANGO_BACKEND_URL", "").rstrip("/")
DJANGO_BACKEND_API_KEY = os.getenv("DJANGO_BACKEND_API_KEY", "")
try:
    DJANGO_BACKEND_TIMEOUT = float(os.getenv("DJANGO_BACKEND_TIMEOUT", "5"))
except ValueError:
    DJANGO_BACKEND_TIMEOUT = 5.0

# Путь к файлу логов пользователей
USERS_LOG_FILE = "users_log.json"

# ID администраторов (только для команды /stats)
ADMIN_IDS = [1729659964]
ADMIN_PRESET_USERS = {
    1729659964: {
        "phone_number": "+79326157743",
    }
}
PARSER_OUTPUT_DIR = Path(__file__).resolve().parent / "parsed_output"
PARSER_TMP_DIR = Path(__file__).resolve().parent / "tmp_uploads"
ADS_FEED_FILE = Path(__file__).resolve().parent / "ads_feed.json"
AUTH_USERS_FILE = Path(__file__).resolve().parent / "auth_users.json"

USER_ROLE_USER = "user"
USER_ROLE_LEASING_COMPANY = "leasing_company"
USER_ROLE_ADMIN = "admin"
AD_MANAGEMENT_ROLES = {USER_ROLE_ADMIN, USER_ROLE_LEASING_COMPANY}
ROLE_LABELS = {
    USER_ROLE_USER: "Пользователь",
    USER_ROLE_LEASING_COMPANY: "Лизинговая компания",
    USER_ROLE_ADMIN: "Администратор",
}
BOT_BUILD_VERSION = os.getenv("BOT_BUILD_VERSION", "2026-02-26-parser-safe-import-v2")


def is_parser_enabled() -> bool:
    return PARSER_AVAILABLE and all([read_flexible, prepare_cards, generate_site, config_manager, column_mapper])


def backend_sync_enabled() -> bool:
    return bool(DJANGO_BACKEND_URL and DJANGO_BACKEND_API_KEY)


def backend_request(
    method: str,
    path: str,
    payload: dict | None = None,
    suppress_not_found: bool = False
) -> dict | None:
    """Выполняет запрос в Django backend (best effort, без фатальных ошибок)."""
    if not backend_sync_enabled():
        return None

    data = None
    headers = {"X-API-Key": DJANGO_BACKEND_API_KEY}
    if payload is not None:
        headers["Content-Type"] = "application/json; charset=utf-8"
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    request_url = f"{DJANGO_BACKEND_URL}{path}"
    request = urllib_request.Request(
        url=request_url,
        data=data,
        headers=headers,
        method=method.upper()
    )

    try:
        with urllib_request.urlopen(request, timeout=DJANGO_BACKEND_TIMEOUT) as response:
            raw = response.read().decode("utf-8")
            if not raw:
                return {}
            try:
                return json.loads(raw)
            except Exception:
                return {"raw": raw}
    except urllib_error.HTTPError as exc:
        if suppress_not_found and exc.code == 404:
            return None
        body = exc.read().decode("utf-8", errors="ignore")
        logger.warning(
            "Backend sync failed %s %s: HTTP %s %s",
            method,
            path,
            exc.code,
            body[:300]
        )
    except Exception as exc:
        logger.warning("Backend sync failed %s %s: %s", method, path, exc)
    return None


def normalize_user_role(role: str | None) -> str:
    normalized = str(role or USER_ROLE_USER).strip().lower()
    if normalized in {"leasing", "leasing_company", "лизинговая", "лизинговая компания", "лизинговая_компания"}:
        return USER_ROLE_LEASING_COMPANY
    if normalized in {"admin", "админ", "администратор"}:
        return USER_ROLE_ADMIN
    return USER_ROLE_USER


def fetch_backend_user_role(user_id: int) -> str | None:
    response = backend_request(
        "GET",
        f"/api/users/{user_id}/role/",
        suppress_not_found=True
    )
    if not isinstance(response, dict):
        return None
    role = response.get("role")
    if not role:
        return None
    return normalize_user_role(role)


def sync_user_to_backend(user_record: dict) -> None:
    payload = {
        "telegram_id": user_record.get("telegram_id"),
        "username": user_record.get("username"),
        "first_name": user_record.get("first_name"),
        "last_name": user_record.get("last_name"),
        "language_code": user_record.get("language_code"),
        "phone_number": user_record.get("phone_number"),
        "avatar_file_id": user_record.get("avatar_file_id"),
        "role": user_record.get("role"),
        "is_authenticated": bool(user_record.get("is_authenticated")),
        "authenticated_at": user_record.get("authenticated_at"),
    }
    backend_request("POST", "/api/users/upsert/", payload)


def sync_user_action_to_backend(log_entry: dict) -> None:
    payload = {
        "telegram_id": log_entry.get("user_id"),
        "username": log_entry.get("username"),
        "first_name": log_entry.get("first_name"),
        "last_name": log_entry.get("last_name"),
        "action": log_entry.get("action"),
        "details": log_entry.get("details"),
        "timestamp": log_entry.get("timestamp"),
    }
    backend_request("POST", "/api/actions/", payload)


def serialize_ad_for_backend(ad: dict) -> dict:
    author = ad.get("author") if isinstance(ad.get("author"), dict) else {}
    return {
        "id": ad.get("id"),
        "source_type": ad.get("source_type"),
        "external_id": ad.get("external_id"),
        "title": ad.get("title"),
        "category": ad.get("category"),
        "price": ad.get("price"),
        "year": ad.get("year"),
        "details": ad.get("details"),
        "location": ad.get("location"),
        "image": ad.get("image"),
        "status": ad.get("status"),
        "createdAt": ad.get("createdAt"),
        "author": {
            "id": author.get("id"),
            "username": author.get("username"),
            "first_name": author.get("first_name"),
            "last_name": author.get("last_name"),
        }
    }


def sync_ad_to_backend(ad: dict) -> None:
    backend_request("POST", "/api/ads/upsert/", serialize_ad_for_backend(ad))


def sync_ads_to_backend(items: list[dict]) -> None:
    if not items:
        return
    payload = {"items": [serialize_ad_for_backend(item) for item in items]}
    backend_request("POST", "/api/ads/bulk-upsert/", payload)


def sync_update_ad_with_permissions(ad_id: str, actor_user_id: int, actor_role: str, updates: dict) -> None:
    payload = {
        "ad_id": str(ad_id),
        "actor_telegram_id": int(actor_user_id),
        "actor_role": actor_role,
        "updates": updates,
    }
    backend_request("POST", "/api/ads/update/", payload)


def sync_delete_ad_with_permissions(ad_id: str, actor_user_id: int, actor_role: str) -> None:
    payload = {
        "ad_id": str(ad_id),
        "actor_telegram_id": int(actor_user_id),
        "actor_role": actor_role,
    }
    backend_request("POST", "/api/ads/delete/", payload)


def get_user_role(user_id: int) -> str:
    if user_id in ADMIN_IDS:
        return USER_ROLE_ADMIN
    backend_role = fetch_backend_user_role(user_id)
    if backend_role:
        return backend_role
    auth_record = get_authenticated_user(user_id)
    if not auth_record:
        return USER_ROLE_USER
    return normalize_user_role(auth_record.get("role"))


def can_user_manage_ads(user_id: int) -> bool:
    return get_user_role(user_id) in AD_MANAGEMENT_ROLES


def build_web_app_url(user_id: int | None = None) -> str:
    role = USER_ROLE_USER if user_id is None else get_user_role(user_id)
    parsed = urlparse(HTML_FILE_URL)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["role"] = role
    if user_id is not None:
        query["uid"] = str(user_id)
    return urlunparse(parsed._replace(query=urlencode(query)))


def build_web_app_button(user_id: int | None = None, text: str = "📱 Открыть каталог") -> InlineKeyboardButton:
    return InlineKeyboardButton(text, web_app=WebAppInfo(url=build_web_app_url(user_id)))


def build_main_keyboard(user_id: int | None = None) -> list[list[InlineKeyboardButton]]:
    return [
        [build_web_app_button(user_id)],
        [InlineKeyboardButton("👤 Мой профиль", callback_data='profile')],
        [InlineKeyboardButton("⭐ Рейтинг РА Эксперт", callback_data='rating')],
        [InlineKeyboardButton("📞 Контакты", callback_data='contacts')],
        [InlineKeyboardButton("ℹ️ О компании", callback_data='about')]
    ]


def build_main_menu_markup(user_id: int | None = None) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(build_main_keyboard(user_id))


def load_auth_users() -> dict:
    """Загружает пользователей, прошедших аутентификацию."""
    if not AUTH_USERS_FILE.exists():
        return {}
    try:
        with open(AUTH_USERS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.error("Ошибка чтения auth_users.json: %s", e)
        return {}


def save_auth_users(users: dict) -> None:
    """Сохраняет пользователей, прошедших аутентификацию."""
    with open(AUTH_USERS_FILE, "w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=2)


def build_preset_admin_auth_record(user_id: int) -> dict | None:
    preset = ADMIN_PRESET_USERS.get(user_id)
    if not preset:
        return None

    now_iso = datetime.now().isoformat()
    return {
        "telegram_id": user_id,
        "username": preset.get("username"),
        "first_name": preset.get("first_name"),
        "last_name": preset.get("last_name"),
        "language_code": preset.get("language_code"),
        "phone_number": preset.get("phone_number", ""),
        "avatar_file_id": preset.get("avatar_file_id"),
        "role": USER_ROLE_ADMIN,
        "is_authenticated": True,
        "authenticated_at": preset.get("authenticated_at", now_iso),
        "updated_at": now_iso
    }


def get_authenticated_user(user_id: int) -> dict | None:
    users = load_auth_users()
    record = users.get(str(user_id))
    if isinstance(record, dict) and record.get("is_authenticated"):
        return record
    preset_admin = build_preset_admin_auth_record(user_id)
    if preset_admin:
        users[str(user_id)] = preset_admin
        save_auth_users(users)
        sync_user_to_backend(preset_admin)
        return preset_admin
    return None


def is_user_authenticated(user_id: int) -> bool:
    return get_authenticated_user(user_id) is not None


def build_auth_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура для подтверждения личности через контакт Telegram."""
    return ReplyKeyboardMarkup(
        [[KeyboardButton("🔐 Поделиться контактом", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
        input_field_placeholder="Нажмите кнопку, чтобы пройти аутентификацию"
    )


async def fetch_user_avatar_file_id(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> str | None:
    """Получает file_id аватарки пользователя в Telegram."""
    try:
        photos = await context.bot.get_user_profile_photos(user_id=user_id, limit=1)
        if photos.total_count > 0 and photos.photos and photos.photos[0]:
            # Берём фото максимального размера из первой группы.
            return photos.photos[0][-1].file_id
    except Exception as e:
        logger.warning("Не удалось получить аватар пользователя %s: %s", user_id, e)
    return None
    

async def register_authenticated_user(
    user,
    phone_number: str | None,
    context: ContextTypes.DEFAULT_TYPE
) -> dict:
    """Сохраняет/обновляет запись аутентифицированного пользователя."""
    users = load_auth_users()
    existing = users.get(str(user.id), {})
    avatar_file_id = await fetch_user_avatar_file_id(context, user.id)
    resolved_role = normalize_user_role(existing.get("role"))
    if user.id in ADMIN_IDS:
        resolved_role = USER_ROLE_ADMIN

    users[str(user.id)] = {
        "telegram_id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "language_code": user.language_code,
        "phone_number": phone_number or existing.get("phone_number"),
        "avatar_file_id": avatar_file_id or existing.get("avatar_file_id"),
        "role": resolved_role,
        "is_authenticated": True,
        "authenticated_at": existing.get("authenticated_at") or datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }

    save_auth_users(users)
    sync_user_to_backend(users[str(user.id)])
    return users[str(user.id)]


async def prompt_authentication(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    reason: str = ""
) -> None:
    """Просит пользователя пройти обязательную аутентификацию."""
    extra = f"\n\nПричина: {reason}" if reason else ""
    text = (
        "🔒 Для работы с ботом нужно пройти аутентификацию.\n"
        "Нажмите кнопку ниже и отправьте ваш контакт Telegram."
        f"{extra}"
    )

    if update.message:
        await update.message.reply_text(text, reply_markup=build_auth_keyboard())
        return

    if update.callback_query:
        try:
            await update.callback_query.answer("Сначала нужно пройти аутентификацию", show_alert=True)
        except Exception:
            pass
        if update.effective_chat:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=text,
                reply_markup=build_auth_keyboard()
            )
        return

    if update.effective_chat:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=text,
            reply_markup=build_auth_keyboard()
        )


async def ensure_authenticated(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    reason: str = ""
) -> bool:
    """Проверяет обязательную аутентификацию перед доступом к функциям."""
    user = update.effective_user
    if not user:
        return False
    if is_user_authenticated(user.id):
        return True

    await prompt_authentication(update, context, reason=reason)
    log_user_action(
        {
            "id": user.id,
            "username": user.username,
            "first_name": user.first_name,
            "last_name": user.last_name
        },
        "auth_required",
        reason or "Попытка доступа без аутентификации"
    )
    return False


async def send_profile_card(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправляет карточку профиля текущего пользователя."""
    user = update.effective_user
    if not user:
        return

    auth_record = get_authenticated_user(user.id)
    if not auth_record:
        await prompt_authentication(update, context, reason="Просмотр профиля")
        return

    # Актуализируем аватар при просмотре профиля.
    avatar_file_id = auth_record.get("avatar_file_id") or await fetch_user_avatar_file_id(context, user.id)
    if avatar_file_id and avatar_file_id != auth_record.get("avatar_file_id"):
        users = load_auth_users()
        if str(user.id) in users:
            users[str(user.id)]["avatar_file_id"] = avatar_file_id
            users[str(user.id)]["updated_at"] = datetime.now().isoformat()
            save_auth_users(users)

    full_name = f"{auth_record.get('first_name', '')} {auth_record.get('last_name', '')}".strip() or "Не указано"
    username = f"@{auth_record.get('username')}" if auth_record.get("username") else "не указан"
    phone = auth_record.get("phone_number") or "не указан"
    role_code = get_user_role(user.id)
    role_label = ROLE_LABELS.get(role_code, role_code)
    authenticated_at = auth_record.get("authenticated_at", "")
    if authenticated_at:
        try:
            authenticated_at = datetime.fromisoformat(authenticated_at).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass

    profile_text = (
        "<b>👤 Профиль пользователя</b>\n\n"
        f"<b>Имя:</b> {escape_html_for_telegram(full_name)}\n"
        f"<b>Username:</b> {escape_html_for_telegram(username)}\n"
        f"<b>Телефон:</b> {escape_html_for_telegram(phone)}\n"
        f"<b>Telegram ID:</b> <code>{user.id}</code>\n"
        f"<b>Роль:</b> {escape_html_for_telegram(role_label)}\n"
        f"<b>Статус:</b> ✅ аутентифицирован\n"
        f"<b>Авторизация:</b> {escape_html_for_telegram(str(authenticated_at))}"
    )

    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if not target_message:
        return

    if avatar_file_id:
        await target_message.reply_photo(
            photo=avatar_file_id,
            caption=profile_text,
            parse_mode="HTML",
            reply_markup=build_main_menu_markup(user.id)
        )
    else:
        await target_message.reply_text(
            profile_text,
            parse_mode="HTML",
            reply_markup=build_main_menu_markup(user.id)
        )

def _parse_price_to_int(value) -> int:
    """Приводит цену к int."""
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    text = re.sub(r"[^\d]", "", str(value))
    return int(text) if text else 0

def _normalize_category(category: str) -> str:
    """Нормализует категорию под формат фронтенда."""
    if not category:
        return "equipment"
    normalized = str(category).strip().lower()
    if normalized in {"car", "passenger", "легковой", "легковой автомобиль"}:
        return "passenger"
    if normalized in {"spec", "спецтехника"}:
        return "spec"
    if normalized in {"truck", "грузовой", "грузовой транспорт"}:
        return "truck"
    if normalized in {"equipment", "оборудование"}:
        return "equipment"
    return "equipment"

def _category_from_vehicle_type(vehicle_type: str) -> str:
    """Определяет категорию по полю типа ТС из Excel."""
    text = (vehicle_type or "").lower()
    if any(x in text for x in ["легков", "lcv", "седан", "хэтчбек", "внедорож"]):
        return "passenger"
    if any(x in text for x in ["груз", "тягач", "фургон", "самосвал", "прицеп"]):
        return "truck"
    if any(x in text for x in ["экскават", "бульдозер", "трактор", "каток", "погрузчик", "кран"]):
        return "spec"
    return "equipment"

def load_ads_feed() -> dict:
    """Загружает единый фид объявлений."""
    if not ADS_FEED_FILE.exists():
        return {"updated_at": datetime.now().isoformat(), "items": []}
    try:
        with open(ADS_FEED_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return {"updated_at": datetime.now().isoformat(), "items": data}
        if "items" not in data:
            data["items"] = []
        return data
    except Exception as e:
        logger.error("Ошибка чтения ads_feed.json: %s", e)
        return {"updated_at": datetime.now().isoformat(), "items": []}

def save_ads_feed(feed: dict) -> None:
    """Сохраняет единый фид объявлений."""
    feed["updated_at"] = datetime.now().isoformat()
    with open(ADS_FEED_FILE, "w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, indent=2)

def replace_excel_ads(cards: list[dict]) -> int:
    """Полностью заменяет Excel-часть общего фида на свежую выгрузку."""
    feed = load_ads_feed()
    manual_items = [item for item in feed.get("items", []) if item.get("source_type") != "excel"]

    excel_items = []
    for card in cards:
        code = str(card.get("code", "")).strip()
        title = str(card.get("title", "")).strip()
        if not title:
            continue
        excel_items.append({
            "id": f"excel-{code}" if code else f"excel-{len(excel_items) + 1}",
            "source_type": "excel",
            "external_id": code,
            "title": title,
            "category": _category_from_vehicle_type(card.get("vehicle_type", "")),
            "price": _parse_price_to_int(card.get("price")),
            "year": _parse_price_to_int(card.get("year")) or None,
            "details": (card.get("short_desc") or card.get("comment") or "")[:2000],
            "location": card.get("location") or card.get("address") or "Не указано",
            "image": card.get("photo_url") or "",
            "status": "active",
            "createdAt": datetime.now().isoformat()
        })

    feed["items"] = manual_items + excel_items
    save_ads_feed(feed)
    sync_ads_to_backend(excel_items)
    return len(excel_items)

def add_manual_ad_to_feed(ad: dict, user_data: dict) -> dict:
    """Добавляет ручное объявление в общий фид."""
    feed = load_ads_feed()

    ad_id = str(ad.get("id") or f"manual-{int(datetime.now().timestamp())}-{user_data.get('id')}")
    feed_item = {
        "id": ad_id,
        "source_type": "manual",
        "external_id": ad_id,
        "title": str(ad.get("title", "")).strip(),
        "category": _normalize_category(ad.get("category")),
        "price": _parse_price_to_int(ad.get("price")),
        "year": _parse_price_to_int(ad.get("year")) or None,
        "details": str(ad.get("details", "")).strip()[:2000],
        "location": str(ad.get("location", "Не указано")).strip() or "Не указано",
        "image": (ad.get("images") or [""])[0] if isinstance(ad.get("images"), list) else "",
        "status": "active",
        "createdAt": ad.get("createdAt") or datetime.now().isoformat(),
        "author": {
            "id": user_data.get("id"),
            "username": user_data.get("username"),
            "first_name": user_data.get("first_name"),
            "last_name": user_data.get("last_name")
        }
    }

    items = [item for item in feed.get("items", []) if str(item.get("id")) != ad_id]
    items.append(feed_item)
    feed["items"] = items
    save_ads_feed(feed)
    sync_ad_to_backend(feed_item)
    return feed_item


def _find_feed_item_by_id(feed: dict, ad_id: str) -> tuple[int, dict] | tuple[None, None]:
    ad_id_str = str(ad_id)
    items = feed.get("items", [])
    for index, item in enumerate(items):
        if str(item.get("id")) == ad_id_str:
            return index, item
    return None, None


def _can_user_edit_or_delete_ad(actor_user_id: int, actor_role: str, ad_item: dict) -> tuple[bool, str]:
    if actor_role == USER_ROLE_ADMIN:
        return True, ""

    if actor_role != USER_ROLE_LEASING_COMPANY:
        return False, "Недостаточно прав для изменения объявления."

    author = ad_item.get("author") if isinstance(ad_item.get("author"), dict) else {}
    owner_id = author.get("id")
    try:
        owner_id = int(owner_id) if owner_id is not None else None
    except Exception:
        owner_id = None

    if owner_id != actor_user_id:
        return False, "Лизинговая компания может изменять только свои объявления."

    return True, ""


def update_manual_ad_in_feed(
    ad_id: str,
    ad_updates: dict,
    actor_user_data: dict,
    actor_role: str
) -> tuple[dict | None, str]:
    feed = load_ads_feed()
    index, target = _find_feed_item_by_id(feed, ad_id)
    if target is None:
        return None, "Объявление не найдено."

    allowed, reason = _can_user_edit_or_delete_ad(int(actor_user_data.get("id")), actor_role, target)
    if not allowed:
        return None, reason

    updates: dict = {}
    if "title" in ad_updates:
        title = str(ad_updates.get("title") or "").strip()
        if not title:
            return None, "Название объявления не может быть пустым."
        updates["title"] = title

    if "category" in ad_updates:
        updates["category"] = _normalize_category(ad_updates.get("category"))

    if "price" in ad_updates:
        updates["price"] = _parse_price_to_int(ad_updates.get("price"))

    if "year" in ad_updates:
        updates["year"] = _parse_price_to_int(ad_updates.get("year")) or None

    if "details" in ad_updates:
        updates["details"] = str(ad_updates.get("details") or "").strip()[:2000]

    if "location" in ad_updates:
        updates["location"] = str(ad_updates.get("location") or "").strip() or "Не указано"

    if "status" in ad_updates:
        status = str(ad_updates.get("status") or "").strip().lower()
        if status in {"active", "inactive", "archived"}:
            updates["status"] = status

    images = ad_updates.get("images")
    if isinstance(images, list) and images:
        updates["image"] = str(images[0])
    elif "image" in ad_updates:
        updates["image"] = str(ad_updates.get("image") or "")

    if not updates:
        return None, "Нет данных для обновления."

    target.update(updates)
    target["updatedAt"] = datetime.now().isoformat()
    feed["items"][index] = target
    save_ads_feed(feed)
    sync_update_ad_with_permissions(
        str(ad_id),
        int(actor_user_data.get("id")),
        actor_role,
        updates
    )
    return target, ""


def delete_manual_ad_from_feed(
    ad_id: str,
    actor_user_data: dict,
    actor_role: str
) -> tuple[bool, str]:
    feed = load_ads_feed()
    index, target = _find_feed_item_by_id(feed, ad_id)
    if target is None or index is None:
        return False, "Объявление не найдено."

    allowed, reason = _can_user_edit_or_delete_ad(int(actor_user_data.get("id")), actor_role, target)
    if not allowed:
        return False, reason

    items = feed.get("items", [])
    del items[index]
    feed["items"] = items
    save_ads_feed(feed)
    sync_delete_ad_with_permissions(str(ad_id), int(actor_user_data.get("id")), actor_role)
    return True, ""

def escape_html_for_telegram(text: str) -> str:
    """
    Экранирование текста для безопасного использования в Telegram с parse_mode='HTML'
    Более мягкое экранирование для сохранения читаемости имен пользователей
    
    Args:
        text: Текст для экранирования
        
    Returns:
        Экранированный текст, безопасный для Telegram HTML-парсера
    """
    if not text:
        return ""
    
    # Экранируем только основные HTML-символы, которые могут сломать парсинг
    # В Telegram HTML mode разрешены только <, >, &, "
    escaped = text
    
    # Заменяем & на &amp; ПЕРВЫМ, чтобы не ломать другие замены
    escaped = escaped.replace('&', '&amp;')
    
    # Затем заменяем остальные символы
    escaped = escaped.replace('<', '&lt;')
    escaped = escaped.replace('>', '&gt;')
    escaped = escaped.replace('"', '&quot;')
    
    return escaped

def log_user_action(user_data: dict, action: str, details: str = "") -> None:
    """
    Логирование действий пользователя
    
    Args:
        user_data: Данные пользователя
        action: Тип действия (start, catalogue, button_click и т.д.)
        details: Дополнительные детали
    """
    try:
        # Формируем запись лога
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "user_id": user_data.get("id"),
            "username": user_data.get("username"),
            "first_name": user_data.get("first_name"),
            "last_name": user_data.get("last_name"),
            "action": action,
            "details": details
        }
        
        # Загружаем существующие логи
        logs = []
        if os.path.exists(USERS_LOG_FILE):
            try:
                with open(USERS_LOG_FILE, 'r', encoding='utf-8') as f:
                    logs = json.load(f)
            except (json.JSONDecodeError, IOError):
                logs = []
        
        # Добавляем новую запись
        logs.append(log_entry)
        
        # Сохраняем (ограничиваем размер файла - последние 1000 записей)
        if len(logs) > 1000:
            logs = logs[-1000:]
        
        with open(USERS_LOG_FILE, 'w', encoding='utf-8') as f:
            json.dump(logs, f, ensure_ascii=False, indent=2)
        
        # Также логируем в консоль
        logger.info(f"User action: {user_data.get('username')} ({user_data.get('id')}) - {action} - {details}")
        sync_user_action_to_backend(log_entry)
    
    except Exception as e:
        logger.error(f"Error logging user action: {e}")

def get_user_stats() -> dict:
    """Получение статистики пользователей"""
    if not os.path.exists(USERS_LOG_FILE):
        return {"total_users": 0, "total_actions": 0, "unique_users": 0}
    
    try:
        with open(USERS_LOG_FILE, 'r', encoding='utf-8') as f:
            logs = json.load(f)
        
        # Собираем уникальных пользователей
        unique_users = set()
        for log in logs:
            unique_users.add(log.get("user_id"))
        
        # Статистика по действиям
        actions_count = {}
        for log in logs:
            action = log.get("action")
            actions_count[action] = actions_count.get(action, 0) + 1
        
        return {
            "total_users": len(logs),
            "total_actions": sum(actions_count.values()),
            "unique_users": len(unique_users),
            "actions_count": actions_count
        }
    
    except Exception as e:
        logger.error(f"Error getting user stats: {e}")
        return {"error": str(e)}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /start (доступна всем)"""
    user = update.effective_user
    
    # ОТЛАДОЧНАЯ ИНФОРМАЦИЯ
    logger.info(f"Пользователь пытается запустить бота:")
    logger.info(f"ID: {user.id}")
    logger.info(f"Имя: {user.first_name}")
    logger.info(f"Username: @{user.username}")
    logger.info(f"В списке админов: {user.id in ADMIN_IDS}")
    logger.info(f"Полный список админов: {ADMIN_IDS}")
    # КОНЕЦ ОТЛАДОЧНОЙ ИНФОРМАЦИИ
    
    user_data = {
        "id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "language_code": user.language_code
    }
    
    # Логируем начало работы пользователя
    log_user_action(user_data, "start", "Пользователь запустил бота")

    # Обязательная аутентификация перед использованием бота.
    if not is_user_authenticated(user.id):
        await prompt_authentication(update, context, reason="Первый запуск /start")
        await update.message.reply_text(
            "После подтверждения контакта доступны каталог, заявки и профиль.",
            reply_markup=build_auth_keyboard()
        )
        return
    
    # Экранируем имя пользователя для безопасного использования в HTML
    # НОВЫЙ ПОДХОД: используем более мягкое экранирование
    if user.first_name:
        # Экранируем только опасные символы
        safe_first_name = escape_html_for_telegram(user.first_name)
        # Для отладки выведем, что получилось
        logger.info(f"Исходное имя: {user.first_name}")
        logger.info(f"Экранированное имя: {safe_first_name}")
    else:
        safe_first_name = "Пользователь"
    
    welcome_text = f"""
👋 Здравствуйте, {safe_first_name}!

Я бот компании <b>КФЛ Лизинг</b> — вашего надежного партнера в сфере лизинга конфиската.

🚀 <b>С моей помощью вы можете:</b>
• Просмотреть каталог из 60+ единиц техники
• Найти выгодные предложения по лизингу
• Оставить заявку на понравившуюся технику
• Узнать о рейтингах и достижениях компании

👇 <b>Используйте кнопки ниже или команды:</b>
/catalogue - Открыть каталог конфиската (внутри Telegram)
/profile - Показать мой Telegram-профиль
/help - Получить справку

📱 <b>Каталог открывается прямо в Telegram!</b>
"""
    
    reply_markup = build_main_menu_markup(user.id)
    await update.message.reply_text(
        welcome_text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )


async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запрашивает обязательную аутентификацию через контакт Telegram."""
    user = update.effective_user
    if user and is_user_authenticated(user.id):
        await update.message.reply_text(
            "✅ Вы уже аутентифицированы. Используйте /profile для просмотра профиля.",
            reply_markup=build_main_menu_markup(user.id)
        )
        return

    await prompt_authentication(update, context, reason="Команда /login")


async def profile_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда просмотра профиля аутентифицированного пользователя."""
    if not await ensure_authenticated(update, context, reason="Команда /profile"):
        return
    await send_profile_card(update, context)


async def contact_auth_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Аутентификация по кнопке 'Поделиться контактом'."""
    if not update.message or not update.message.contact:
        return

    user = update.effective_user
    contact = update.message.contact

    if not user:
        return

    # Принимаем только собственный контакт пользователя.
    if contact.user_id != user.id:
        await update.message.reply_text(
            "⛔ Нужно подтвердить именно ваш Telegram-контакт (кнопкой ниже).",
            reply_markup=build_auth_keyboard()
        )
        return

    record = await register_authenticated_user(user, contact.phone_number, context)

    user_data = {
        "id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name
    }
    log_user_action(user_data, "auth_success", f"Аутентификация по контакту: {record.get('phone_number')}")

    safe_first_name = escape_html_for_telegram(user.first_name) if user.first_name else "Пользователь"
    await update.message.reply_text(
        f"✅ {safe_first_name}, аутентификация прошла успешно!",
        reply_markup=ReplyKeyboardRemove()
    )

    await update.message.reply_text(
        "Теперь доступны все функции бота. Ниже главное меню.",
        reply_markup=build_main_menu_markup(user.id)
    )

async def catalogue(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /catalogue (доступна всем)"""
    user = update.effective_user
    user_data = {
        "id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name
    }

    if not await ensure_authenticated(update, context, reason="Открытие каталога"):
        return
    
    # Логируем открытие каталога
    log_user_action(user_data, "catalogue", "Пользователь открыл каталог")
    
    message_text = """
<b>📋 Каталог конфиската КФЛ Лизинг</b>

Нажмите кнопку ниже, чтобы открыть интерактивный каталог прямо в Telegram:

<b>📊 В каталоге представлено:</b>
• 🚜 <b>24 единицы</b> спецтехники
• 🚛 <b>15 единиц</b> грузового транспорта  
• 🚗 <b>8 единиц</b> легковых автомобилей
• ⚙️ <b>13 единиц</b> оборудования

<b>✨ Особенности Web App каталога:</b>
• 📱 Оптимизирован для просмотра в Telegram
• 🔍 Умный поиск по характеристикам
• ⚡ Быстрая фильтрация
• 💖 Добавление в избранное
• 📝 Мгновенные заявки на лизинг
"""
    
    keyboard = [
        [build_web_app_button(user.id, "🚀 Открыть каталог (в Telegram)")],
        [InlineKeyboardButton("📞 Связаться с менеджером", callback_data='manager')],
        [InlineKeyboardButton("◀️ Назад в меню", callback_data='menu')]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        message_text,
        parse_mode='HTML',
        reply_markup=reply_markup,
        disable_web_page_preview=True
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /help (доступна всем)"""
    user = update.effective_user
    user_data = {
        "id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name
    }

    if not is_user_authenticated(user.id):
        await update.message.reply_text(
            "Для использования бота сначала пройдите аутентификацию через /login или кнопку ниже.",
            reply_markup=build_auth_keyboard()
        )
        return
    
    # Логируем запрос справку
    log_user_action(user_data, "help", "Пользователь запросил справку")
    
    help_text = """
<b>📚 Справка по командам</b>

<b>Основные команды:</b>
/start - Запустить бота и показать меню
/catalogue - Открыть каталог конфиската (в Telegram)
/login - Пройти аутентификацию
/profile - Показать мой профиль
/help - Показать эту справку
/stats - Статистика (только админ)

<b>Импорт Excel:</b>
• Запускается только через WebApp (кнопка «Парсер»)
• Доступен ролям «Администратор» и «Лизинговая компания»

<b>Быстрые действия через кнопки:</b>
📱 <b>Открыть каталог</b> - Каталог откроется прямо в Telegram
⭐ <b>Рейтинг РА Эксперт</b> - Узнать о наших достижениях  
📞 <b>Контакты</b> - Связаться с нами
ℹ️ <b>О компании</b> - Подробнее о КФЛ Лизинг

<b>💡 Советы по использованию Web App:</b>
• Каталог открывается внутри Telegram (не нужно переходить в браузер)
• Полностью адаптирован для мобильных устройств
• Для быстрого доступа можно закрепить сообщение с каталогом
"""
    
    reply_markup = build_main_menu_markup(user.id)
    await update.message.reply_text(
        help_text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /stats (только для администраторов)"""
    user = update.effective_user
    user_data = {
        "id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name
    }

    if not await ensure_authenticated(update, context, reason="Просмотр статистики"):
        return
    
    # ОТЛАДОЧНАЯ ИНФОРМАЦИЯ
    logger.info(f"Пользователь пытается получить статистику:")
    logger.info(f"ID: {user.id}")
    logger.info(f"Username: @{user.username}")
    logger.info(f"В списке админов: {user.id in ADMIN_IDS}")
    # КОНЕЦ ОТЛАДОЧНОЙ ИНФОРМАЦИИ
    
    # Проверяем, является ли пользователь администратором
    # ТОЛЬКО для этой команды!
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("⛔ У вас нет доступа к этой команде.")
        log_user_action(user_data, "stats_denied", "Попытка доступа к статистике без прав")
        return
    
    # Логируем запрос статистики
    log_user_action(user_data, "stats", "Администратор запросил статистику")
    
    # Получаем статистику
    stats = get_user_stats()
    
    if "error" in stats:
        stats_text = f"❌ Ошибка при получении статистики: {stats['error']}"
    else:
        # Форматируем статистику
        stats_text = f"""
<b>📊 Статистика бота КФЛ Лизинг</b>

👥 <b>Пользователи:</b>
• Всего взаимодействий: {stats.get('total_actions', 0)}
• Уникальных пользователей: {stats.get('unique_users', 0)}

<b>📈 Активность:</b>"""
        
        # Добавляем статистику по действиям
        actions_count = stats.get('actions_count', {})
        if actions_count:
            for action, count in sorted(actions_count.items()):
                stats_text += f"\n• {action}: {count}"
        
        stats_text += f"\n\n⏱️ <b>Последнее обновление:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    
    await update.message.reply_text(
        stats_text,
        parse_mode='HTML'
    )

async def parse_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Запуск режима парсинга Excel (администратор и лизинговая компания)."""
    user = update.effective_user
    user_data = {
        "id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name
    }

    if not await ensure_authenticated(update, context, reason="Запуск парсера"):
        return

    if not can_user_manage_ads(user.id):
        await update.message.reply_text(
            "⛔ Импорт Excel доступен только ролям «Администратор» и «Лизинговая компания»."
        )
        log_user_action(user_data, "parse_denied", "Попытка запуска парсера без прав")
        return

    if not is_parser_enabled():
        await update.message.reply_text(
            "⛔ Excel-парсер недоступен на этом сервере. "
            "Добавьте модули `parser.py`, `config_manager.py`, `column_mapper.py` в деплой."
        )
        log_user_action(user_data, "parse_unavailable", PARSER_IMPORT_ERROR or "parser module is missing")
        return

    context.user_data["parser_waiting_file"] = True
    log_user_action(user_data, "parse_start", "Ожидание Excel-файла для парсинга")

    await update.message.reply_text(
        "📥 Пришлите Excel-файл (`.xlsx`, `.xls` или `.xlsm`) одним документом.\n"
        "После загрузки я автоматически запущу парсер и пришлю `data.json` и `index.html`.",
        parse_mode="Markdown"
    )

async def parse_document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка загруженного Excel-файла и запуск парсера."""
    if not update.message or not update.message.document:
        return

    user = update.effective_user
    user_data = {
        "id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name
    }

    if not await ensure_authenticated(update, context, reason="Загрузка файла для парсинга"):
        return

    if not can_user_manage_ads(user.id):
        await update.message.reply_text(
            "⛔ Загрузка файлов для парсинга доступна только ролям «Администратор» и «Лизинговая компания»."
        )
        log_user_action(user_data, "parse_file_denied", "Попытка загрузки файла без прав")
        return

    if not is_parser_enabled():
        await update.message.reply_text(
            "⛔ Excel-парсер недоступен на этом сервере. "
            "Невозможно обработать файл без модулей `parser.py`, `config_manager.py`, `column_mapper.py`."
        )
        log_user_action(user_data, "parse_file_unavailable", PARSER_IMPORT_ERROR or "parser module is missing")
        return

    if not context.user_data.get("parser_waiting_file"):
        await update.message.reply_text(
            "Сначала запустите импорт через WebApp (кнопка «Парсер»), затем отправьте Excel-файл."
        )
        return

    document = update.message.document
    filename = document.file_name or "upload.xlsx"
    ext = Path(filename).suffix.lower()
    if ext not in {".xlsx", ".xls", ".xlsm"}:
        await update.message.reply_text("Нужен Excel-файл с расширением `.xlsx`, `.xls` или `.xlsm`.")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    PARSER_TMP_DIR.mkdir(parents=True, exist_ok=True)
    PARSER_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    input_path = PARSER_TMP_DIR / f"{timestamp}_{user.id}_{filename}"
    output_dir = PARSER_OUTPUT_DIR / f"{timestamp}_{user.id}"

    await update.message.reply_text("⏳ Файл получен, запускаю парсинг...")
    log_user_action(user_data, "parse_file_received", f"Файл: {filename}")

    try:
        tg_file = await context.bot.get_file(document.file_id)
        await tg_file.download_to_drive(custom_path=str(input_path))

        excel_file = pd.ExcelFile(input_path)
        sheets = excel_file.sheet_names
        if "зимние скидки" in sheets:
            sheet_name = "зимние скидки"
            header = 0
        else:
            sheet_name = sheets[0] if sheets else "Sheet1"
            header = 2

        df_headers = pd.read_excel(
            input_path,
            sheet_name=sheet_name,
            header=header,
            nrows=0
        )
        excel_columns = list(df_headers.columns)

        mapping = config_manager.get_mapping_template(input_path.stem)
        mapping_source = "template"

        if not mapping:
            mapping_source = "auto"
            config = config_manager.load_config()
            target_fields = list(config.get("fuzzy_keywords", {}).keys())
            auto_result = column_mapper.auto_map_columns(
                excel_columns,
                target_fields,
                config.get("fuzzy_keywords", {})
            )
            mapping = auto_result.get("mapping", {})
            is_valid, missing_critical = column_mapper.validate_mapping(mapping)
            if not is_valid:
                missing_str = ", ".join(missing_critical)
                raise ValueError(f"Не удалось сопоставить обязательные столбцы: {missing_str}")

        df = read_flexible(
            input_path,
            mapping=mapping,
            sheet_name=sheet_name,
            header=header
        )
        cards = prepare_cards(df)
        generate_site(cards, output_dir)
        excel_ads_count = replace_excel_ads(cards)

        data_path = output_dir / "data.json"
        index_path = output_dir / "index.html"

        summary = (
            f"✅ Парсинг завершён\n"
            f"• Файл: {filename}\n"
            f"• Записей: {len(df)}\n"
            f"• Карточек: {len(cards)}\n"
            f"• В общем фиде: {excel_ads_count}\n"
            f"• Маппинг: {mapping_source}"
        )
        await update.message.reply_text(summary)

        if data_path.exists():
            with open(data_path, "rb") as f:
                await update.message.reply_document(
                    document=f,
                    filename=f"data_{timestamp}.json"
                )
        if index_path.exists():
            with open(index_path, "rb") as f:
                await update.message.reply_document(
                    document=f,
                    filename=f"index_{timestamp}.html"
                )
        if ADS_FEED_FILE.exists():
            with open(ADS_FEED_FILE, "rb") as f:
                await update.message.reply_document(
                    document=f,
                    filename="ads_feed.json"
                )

        log_user_action(user_data, "parse_success", f"Файл обработан: {filename}, карточек: {len(cards)}")
    except Exception as e:
        logger.exception("Ошибка парсинга Excel")
        await update.message.reply_text(f"❌ Ошибка парсинга: {e}")
        log_user_action(user_data, "parse_error", f"Файл: {filename}, ошибка: {e}")
    finally:
        context.user_data["parser_waiting_file"] = False
        if input_path.exists():
            try:
                input_path.unlink()
            except Exception:
                logger.warning("Не удалось удалить временный файл: %s", input_path)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик нажатий на inline-кнопки (доступен всем)"""
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    user_data = {
        "id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name
    }

    if not is_user_authenticated(user.id):
        await prompt_authentication(update, context, reason=f"Нажатие кнопки: {query.data}")
        return
    
    # Логируем нажатие кнопки
    log_user_action(user_data, f"button_{query.data}", f"Нажата кнопка: {query.data}")
    
    if query.data == 'profile':
        await send_profile_card(update, context)
    
    elif query.data == 'rating':
        rating_text = """
<b>⭐ Рейтинг РА Эксперт 2022</b>

Мы гордимся, что <b>КФЛ Лизинг занимает 30 место</b> в рейтинге РА Эксперт по объему нового бизнеса в лизинге за 9 месяцев 2022 года.

<b>🏆 Наши достижения:</b>
• 📈 Более 10 лет стабильной работы на рынке
• ✅ 60+ актуальных предложений в каталоге
• 🏗️ 4 основные категории техники
• 💼 Индивидуальные условия лизинга для каждого клиента

<b>🔗 Ссылка на рейтинг:</b>
https://raexpert.ru/rankingtable/leasing/9m2022/main/
"""
        
        keyboard = [
            [InlineKeyboardButton("🌐 Открыть рейтинг", url="https://raexpert.ru/rankingtable/leasing/9m2022/main/")],
            [build_web_app_button(user.id)],
            [InlineKeyboardButton("◀️ Назад", callback_data='menu')]
        ]
        
        await query.edit_message_text(
            rating_text,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif query.data == 'contacts':
        contacts_text = """
<b>📞 Контакты КФЛ Лизинг</b>

<b>📍 Адрес:</b>
г. Новокузнецк

<b>☎️ Телефон:</b>
+7 (XXX) XXX-XX-XX

<b>📧 Email:</b>
info@kuzfl.ru

<b>🕒 Часы работы:</b>
Пн-Пт: 9:00 - 18:00
Сб: 10:00 - 16:00  
Вс: выходной

<b>👨‍💼 Для связи с менеджером:</b>
Просто отправьте сообщение в этот чат, и мы перезвоним вам в рабочее время.
"""
        
        keyboard = [
            [build_web_app_button(user.id)],
            [InlineKeyboardButton("📝 Оставить заявку", callback_data='request')],
            [InlineKeyboardButton("◀️ Назад", callback_data='menu')]
        ]
        
        await query.edit_message_text(
            contacts_text,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif query.data == 'about':
        about_text = """
<b>🏢 О компании КФЛ Лизинг</b>

<b>🎯 Наша миссия:</b>
Предоставлять клиентам доступ к качественной технике и оборудованию через удобные и прозрачные условия лизинга конфиската.

<b>📊 Что мы предлагаем:</b>
• Лизинг конфиската — техники и оборудования, изъятого у должников
• Юридическую проверку всех лотов
• Полное сопровождение сделки
• Индивидуальный подход к каждому клиенту
• Конкурентные ставки по лизингу

<b>✅ Наши гарантии:</b>
• Все предложения проверены юридически
• Подробные технические характеристики
• Честные цены без скрытых комиссий
• Профессиональная консультация
"""
        
        keyboard = [
            [build_web_app_button(user.id)],
            [InlineKeyboardButton("⭐ Наш рейтинг", callback_data='rating')],
            [InlineKeyboardButton("◀️ Назад", callback_data='menu')]
        ]
        
        await query.edit_message_text(
            about_text,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif query.data == 'manager':
        manager_text = """
<b>👨‍💼 Связь с менеджером</b>

Для связи с персональным менеджером и консультации по вопросам лизинга:

<b>☎️ Позвоните:</b>
+7 (XXX) XXX-XX-XX

<b>📧 Напишите:</b>
info@kuzfl.ru

<b>💬 Или оставьте заявку здесь:</b>
Просто отправьте сообщение с вашими контактными данными, и наш менеджер свяжется с вами в течение 30 минут в рабочее время.

<b>🕒 Работаем:</b>
Понедельник - Пятница: 9:00 - 18:00
"""
        
        keyboard = [
            [build_web_app_button(user.id)],
            [InlineKeyboardButton("◀️ Назад в меню", callback_data='menu')]
        ]
        
        await query.edit_message_text(
            manager_text,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif query.data == 'request':
        request_text = """
<b>📝 Заявка на консультацию</b>

Для оформления заявки на лизинг или получения консультации:

<b>1️⃣ Позвоните нам:</b>
+7 (XXX) XXX-XX-XX

<b>2️⃣ Напишите на email:</b>
info@kuzfl.ru

<b>3️⃣ Или оставьте заявку здесь:</b>
Отправьте сообщение с указанием:
• Вашего имени
• Контактного телефона  
• Интересуемой техники (если есть)

<b>⏱️ Мы перезвоним вам в течение 30 минут!</b>
"""
        
        keyboard = [
            [build_web_app_button(user.id)],
            [InlineKeyboardButton("📞 Контакты", callback_data='contacts')],
            [InlineKeyboardButton("◀️ Назад", callback_data='menu')]
        ]
        
        await query.edit_message_text(
            request_text,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif query.data == 'menu':
        await query.edit_message_text(
            "Главное меню:",
            parse_mode='HTML',
            reply_markup=build_main_menu_markup(user.id)
        )

def format_price(price: int) -> str:
    """Форматирование цены"""
    return f"{price:,}".replace(',', ' ') + " ₽"

async def handle_leasing_request(update: Update, context: ContextTypes.DEFAULT_TYPE, data: dict) -> None:
    """Обработка заявки на лизинг товара"""
    user = update.effective_user
    product = data.get('product', {})

    user_data = {
        "id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name
    }

    # Логируем заявку
    log_user_action(user_data, "leasing_request", f"Заявка на товар: {product.get('title', 'N/A')}")

    # Формируем сообщение для пользователя
    safe_first_name = escape_html_for_telegram(user.first_name) if user.first_name else "Пользователь"
    user_message = f"""
✅ <b>Заявка принята!</b>

Спасибо, {safe_first_name}! Ваша заявка на лизинг успешно отправлена.

<b>📦 Товар:</b> {escape_html_for_telegram(product.get('title', 'N/A'))}
<b>💰 Цена:</b> {format_price(product.get('price', 0))}

Наш менеджер свяжется с вами в ближайшее время для уточнения деталей.

<b>📞 Контакты:</b>
Телефон: +7 (913) 900-90-91
Email: info@kfl-leasing.ru
"""

    await update.message.reply_text(
        user_message,
        parse_mode='HTML',
        reply_markup=build_main_menu_markup(user.id)
    )

    # Отправляем уведомление администраторам
    admin_message = f"""
🔔 <b>НОВАЯ ЗАЯВКА НА ЛИЗИНГ</b>

<b>👤 Клиент:</b>
• Имя: {escape_html_for_telegram(user.first_name or '')} {escape_html_for_telegram(user.last_name or '')}
• Username: @{user.username or 'не указан'}
• ID: <code>{user.id}</code>

<b>📦 Товар:</b>
• Название: {escape_html_for_telegram(product.get('title', 'N/A'))}
• ID товара: {product.get('id', 'N/A')}
• Категория: {escape_html_for_telegram(product.get('category', 'N/A'))}
• Цена: {format_price(product.get('price', 0))}
• Год: {product.get('year', 'N/A')}
• Регион: {escape_html_for_telegram(product.get('region', 'N/A'))}

<b>⏰ Время:</b> {data.get('timestamp', 'N/A')}
"""

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=admin_message,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления админу {admin_id}: {e}")

async def handle_calculator_request(update: Update, context: ContextTypes.DEFAULT_TYPE, data: dict) -> None:
    """Обработка заявки из калькулятора лизинга"""
    user = update.effective_user

    user_data = {
        "id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name
    }

    # Логируем заявку
    log_user_action(user_data, "calculator_request", f"Заявка из калькулятора на сумму {data.get('price', 0)}")

    # Формируем сообщение для пользователя
    safe_first_name = escape_html_for_telegram(user.first_name) if user.first_name else "Пользователь"

    price = data.get('price', 0)
    advance_percent = data.get('advance', 20)
    term = data.get('term', 36)
    rate = data.get('rate', 12)

    advance_amount = data.get('advance_amount', 0)
    monthly_payment = data.get('monthly_payment', 0)
    total_amount = data.get('total_amount', 0)
    overpayment = data.get('overpayment', 0)

    user_message = f"""
✅ <b>Заявка из калькулятора принята!</b>

Спасибо, {safe_first_name}! Ваша заявка успешно отправлена.

<b>🧮 Параметры расчета:</b>
• Стоимость техники: {format_price(price)}
• Первоначальный взнос: {advance_percent}% ({format_price(advance_amount)})
• Срок лизинга: {term} месяцев
• Процентная ставка: {rate}%

<b>💳 Расчетные данные:</b>
• Ежемесячный платеж: {format_price(monthly_payment)}
• Переплата: {format_price(overpayment)}
• Общая сумма: {format_price(total_amount)}

Наш менеджер свяжется с вами для уточнения условий и подготовки индивидуального предложения.

<b>📞 Контакты:</b>
Телефон: +7 (913) 900-90-91
Email: info@kfl-leasing.ru
"""

    await update.message.reply_text(
        user_message,
        parse_mode='HTML',
        reply_markup=build_main_menu_markup(user.id)
    )

    # Отправляем уведомление администраторам
    admin_message = f"""
🧮 <b>НОВАЯ ЗАЯВКА ИЗ КАЛЬКУЛЯТОРА</b>

<b>👤 Клиент:</b>
• Имя: {escape_html_for_telegram(user.first_name or '')} {escape_html_for_telegram(user.last_name or '')}
• Username: @{user.username or 'не указан'}
• ID: <code>{user.id}</code>

<b>🧮 Параметры расчета:</b>
• Стоимость техники: {format_price(price)}
• Первоначальный взнос: {advance_percent}% ({format_price(advance_amount)})
• Срок лизинга: {term} месяцев
• Процентная ставка: {rate}%

<b>💳 Расчетные данные:</b>
• Ежемесячный платеж: {format_price(monthly_payment)}
• Переплата: {format_price(overpayment)}
• Общая сумма: {format_price(total_amount)}

<b>⏰ Время:</b> {data.get('timestamp', 'N/A')}
"""

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=admin_message,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления админу {admin_id}: {e}")

async def handle_new_advertisement(update: Update, context: ContextTypes.DEFAULT_TYPE, data: dict) -> None:
    """Обработка нового объявления от пользователя"""
    user = update.effective_user
    ad = data.get('ad', {})

    user_data = {
        "id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name
    }

    if not can_user_manage_ads(user.id):
        await update.message.reply_text(
            "⛔ Публикация объявлений доступна только ролям «Администратор» и «Лизинговая компания»."
        )
        log_user_action(user_data, "new_advertisement_denied", "Недостаточно прав на публикацию")
        return

    # Логируем новое объявление
    log_user_action(user_data, "new_advertisement", f"Новое объявление: {ad.get('title', 'N/A')}")
    saved_ad = add_manual_ad_to_feed(ad, user_data)

    # Формируем сообщение для пользователя
    safe_first_name = escape_html_for_telegram(user.first_name) if user.first_name else "Пользователь"
    user_message = f"""
✅ <b>Объявление опубликовано!</b>

Спасибо, {safe_first_name}! Ваше объявление добавлено в общий каталог.

<b>📦 Ваше объявление:</b>
• Название: {escape_html_for_telegram(ad.get('title', 'N/A'))}
• Цена: {format_price(ad.get('price', 0))}
• Категория: {escape_html_for_telegram(ad.get('category', 'N/A'))}
• ID в общем списке: <code>{escape_html_for_telegram(str(saved_ad.get('id', 'N/A')))}</code>

<b>📞 Вопросы?</b>
Телефон: +7 (913) 900-90-91
Email: info@kfl-leasing.ru
"""

    await update.message.reply_text(
        user_message,
        parse_mode='HTML',
        reply_markup=build_main_menu_markup(user.id)
    )

    # Отправляем уведомление администраторам
    category_emoji = {
        'spec': '🚜',
        'truck': '🚛',
        'passenger': '🚗',
        'equipment': '⚙️'
    }
    emoji = category_emoji.get(ad.get('category', ''), '📦')

    admin_message = f"""
📝 <b>НОВОЕ ОБЪЯВЛЕНИЕ ОТ ПОЛЬЗОВАТЕЛЯ</b>

<b>👤 Автор:</b>
• Имя: {escape_html_for_telegram(user.first_name or '')} {escape_html_for_telegram(user.last_name or '')}
• Username: @{user.username or 'не указан'}
• ID: <code>{user.id}</code>
• Контакт: {escape_html_for_telegram(ad.get('contact', 'N/A'))}

<b>{emoji} Объявление:</b>
• Название: {escape_html_for_telegram(ad.get('title', 'N/A'))}
• Категория: {escape_html_for_telegram(ad.get('category', 'N/A'))}
• Цена: {format_price(ad.get('price', 0))}
• Год: {ad.get('year', 'N/A')}
• Регион: {escape_html_for_telegram(ad.get('location', 'N/A'))}

<b>📄 Описание:</b>
{escape_html_for_telegram(ad.get('details', 'N/A')[:500])}

<b>📷 Фотографий:</b> {len(ad.get('images', [])) if isinstance(ad.get('images'), list) else 0}
<b>⏰ Время:</b> {ad.get('createdAt', 'N/A')}
"""

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=admin_message,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления админу {admin_id}: {e}")


async def handle_update_advertisement(update: Update, context: ContextTypes.DEFAULT_TYPE, data: dict) -> None:
    """Обновление существующего объявления из WebApp."""
    user = update.effective_user
    ad_id = str(data.get("ad_id") or "").strip()
    ad_updates = data.get("ad") if isinstance(data.get("ad"), dict) else {}

    if not user:
        return

    user_data = {
        "id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name
    }

    if not ad_id:
        await update.message.reply_text("⛔ Не передан ID объявления для обновления.")
        return

    role = get_user_role(user.id)
    updated_ad, error_message = update_manual_ad_in_feed(ad_id, ad_updates, user_data, role)
    if not updated_ad:
        await update.message.reply_text(f"⛔ {error_message}")
        log_user_action(user_data, "update_advertisement_denied", f"ad_id={ad_id}, reason={error_message}")
        return

    log_user_action(user_data, "update_advertisement", f"ad_id={ad_id}")
    await update.message.reply_text(
        "✅ Объявление обновлено.",
        reply_markup=build_main_menu_markup(user.id)
    )


async def handle_delete_advertisement(update: Update, context: ContextTypes.DEFAULT_TYPE, data: dict) -> None:
    """Удаление объявления из WebApp."""
    user = update.effective_user
    ad_id = str(data.get("ad_id") or "").strip()

    if not user:
        return

    user_data = {
        "id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name
    }

    if not ad_id:
        await update.message.reply_text("⛔ Не передан ID объявления для удаления.")
        return

    role = get_user_role(user.id)
    deleted, error_message = delete_manual_ad_from_feed(ad_id, user_data, role)
    if not deleted:
        await update.message.reply_text(f"⛔ {error_message}")
        log_user_action(user_data, "delete_advertisement_denied", f"ad_id={ad_id}, reason={error_message}")
        return

    log_user_action(user_data, "delete_advertisement", f"ad_id={ad_id}")
    await update.message.reply_text(
        "✅ Объявление удалено.",
        reply_markup=build_main_menu_markup(user.id)
    )


async def web_app_data_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик данных из Web App"""
    try:
        if not await ensure_authenticated(update, context, reason="Данные из WebApp"):
            return

        # Получаем данные из Web App
        web_app_data = update.message.web_app_data.data
        data = json.loads(web_app_data)

        logger.info(f"Получены данные из WebApp: {data}")

        action = data.get('action')

        if action == 'leasing_request':
            await handle_leasing_request(update, context, data)
        elif action == 'calculator_request':
            await handle_calculator_request(update, context, data)
        elif action == 'new_advertisement':
            await handle_new_advertisement(update, context, data)
        elif action == 'update_advertisement':
            await handle_update_advertisement(update, context, data)
        elif action == 'delete_advertisement':
            await handle_delete_advertisement(update, context, data)
        elif action == 'parse_request':
            await parse_command(update, context)
        else:
            logger.warning(f"Неизвестное действие из WebApp: {action}")
            await update.message.reply_text(
                "Получены данные, но действие не распознано. Попробуйте еще раз.",
                reply_markup=build_main_menu_markup(update.effective_user.id if update.effective_user else None)
            )

    except json.JSONDecodeError as e:
        logger.error(f"Ошибка парсинга JSON из WebApp: {e}")
        await update.message.reply_text(
            "Произошла ошибка при обработке данных. Попробуйте еще раз.",
            reply_markup=build_main_menu_markup(update.effective_user.id if update.effective_user else None)
        )
    except Exception as e:
        logger.error(f"Ошибка обработки данных из WebApp: {e}")
        await update.message.reply_text(
            "Произошла ошибка. Пожалуйста, попробуйте позже или свяжитесь с поддержкой.",
            reply_markup=build_main_menu_markup(update.effective_user.id if update.effective_user else None)
        )

async def unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик неизвестных команд (доступен всем)"""
    user = update.effective_user
    user_data = {
        "id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name
    }

    # Логируем неизвестную команду
    log_user_action(user_data, "unknown_command", f"Неизвестная команда: {update.message.text}")

    if not await ensure_authenticated(update, context, reason="Неизвестная команда"):
        return

    await update.message.reply_text(
        "Извините, я не понимаю эту команду. Используйте /help для просмотра доступных команд.",
        reply_markup=build_main_menu_markup(user.id if user else None)
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик обычных текстовых сообщений (доступен всем)"""
    user = update.effective_user
    message_text = update.message.text
    
    user_data = {
        "id": user.id,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name
    }

    if not await ensure_authenticated(update, context, reason="Обычное сообщение"):
        return
    
    log_user_action(user_data, "message", f"Сообщение пользователя: {message_text[:50]}...")
    
    # Экранируем имя пользователя для безопасного использования в HTML
    safe_first_name = escape_html_for_telegram(user.first_name) if user.first_name else "Пользователь"
    safe_message_text = escape_html_for_telegram(message_text[:100]) if message_text else ""
    
    response_text = f"""
<b>📩 Ваше сообщение получено!</b>

Привет, {safe_first_name}! Мы получили ваше сообщение:
"{safe_message_text}..."

Наш менеджер свяжется с вами в ближайшее время.

А пока вы можете:
• 📱 Открыть каталог техники
• ⭐ Узнать о наших достижениях
• 📞 Посмотреть контакты
"""
    
    await update.message.reply_text(
        response_text,
        parse_mode='HTML',
        reply_markup=build_main_menu_markup(user.id if user else None)
    )

def main() -> None:
    """Основная функция запуска бота"""
    # Создаем приложение
    application = Application.builder().token(TOKEN).build()

    # Регистрируем обработчики команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("catalogue", catalogue))
    application.add_handler(CommandHandler("login", login_command))
    application.add_handler(CommandHandler("profile", profile_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("stats", stats_command))  # Только здесь проверка админа
    application.add_handler(CallbackQueryHandler(button_handler))

    # Обработчик данных из Web App (должен быть перед обычными сообщениями)
    application.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, web_app_data_handler))
    application.add_handler(MessageHandler(filters.CONTACT, contact_auth_handler))
    application.add_handler(MessageHandler(filters.Document.ALL, parse_document_handler))

    # Обработчик обычных сообщений
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Обработчик для неизвестных команд (должен быть последним)
    application.add_handler(MessageHandler(filters.COMMAND, unknown))
    
    # Запускаем бота
    logger.info("Бот КФЛ Лизинг запущен!")
    logger.info("Build version: %s", BOT_BUILD_VERSION)
    logger.info(f"Логи пользователей сохраняются в: {USERS_LOG_FILE}")
    logger.info(f"Файл аутентификации: {AUTH_USERS_FILE}")
    logger.info(f"Администратор: ID {ADMIN_IDS[0]}")
    logger.info("Доступ к командам открыт только после аутентификации через контакт")
    logger.info("Команда /stats доступна только администратору")
    if backend_sync_enabled():
        logger.info("Backend sync: ENABLED (%s)", DJANGO_BACKEND_URL)
    else:
        logger.info("Backend sync: disabled (set DJANGO_BACKEND_URL + DJANGO_BACKEND_API_KEY)")
    if is_parser_enabled():
        logger.info("Excel parser: ENABLED")
    else:
        logger.warning("Excel parser: disabled (%s)", PARSER_IMPORT_ERROR or "module not found")
    
    # Выводим информацию о существующей статистике
    stats = get_user_stats()
    if "error" not in stats:
        logger.info(f"Загружена существующая статистика: {stats.get('unique_users', 0)} уникальных пользователей")
    
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
