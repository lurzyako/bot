# KFL Bot + Django Backend

В проект добавлен Django backend, который синхронизирует данные из Telegram-бота и позволяет смотреть их в MySQL через phpMyAdmin.

## Что уже реализовано

- Роли пользователей в боте уже есть: `user`, `leasing_company`, `admin`.
- Django API для синхронизации:
  - `POST /api/users/upsert/`
  - `GET /api/users/<telegram_id>/role/`
  - `POST /api/actions/`
  - `POST /api/ads/upsert/`
  - `POST /api/ads/bulk-upsert/`
  - `POST /api/ads/update/` (с проверкой прав)
  - `POST /api/ads/delete/` (с проверкой прав)
- Бот теперь пишет данные в JSON (как раньше) и параллельно в Django backend (если заданы env-переменные).

## 1. Установка зависимостей

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2. Настройка окружения

```bash
cp .env.example .env
```

Заполните значения в `.env`:
- `BOT_TOKEN`
- `DJANGO_BACKEND_API_KEY` и `DJANGO_BOT_API_KEY` (должны быть одинаковыми)
- MySQL-параметры (`MYSQL_*`)

## 3. Создание БД MySQL

Создайте базу, например:

```sql
CREATE DATABASE kfl_bot CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

## 4. Запуск Django backend

```bash
cd backend
export $(grep -v '^#' ../.env | xargs)
python manage.py migrate
python manage.py createsuperuser
python manage.py runserver 0.0.0.0:8000
```

## 5. (Опционально) Импорт старых JSON в БД

```bash
cd backend
export $(grep -v '^#' ../.env | xargs)
python manage.py import_bot_json --project-root ..
```

## 6. Запуск Telegram-бота

В новом терминале:

```bash
cd /Users/lurzyako/Desktop/untitled\ folder
export $(grep -v '^#' .env | xargs)
python3 bot.py
```

## 7. Просмотр через phpMyAdmin

Подключитесь к MySQL и откройте БД `kfl_bot`.
Основные таблицы:
- `core_telegramuser`
- `core_useraction`
- `core_aditem`

## Роли пользователей

Да, разделение уже реализовано в боте:
- `Пользователь` (`user`)
- `Лизинговая компания` (`leasing_company`)
- `Администратор` (`admin`)

В Django можно менять роль пользователя в таблице `core_telegramuser` (поле `role`). Бот будет читать роль из backend (если синхронизация включена).

## Ограничения прав по объявлениям

- `leasing_company` может редактировать и удалять только объявления, где `author_telegram_id` совпадает с его Telegram ID.
- `admin` может редактировать и удалять любые объявления.
- Проверка выполняется в боте и в Django API (`/api/ads/update/`, `/api/ads/delete/`).
