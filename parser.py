#!/usr/bin/env python3
"""
Парсер Excel-файлов для создания каталога транспортных средств.
Читает два файла со стоком ТС и генерирует:
  - data.json — все карточки
  - index.html — одностраничное приложение, которое динамически подгружает данные
"""

import os
import re
import sys
import math
import json
import subprocess
import threading
import webbrowser
try:
    import tkinter as tk
    from tkinter import filedialog, messagebox, scrolledtext
except ImportError:
    tk = None
    filedialog = None
    messagebox = None
    scrolledtext = None
import pandas as pd
from pathlib import Path

# ─── Конфигурация ────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"

FILE1 = BASE_DIR / "Актуальный_сток_ИЗТ_ГПБА_от_04_02_2026_МЛ.xlsx"
FILE2 = BASE_DIR / "Копия СТОК Зимняя выгода.xlsx"


# ─── Чтение и нормализация данных ────────────────────────────────────────────

def read_file1(path: Path) -> pd.DataFrame:
    """Читает основной файл стока."""
    df = pd.read_excel(path, sheet_name="Sheet1", header=2)
    df = df.rename(columns={
        "Код предложения": "code",
        "Категория ТС": "category",
        "Статус ИЗТ": "status",
        "Марка": "brand",
        "Модель": "model",
        "Модификация": "modification",
        "Цвет кузова": "color",
        "Состояние ПЛ": "condition",
        "VIN": "vin",
        "Тип ТС": "vehicle_type",
        "Год выпуска": "year",
        "Пробег": "mileage",
        "СРС": "price_original",
        "Переоценка": "price_revaluation",
        "СРС с переоценкой": "price",
        "Количество ключей после изъятия": "keys",
        "Тип ПТС/ЭПТС": "pts_type",
        "Федеральный округ": "federal_district",
        "Адрес стоянки": "address",
        "Кол-во дней в реализации": "days_on_sale",
        "Фото и видео материалы ТС": "photo_url",
        "Комментарий по оценке": "comment",
    })
    df["code"] = df["code"].astype(str).str.strip()
    df["source"] = "stock"
    df["discount_pct"] = None
    df["discount_price"] = None
    return df


def read_file2(path: Path) -> pd.DataFrame:
    """Читает файл зимних скидок."""
    df = pd.read_excel(path, sheet_name="зимние скидки")
    df = df.rename(columns={
        "Код предложения": "code",
        "% скидки": "discount_pct",
        "Минимальная цена со скидкой": "discount_price",
        "Категория ТС": "category",
        "Марка": "brand",
        "Модель": "model",
        "Модификация": "modification",
        "Цвет кузова": "color",
        "Состояние ПЛ": "condition",
        "Комплектность ТС": "completeness",
        "VIN": "vin",
        "Тип ТС": "vehicle_type",
        "Год выпуска": "year",
        "Пробег": "mileage",
        "СРС с переоценкой": "price",
        "Тип ПТС/ЭПТС": "pts_type",
        "Федеральный округ": "federal_district",
        "Адрес стоянки": "address",
        "Аккредитация стоянки": "accreditation",
        "Кол-во дней в реализации": "days_on_sale",
        "Фото и видео материалы ТС": "photo_url",
        "Комментарий по оценке": "comment",
    })
    df["code"] = df["code"].astype(str).str.strip()
    df["source"] = "winter_sale"
    df["price_original"] = None
    df["price_revaluation"] = None
    df["status"] = "Зимняя выгода"
    df["keys"] = None
    return df


def handle_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Обрабатывает дублирующиеся столбцы, добавляя суффиксы."""
    cols = pd.Series(df.columns)

    for dup in cols[cols.duplicated()].unique():
        dup_indices = cols[cols == dup].index.tolist()
        for i, idx in enumerate(dup_indices[1:], start=1):
            cols[idx] = f"{dup}_{i}"

    df.columns = cols
    return df


def safe_convert_year(value):
    """Безопасное преобразование года."""
    if pd.isna(value):
        return None
    try:
        year = int(float(value))
        if 1900 <= year <= 2100:
            return str(year)
        return None
    except (ValueError, TypeError):
        return None


def safe_convert_price(value):
    """Безопасное преобразование цены."""
    if pd.isna(value):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def read_flexible(
    path: Path,
    mapping: dict,
    sheet_name: str = None,
    header: int = None
) -> pd.DataFrame:
    """
    Универсальная функция чтения Excel с гибким маппингом.

    Args:
        path: Путь к Excel файлу
        mapping: Словарь маппинга {excel_column: target_field}
        sheet_name: Название листа (auto-detect если None)
        header: Строка с заголовками (auto-detect если None)
вы
    Returns:
        pd.DataFrame с нормализованными столбцами
    """
    # Автоопределение sheet и header если не указаны
    if sheet_name is None or header is None:
        sheets = pd.ExcelFile(path).sheet_names
        if "зимние скидки" in sheets:
            sheet_name = "зимние скидки"
            header = 0
        else:
            sheet_name = sheets[0] if sheets else "Sheet1"
            header = 2  # Для стандартных файлов стока

    # Читаем Excel
    df = pd.read_excel(path, sheet_name=sheet_name, header=header)

    # Обрабатываем дублирующиеся столбцы
    df = handle_duplicate_columns(df)

    # Применяем маппинг
    df = df.rename(columns=mapping)

    # Нормализация code
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.strip()

    # Добавляем все целевые поля, которых нет
    target_fields = [
        "code", "category", "status", "brand", "model", "modification",
        "color", "condition", "vin", "vehicle_type", "year", "mileage",
        "price", "price_original", "price_revaluation", "keys", "pts_type",
        "federal_district", "address", "days_on_sale", "photo_url",
        "comment", "discount_pct", "discount_price"
    ]

    for field in target_fields:
        if field not in df.columns:
            df[field] = None

    # Безопасная конвертация типов
    if "year" in df.columns:
        df["year"] = df["year"].apply(safe_convert_year)

    if "price" in df.columns:
        df["price"] = df["price"].apply(safe_convert_price)

    if "price_original" in df.columns:
        df["price_original"] = df["price_original"].apply(safe_convert_price)

    if "price_revaluation" in df.columns:
        df["price_revaluation"] = df["price_revaluation"].apply(safe_convert_price)

    if "discount_price" in df.columns:
        df["discount_price"] = df["discount_price"].apply(safe_convert_price)

    # Определяем тип источника (stock или winter_sale)
    # на основе наличия специфических полей
    if "discount_pct" in list(mapping.values()):
        # Это файл зимних скидок
        df["source"] = "winter_sale"
        if df["status"].isna().all():
            df["status"] = "Зимняя выгода"
        if df["price_original"].isna().all():
            df["price_original"] = None
        if df["price_revaluation"].isna().all():
            df["price_revaluation"] = None
    else:
        # Это обычный файл стока
        df["source"] = "stock"
        if df["discount_pct"].isna().all():
            df["discount_pct"] = None
        if df["discount_price"].isna().all():
            df["discount_price"] = None

    return df


def merge_data(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """Объединяет данные из обоих файлов, помечая товары со скидкой."""
    cols = [
        "code", "source", "brand", "model", "modification", "color",
        "condition", "vin", "vehicle_type", "year", "mileage",
        "price", "price_original", "discount_pct", "discount_price",
        "keys", "pts_type", "federal_district", "address",
        "days_on_sale", "photo_url", "comment", "status", "category",
    ]

    for c in cols:
        if c not in df1.columns:
            df1[c] = None
        if c not in df2.columns:
            df2[c] = None

    combined = pd.concat([df1[cols], df2[cols]], ignore_index=True)
    combined = combined.sort_values("source", ascending=False)
    combined = combined.drop_duplicates(subset=["code"], keep="first")
    combined = combined.sort_values("code").reset_index(drop=True)

    return combined


# ─── Форматирование данных для карточек ──────────────────────────────────────

def format_price(value) -> str:
    if pd.isna(value) or value is None or value == "":
        return "Цена по запросу"
    try:
        v = int(float(value))
        return f"{v:,}".replace(",", " ") + " ₽"
    except (ValueError, TypeError):
        return "Цена по запросу"


def format_mileage(value) -> str:
    if pd.isna(value) or value is None or value == "":
        return "н/д"
    try:
        v = int(float(value))
        if v == 0:
            return "Без пробега"
        return f"{v:,}".replace(",", " ") + " км"
    except (ValueError, TypeError):
        return "н/д"


def clean_text(value) -> str:
    if pd.isna(value) or value is None:
        return ""
    return str(value).strip()


def is_valid_url(url: str) -> bool:
    if not url:
        return False
    if not (url.startswith("http://") or url.startswith("https://")):
        return False
    # Отсекаем мусорные URL вроде "https://." или "https://ТЕСТ"
    domain = url.split("//", 1)[-1].split("/", 1)[0]
    return "." in domain and len(domain) > 3


def make_short_description(row) -> str:
    parts = []
    mod = clean_text(row.get("modification"))
    if mod:
        parts.append(mod)
    cond = clean_text(row.get("condition"))
    if cond:
        parts.append(f"Состояние: {cond.lower()}")
    keys = clean_text(row.get("keys"))
    if keys:
        parts.append(f"Ключи: {keys.lower()}")
    return ". ".join(parts)


def prepare_cards(df: pd.DataFrame) -> list[dict]:
    """Преобразует DataFrame в список словарей для JSON."""
    cards = []
    for _, row in df.iterrows():
        brand = clean_text(row.get("brand"))
        model = clean_text(row.get("model"))
        if not brand and not model:
            continue

        title = f"{brand} {model}".strip()
        year = clean_text(row.get("year"))
        if year:
            title += f" ({year})"

        photo = clean_text(row.get("photo_url"))
        has_photo = is_valid_url(photo)

        price_str = format_price(row.get("price"))
        discount_price_str = ""
        discount_pct_str = ""
        has_discount = False

        if pd.notna(row.get("discount_pct")) and row.get("discount_pct"):
            try:
                pct = float(row["discount_pct"]) * 100
                if pct > 0:
                    has_discount = True
                    discount_pct_str = f"-{pct:.0f}%"
                    discount_price_str = format_price(row.get("discount_price"))
            except (ValueError, TypeError):
                pass

        color = clean_text(row.get("color"))
        vehicle_type = clean_text(row.get("vehicle_type"))
        mileage = format_mileage(row.get("mileage"))
        address = clean_text(row.get("address"))
        federal_district = clean_text(row.get("federal_district"))
        location = federal_district
        if address:
            parts = address.split(",")
            if len(parts) >= 2:
                location = parts[1].strip()
            else:
                location = address[:60]

        comment = clean_text(row.get("comment"))
        short_desc = make_short_description(row)
        vin = clean_text(row.get("vin"))
        code = clean_text(row.get("code"))
        status = clean_text(row.get("status"))

        cards.append({
            "code": code,
            "title": title,
            "brand": brand,
            "model": model,
            "year": year,
            "color": color.capitalize() if color else "",
            "vehicle_type": vehicle_type,
            "mileage": mileage,
            "price": price_str,
            "has_discount": has_discount,
            "discount_pct": discount_pct_str,
            "discount_price": discount_price_str,
            "original_price": price_str if has_discount else "",
            "photo_url": photo if has_photo else "",
            "has_photo": has_photo,
            "location": location,
            "address": address,
            "vin": vin,
            "comment": comment,
            "short_desc": short_desc,
            "status": status,
        })

    return cards


# ─── HTML-шаблон (одностраничное приложение) ─────────────────────────────────

HTML_PAGE = """\
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Каталог транспортных средств</title>
<style>
  :root {
    --primary: #2563eb;
    --primary-dark: #1d4ed8;
    --primary-light: #dbeafe;
    --accent: #dc2626;
    --accent-light: #fef2f2;
    --green: #16a34a;
    --bg: #f8fafc;
    --card-bg: #ffffff;
    --text: #0f172a;
    --text-mid: #475569;
    --text-light: #94a3b8;
    --border: #e2e8f0;
    --shadow: 0 1px 3px rgba(0,0,0,.06), 0 1px 2px rgba(0,0,0,.04);
    --shadow-md: 0 4px 6px rgba(0,0,0,.07), 0 2px 4px rgba(0,0,0,.04);
    --shadow-lg: 0 10px 25px rgba(0,0,0,.08), 0 4px 10px rgba(0,0,0,.04);
    --radius: 14px;
    --radius-sm: 8px;
  }
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family: -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; background:var(--bg); color:var(--text); line-height:1.5; }

  /* ── Filters ── */
  .filters { max-width:1400px; margin:1.2rem auto .5rem; padding:0 1rem; display:flex; flex-wrap:wrap; gap:.5rem; }
  .filters input,.filters select { padding:.5rem .8rem; border:1px solid var(--border); border-radius:var(--radius-sm); font-size:.88rem; background:#fff; outline:none; transition:border .15s,box-shadow .15s; }
  .filters input:focus,.filters select:focus { border-color:var(--primary); box-shadow:0 0 0 3px var(--primary-light); }
  .filters #search { flex:1 1 240px; }
  .filters #priceMin,.filters #priceMax { flex:0 1 140px; }
  .filters #sortBy { flex:0 1 160px; }
  .filters select { flex:0 1 170px; }

  .stats { max-width:1400px; margin:0 auto .6rem; padding:0 1rem; font-size:.82rem; color:var(--text-light); }

  /* ── Loading ── */
  .loading { text-align:center; padding:4rem 1rem; color:var(--text-light); font-size:1rem; }
  .loading .spinner { width:36px; height:36px; border:4px solid var(--border); border-top-color:var(--primary); border-radius:50%; animation:spin .7s linear infinite; margin:0 auto 1rem; }

  /* ── Grid ── */
  .grid { max-width:1400px; margin:0 auto; padding:0 1rem 2rem; display:grid; grid-template-columns:repeat(4,1fr); gap:1rem; }

  /* ── Card ── */
  .card { background:var(--card-bg); border-radius:var(--radius); overflow:hidden; box-shadow:var(--shadow); transition:transform .18s,box-shadow .18s; display:flex; flex-direction:column; cursor:pointer; }
  .card:hover { transform:translateY(-4px); box-shadow:var(--shadow-lg); }

  /* ── Photo Preview (на карточках) ── */
  .photo-preview { width:100%; aspect-ratio:4/3; background:#e2e8f0; position:relative; overflow:hidden; user-select:none; }
  .photo-preview img { width:100%; height:100%; object-fit:contain; }
  .photo-preview .img-loader { position:absolute; inset:0; display:flex; align-items:center; justify-content:center; flex-direction:column; color:var(--text-light); font-size:.82rem; }
  .photo-preview .no-photo { display:flex; align-items:center; justify-content:center; flex-direction:column; width:100%; height:100%; color:var(--text-light); font-size:.82rem; }
  .photo-preview .no-photo svg { margin-bottom:.4rem; opacity:.35; }
  .photo-preview .photo-count { position:absolute; bottom:8px; right:8px; background:rgba(0,0,0,.7); color:#fff; font-size:.7rem; padding:3px 8px; border-radius:10px; z-index:3; pointer-events:none; font-weight:600; }
  .spinner { width:26px; height:26px; border:3px solid var(--border); border-top-color:var(--primary); border-radius:50%; animation:spin .7s linear infinite; margin-bottom:.4rem; }
  @keyframes spin { to { transform:rotate(360deg); } }

  /* Badges */
  .badge { position:absolute; top:8px; padding:3px 8px; border-radius:6px; font-size:.72rem; font-weight:700; z-index:4; line-height:1.3; }
  .badge-discount { left:8px; background:var(--accent); color:#fff; }
  .badge-status  { right:8px; background:rgba(0,0,0,.6); color:#fff; backdrop-filter:blur(4px); }

  /* ── Card Body ── */
  .card-body { padding:.6rem .7rem .5rem; flex:1; display:flex; flex-direction:column; }
  .card-type { font-size:.62rem; color:var(--text-light); text-transform:uppercase; letter-spacing:.06em; font-weight:600; margin-bottom:.15rem; }
  .card-title { font-size:.88rem; font-weight:700; line-height:1.25; margin-bottom:.35rem; color:var(--text); display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; }

  .card-meta { display:flex; flex-wrap:wrap; gap:.25rem; margin-bottom:.4rem; }
  .meta-chip { display:inline-flex; align-items:center; gap:3px; padding:2px 6px; background:var(--bg); border-radius:5px; font-size:.68rem; color:var(--text-mid); }
  .meta-chip svg { flex-shrink:0; }
  .color-dot { width:10px; height:10px; border-radius:50%; border:1px solid var(--border); flex-shrink:0; }

  .card-desc { font-size:.72rem; color:var(--text-light); line-height:1.4; flex:1; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; margin-bottom:.35rem; }

  /* ── Card Footer ── */
  .card-footer { padding:.5rem .7rem; border-top:1px solid var(--border); display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:.2rem; }
  .price-block { display:flex; align-items:baseline; gap:.3rem; flex-wrap:wrap; }
  .price-current { font-size:.95rem; font-weight:800; color:var(--primary); }
  .price-current.sale { color:var(--accent); }
  .price-old { font-size:.72rem; color:var(--text-light); text-decoration:line-through; }
  .card-location { display:flex; align-items:center; gap:3px; font-size:.66rem; color:var(--text-light); }
  .card-location svg { flex-shrink:0; opacity:.6; }

  /* ── Pagination ── */
  .pager { max-width:1400px; margin:0 auto 3rem; padding:0 1rem; display:flex; justify-content:center; align-items:center; gap:6px; flex-wrap:wrap; }
  .pager button,.pager span { display:inline-flex; align-items:center; justify-content:center; min-width:36px; height:36px; padding:0 10px; border-radius:var(--radius-sm); font-size:.85rem; font-weight:500; transition:all .15s; border:1px solid var(--border); cursor:pointer; font-family:inherit; }
  .pager button { background:#fff; color:var(--text-mid); }
  .pager button:hover { background:var(--primary); color:#fff; border-color:var(--primary); }
  .pager .cur { background:var(--primary); color:#fff; border-color:var(--primary); font-weight:700; cursor:default; }
  .pager .ell { color:var(--text-light); border:none; min-width:24px; pointer-events:none; cursor:default; }
  .pager .nav-btn { padding:0 14px; font-weight:600; }

  /* ── Modal ── */
  .modal-overlay { display:none; position:fixed; inset:0; background:rgba(0,0,0,.55); z-index:1000; align-items:center; justify-content:center; padding:1rem; backdrop-filter:blur(3px); }
  .modal-overlay.active { display:flex; }
  .modal { background:#fff; border-radius:var(--radius); max-width:720px; width:100%; max-height:92vh; overflow-y:auto; position:relative; }
  .modal-close { position:absolute; top:12px; right:12px; background:rgba(0,0,0,.45); border:none; width:34px; height:34px; border-radius:50%; font-size:1.3rem; cursor:pointer; color:#fff; display:flex; align-items:center; justify-content:center; z-index:10; transition:background .15s; }
  .modal-close:hover { background:rgba(0,0,0,.7); }

  /* Modal Gallery (как на drom.ru) */
  .modal-gallery { width:100%; border-radius:var(--radius) var(--radius) 0 0; background:#f8fafc; }

  /* Главное фото */
  .modal-gallery-main { width:100%; aspect-ratio:16/10; background:#e2e8f0; position:relative; overflow:hidden; }
  .modal-gallery-main img { width:100%; height:100%; object-fit:contain; }
  .modal-gallery-main .arr { position:absolute; top:50%; transform:translateY(-50%); width:40px; height:40px; border-radius:50%; background:rgba(255,255,255,.95); border:none; cursor:pointer; display:flex; align-items:center; justify-content:center; box-shadow:var(--shadow-lg); z-index:5; font-size:1.3rem; color:var(--text); transition:all .15s; }
  .modal-gallery-main .arr:hover { background:#fff; transform:translateY(-50%) scale(1.1); }
  .modal-gallery-main .arr-l { left:12px; }
  .modal-gallery-main .arr-r { right:12px; }
  .modal-gallery-main .photo-counter { position:absolute; bottom:12px; right:12px; background:rgba(0,0,0,.7); color:#fff; font-size:.78rem; padding:4px 10px; border-radius:8px; z-index:5; font-weight:600; }
  .modal-gallery-main .m-loader { position:absolute; inset:0; display:flex; align-items:center; justify-content:center; flex-direction:column; color:var(--text-light); font-size:.88rem; }
  .modal-gallery-main .m-no-photo { display:flex; align-items:center; justify-content:center; width:100%; height:100%; color:var(--text-light); font-size:.88rem; gap:8px; flex-direction:column; }

  /* Галерея миниатюр */
  .modal-gallery-thumbs { padding:12px; display:flex; gap:8px; overflow-x:auto; scroll-behavior:smooth; }
  .modal-gallery-thumbs::-webkit-scrollbar { height:6px; }
  .modal-gallery-thumbs::-webkit-scrollbar-track { background:var(--border); border-radius:3px; }
  .modal-gallery-thumbs::-webkit-scrollbar-thumb { background:var(--text-light); border-radius:3px; }
  .modal-gallery-thumbs::-webkit-scrollbar-thumb:hover { background:var(--text-mid); }
  .modal-gallery-thumb { flex:0 0 80px; height:60px; border-radius:6px; overflow:hidden; cursor:pointer; border:2px solid transparent; transition:all .15s; background:#e2e8f0; }
  .modal-gallery-thumb:hover { border-color:var(--primary); transform:scale(1.05); }
  .modal-gallery-thumb.active { border-color:var(--primary); box-shadow:0 0 0 1px var(--primary); }
  .modal-gallery-thumb img { width:100%; height:100%; object-fit:cover; }

  /* Modal body */
  .modal-body { padding:1.2rem 1.4rem 1.4rem; }
  .modal-header { display:flex; align-items:flex-start; justify-content:space-between; gap:.8rem; margin-bottom:.8rem; flex-wrap:wrap; }
  .modal-header h2 { font-size:1.2rem; font-weight:800; line-height:1.3; flex:1; }
  .modal-status { display:inline-block; padding:3px 10px; border-radius:6px; font-size:.72rem; font-weight:700; background:var(--bg); color:var(--text-mid); white-space:nowrap; }
  .modal-status.sale { background:var(--accent-light); color:var(--accent); }

  .modal-price-row { display:flex; align-items:baseline; gap:.6rem; flex-wrap:wrap; margin-bottom:1rem; padding-bottom:.8rem; border-bottom:1px solid var(--border); }
  .modal-price { font-size:1.4rem; font-weight:800; color:var(--primary); }
  .modal-price.sale { color:var(--accent); }
  .modal-price-old { font-size:1rem; color:var(--text-light); text-decoration:line-through; }
  .modal-discount-badge { background:var(--accent); color:#fff; padding:2px 8px; border-radius:5px; font-size:.78rem; font-weight:700; }

  .modal-specs { display:grid; grid-template-columns:1fr 1fr; gap:0; margin-bottom:1rem; border:1px solid var(--border); border-radius:var(--radius-sm); overflow:hidden; }
  .spec-item { padding:.55rem .8rem; border-bottom:1px solid var(--border); display:flex; flex-direction:column; }
  .spec-item:nth-child(odd) { border-right:1px solid var(--border); }
  .spec-item:nth-last-child(-n+2) { border-bottom:none; }
  .spec-label { font-size:.7rem; color:var(--text-light); text-transform:uppercase; letter-spacing:.04em; font-weight:600; margin-bottom:1px; }
  .spec-value { font-size:.88rem; font-weight:600; color:var(--text); word-break:break-word; }

  .modal-section { margin-bottom:1rem; }
  .modal-section-title { font-size:.75rem; color:var(--text-light); text-transform:uppercase; letter-spacing:.05em; font-weight:700; margin-bottom:.4rem; }
  .modal-comment { background:var(--bg); border-radius:var(--radius-sm); padding:.8rem 1rem; font-size:.85rem; line-height:1.7; color:var(--text-mid); white-space:pre-wrap; word-break:break-word; }
  .modal-address { font-size:.85rem; color:var(--text-mid); line-height:1.5; }
  .modal-link { display:inline-flex; align-items:center; gap:6px; margin-top:.6rem; padding:.5rem 1rem; background:var(--primary); color:#fff; border-radius:var(--radius-sm); font-size:.85rem; font-weight:600; text-decoration:none; transition:background .15s; }
  .modal-link:hover { background:var(--primary-dark); }

  @media (max-width:1100px) {
    .grid { grid-template-columns:repeat(3,1fr); }
  }
  @media (max-width:820px) {
    .grid { grid-template-columns:repeat(2,1fr); }
  }
  @media (max-width:700px) {
    .grid { grid-template-columns:1fr; }
    .photo-preview { aspect-ratio:4/3; }
    .modal-gallery-main { aspect-ratio:4/3; }
    .modal-body { padding:1rem; }
    .modal-specs { grid-template-columns:1fr; }
    .spec-item:nth-child(odd) { border-right:none; }
    .spec-item { border-bottom:1px solid var(--border); }
    .spec-item:last-child { border-bottom:none; }
    .modal-gallery-thumbs { padding:8px; }
    .modal-gallery-thumb { flex:0 0 60px; height:45px; }
  }
</style>
</head>
<body>

<div class="filters">
  <input type="text" id="search" placeholder="Поиск по марке, модели, VIN..." oninput="applyFilters()">
  <select id="filterBrand" onchange="applyFilters()"><option value="">Все марки</option></select>
  <select id="filterType" onchange="applyFilters()"><option value="">Все типы ТС</option></select>
  <select id="filterDistrict" onchange="applyFilters()"><option value="">Все округа</option></select>
  <select id="filterDiscount" onchange="applyFilters()">
    <option value="">Все товары</option>
    <option value="discount">Со скидкой</option>
  </select>
  <input type="number" id="priceMin" placeholder="Цена от" oninput="applyFilters()">
  <input type="number" id="priceMax" placeholder="Цена до" oninput="applyFilters()">
  <select id="sortBy" onchange="applyFilters()">
    <option value="">Без сортировки</option>
    <option value="price_asc">Цена ↑</option>
    <option value="price_desc">Цена ↓</option>
    <option value="year_asc">Год ↑</option>
    <option value="year_desc">Год ↓</option>
    <option value="mileage_asc">Пробег ↑</option>
    <option value="mileage_desc">Пробег ↓</option>
  </select>
</div>

<div class="stats" id="stats"></div>

<div id="loadingIndicator" class="loading">
  <div class="spinner" style="width:36px;height:36px;border-width:4px;margin-bottom:0"></div>
  <p style="margin-top:1rem">Загрузка каталога...</p>
</div>

<div class="grid" id="grid"></div>

<div class="pager" id="pager"></div>

<!-- Modal -->
<div class="modal-overlay" id="modalOverlay" onclick="closeModal(event)">
  <div class="modal" onclick="event.stopPropagation()">
    <button class="modal-close" onclick="document.getElementById('modalOverlay').classList.remove('active')">&times;</button>
    <div class="modal-carousel" id="modalCarousel"></div>
    <div class="modal-body" id="modalBody"></div>
  </div>
</div>

<script>
/* ── Состояние приложения ── */
let allCards = [];
let filteredCards = [];
let currentPage = 1;
const PER_PAGE = 48;
const YANDEX_API = 'https://cloud-api.yandex.net/v1/disk/public/resources';
const IMG_EXT = ['.jpg','.jpeg','.png','.bmp','.webp','.jfif','.heic','.heif','.tiff','.tif','.gif','.avif'];

/* ── Инициализация ── */
async function init() {
  try {
    const resp = await fetch('data.json');
    if (!resp.ok) throw new Error('Не удалось загрузить data.json');
    allCards = await resp.json();
  } catch (e) {
    document.getElementById('loadingIndicator').innerHTML =
      '<p style="color:var(--accent)">Ошибка загрузки данных: ' + e.message + '</p>';
    return;
  }
  document.getElementById('loadingIndicator').style.display = 'none';

  buildFilterOptions();
  loadStateFromUrl();
  applyFilters();
}

/* ── Фильтры: заполняем select-ы из данных ── */
function buildFilterOptions() {
  const brands = new Set();
  const types = new Set();
  const districts = new Set();
  allCards.forEach(c => {
    if (c.brand) brands.add(c.brand);
    if (c.vehicle_type) types.add(c.vehicle_type);
    if (c.location) districts.add(c.location);
  });

  const brandSelect = document.getElementById('filterBrand');
  [...brands].sort().forEach(b => {
    const opt = document.createElement('option');
    opt.value = b; opt.textContent = b;
    brandSelect.appendChild(opt);
  });

  const typeSelect = document.getElementById('filterType');
  [...types].sort().forEach(t => {
    const opt = document.createElement('option');
    opt.value = t; opt.textContent = t;
    typeSelect.appendChild(opt);
  });

  const distSelect = document.getElementById('filterDistrict');
  [...districts].sort().forEach(d => {
    const opt = document.createElement('option');
    opt.value = d; opt.textContent = d;
    distSelect.appendChild(opt);
  });
}

/* ── Фильтрация и сортировка ── */
function parsePrice(priceStr) {
  if (!priceStr || priceStr === 'Цена по запросу') return 0;
  return parseInt(priceStr.replace(/[^\\d]/g, '')) || 0;
}

function parseMileage(mileageStr) {
  if (!mileageStr || mileageStr === 'н/д' || mileageStr === 'Без пробега') return 0;
  return parseInt(mileageStr.replace(/[^\\d]/g, '')) || 0;
}

function applyFilters() {
  const q = document.getElementById('search').value.toLowerCase();
  const brand = document.getElementById('filterBrand').value;
  const type = document.getElementById('filterType').value;
  const district = document.getElementById('filterDistrict').value;
  const disc = document.getElementById('filterDiscount').value;
  const priceMin = parseInt(document.getElementById('priceMin').value) || 0;
  const priceMax = parseInt(document.getElementById('priceMax').value) || Infinity;
  const sortBy = document.getElementById('sortBy').value;

  filteredCards = allCards.filter(c => {
    if (q) {
      const haystack = (c.title + ' ' + c.vin + ' ' + c.brand + ' ' + c.model + ' ' + c.code).toLowerCase();
      if (!haystack.includes(q)) return false;
    }
    if (brand && c.brand !== brand) return false;
    if (type && c.vehicle_type !== type) return false;
    if (district && c.location !== district) return false;
    if (disc === 'discount' && !c.has_discount) return false;

    const price = parsePrice(c.has_discount ? c.discount_price : c.price);
    if (price < priceMin || price > priceMax) return false;

    return true;
  });

  // Сортировка
  if (sortBy) {
    const [field, order] = sortBy.split('_');
    filteredCards.sort((a, b) => {
      let valA, valB;
      if (field === 'price') {
        valA = parsePrice(a.has_discount ? a.discount_price : a.price);
        valB = parsePrice(b.has_discount ? b.discount_price : b.price);
      } else if (field === 'year') {
        valA = parseInt(a.year) || 0;
        valB = parseInt(b.year) || 0;
      } else if (field === 'mileage') {
        valA = parseMileage(a.mileage);
        valB = parseMileage(b.mileage);
      }
      return order === 'asc' ? valA - valB : valB - valA;
    });
  }

  currentPage = 1;
  saveStateToUrl();
  renderPage();
}

/* ── Сохранение и загрузка состояния из URL ── */
function saveStateToUrl() {
  const params = new URLSearchParams();
  if (currentPage > 1) params.set('page', currentPage);
  const q = document.getElementById('search').value;
  if (q) params.set('q', q);
  const brand = document.getElementById('filterBrand').value;
  if (brand) params.set('brand', brand);
  const type = document.getElementById('filterType').value;
  if (type) params.set('type', type);
  const district = document.getElementById('filterDistrict').value;
  if (district) params.set('district', district);
  const disc = document.getElementById('filterDiscount').value;
  if (disc) params.set('discount', disc);
  const priceMin = document.getElementById('priceMin').value;
  if (priceMin) params.set('priceMin', priceMin);
  const priceMax = document.getElementById('priceMax').value;
  if (priceMax) params.set('priceMax', priceMax);
  const sortBy = document.getElementById('sortBy').value;
  if (sortBy) params.set('sort', sortBy);

  const hash = params.toString();
  history.replaceState(null, '', hash ? '#' + hash : location.pathname);
}

function loadStateFromUrl() {
  const params = new URLSearchParams(location.hash.slice(1));
  if (params.has('q')) document.getElementById('search').value = params.get('q');
  if (params.has('brand')) document.getElementById('filterBrand').value = params.get('brand');
  if (params.has('type')) document.getElementById('filterType').value = params.get('type');
  if (params.has('district')) document.getElementById('filterDistrict').value = params.get('district');
  if (params.has('discount')) document.getElementById('filterDiscount').value = params.get('discount');
  if (params.has('priceMin')) document.getElementById('priceMin').value = params.get('priceMin');
  if (params.has('priceMax')) document.getElementById('priceMax').value = params.get('priceMax');
  if (params.has('sort')) document.getElementById('sortBy').value = params.get('sort');
  if (params.has('page')) currentPage = parseInt(params.get('page')) || 1;
}

/* ── Отрисовка страницы ── */
function renderPage() {
  const total = filteredCards.length;
  const totalPages = Math.max(1, Math.ceil(total / PER_PAGE));
  if (currentPage > totalPages) currentPage = totalPages;

  const start = (currentPage - 1) * PER_PAGE;
  const end = Math.min(start + PER_PAGE, total);
  const pageCards = filteredCards.slice(start, end);

  document.getElementById('stats').textContent =
    'Показано ' + (total === 0 ? 0 : start + 1) + '–' + end + ' из ' + total;

  renderCards(pageCards, start);
  renderPager(currentPage, totalPages);

  window.scrollTo({ top: 0, behavior: 'smooth' });
}

/* ── Отрисовка карточек ── */
function esc(s) { return s ? s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;') : ''; }

const COLOR_MAP = {
  'белый':'#f5f5f5','белай':'#f5f5f5','серый':'#9e9e9e','светло-серый':'#bdbdbd','темно-серый':'#616161',
  'черный':'#212121','черно-серый':'#424242','серо-зеленый':'#6b8e6b','черно-зеленый':'#2e4a2e',
  'синий':'#1565c0','темно-синий':'#0d47a1','голубой':'#42a5f5','ультрамарин':'#3f51b5',
  'красный':'#d32f2f','светло-красно-оранжевый':'#ef6c00','бордовый':'#880e4f',
  'зеленый':'#388e3c','зелёный':'#388e3c','салатовый':'#7cb342',
  'желтый':'#fdd835','жёлтый':'#fdd835','рапсово-желтый':'#f9a825',
  'оранжевый':'#ef6c00','светло-серо-оранжевый':'#d4a057',
  'коричневый':'#6d4c41','светло-коричневый':'#8d6e63','светло-бежевый':'#d7ccc8',
  'фиолетовый':'#7b1fa2','серебристый':'#c0c0c0','комбинированный':'linear-gradient(135deg,#eee 50%,#666 50%)',
  'многоцветный':'linear-gradient(135deg,#f44 25%,#ff0 25%,#ff0 50%,#4a4 50%,#4a4 75%,#44f 75%)',
};
function colorToCss(name) {
  if (!name) return '#ccc';
  const low = name.toLowerCase();
  for (const [key, val] of Object.entries(COLOR_MAP)) {
    if (low.startsWith(key) || low.includes(key)) return val;
  }
  return '#ccc';
}

function renderCards(cards, globalOffset) {
  const grid = document.getElementById('grid');
  grid.innerHTML = '';

  cards.forEach((c, i) => {
    const globalIdx = globalOffset + i;
    const card = document.createElement('div');
    card.className = 'card';
    card.onclick = () => showModal(globalIdx);

    /* Photo Preview - одно фото */
    let photoHtml = '';
    if (c.has_discount) photoHtml += '<span class="badge badge-discount">' + esc(c.discount_pct) + '</span>';
    if (c.status) photoHtml += '<span class="badge badge-status">' + esc(c.status) + '</span>';
    if (c.has_photo) {
      photoHtml += '<div class="img-loader"><div class="spinner"></div><span>Загрузка...</span></div>';
    } else {
      photoHtml += '<div class="no-photo"><svg width="44" height="44" fill="none" stroke="currentColor" stroke-width="1.5" viewBox="0 0 24 24"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><path d="m21 15-5-5L5 21"/></svg>Нет фото</div>';
    }

    /* Meta chips */
    let metaHtml = '';
    if (c.color) metaHtml += '<span class="meta-chip"><span class="color-dot" style="background:' + colorToCss(c.color) + '"></span>' + esc(c.color) + '</span>';
    metaHtml += '<span class="meta-chip"><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 8v4l3 3"/><circle cx="12" cy="12" r="10"/></svg>' + esc(c.mileage) + '</span>';
    if (c.year) metaHtml += '<span class="meta-chip">' + esc(c.year) + ' г.</span>';

    /* Price */
    let priceHtml = '';
    if (c.has_discount) {
      priceHtml = '<span class="price-current sale">' + esc(c.discount_price) + '</span><span class="price-old">' + esc(c.original_price) + '</span>';
    } else {
      priceHtml = '<span class="price-current">' + esc(c.price) + '</span>';
    }

    card.innerHTML =
      '<div class="photo-preview"' + (c.has_photo ? ' data-photo-url="' + esc(c.photo_url) + '"' : '') + '>' + photoHtml + '</div>' +
      '<div class="card-body">' +
        '<div class="card-type">' + esc(c.vehicle_type) + '</div>' +
        '<div class="card-title">' + esc(c.title) + '</div>' +
        '<div class="card-meta">' + metaHtml + '</div>' +
        (c.short_desc ? '<div class="card-desc">' + esc(c.short_desc) + '</div>' : '') +
      '</div>' +
      '<div class="card-footer">' +
        '<div class="price-block">' + priceHtml + '</div>' +
        '<div class="card-location"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 1 1 18 0z"/><circle cx="12" cy="10" r="3"/></svg>' + esc(c.location) + '</div>' +
      '</div>';

    grid.appendChild(card);
  });

  /* Запускаем lazy-load для превью */
  observePhotos();
}

/* ── Пагинация ── */
function paginationPages(current, total) {
  if (total <= 9) {
    const arr = [];
    for (let i = 1; i <= total; i++) arr.push(i);
    return arr;
  }
  const pages = [1];
  if (current > 4) pages.push(null);
  const s = Math.max(2, current - 2);
  const e = Math.min(total - 1, current + 2);
  for (let i = s; i <= e; i++) pages.push(i);
  if (current < total - 3) pages.push(null);
  if (pages[pages.length - 1] !== total) pages.push(total);
  return pages;
}

function renderPager(current, total) {
  const pager = document.getElementById('pager');
  pager.innerHTML = '';
  if (total <= 1) return;

  if (current > 1) {
    const btn = document.createElement('button');
    btn.className = 'nav-btn';
    btn.innerHTML = '&larr; Назад';
    btn.onclick = () => goToPage(current - 1);
    pager.appendChild(btn);
  }

  paginationPages(current, total).forEach(p => {
    if (p === null) {
      const sp = document.createElement('span');
      sp.className = 'ell';
      sp.innerHTML = '&hellip;';
      pager.appendChild(sp);
    } else if (p === current) {
      const sp = document.createElement('span');
      sp.className = 'cur';
      sp.textContent = p;
      pager.appendChild(sp);
    } else {
      const btn = document.createElement('button');
      btn.textContent = p;
      btn.onclick = () => goToPage(p);
      pager.appendChild(btn);
    }
  });

  if (current < total) {
    const btn = document.createElement('button');
    btn.className = 'nav-btn';
    btn.innerHTML = 'Вперёд &rarr;';
    btn.onclick = () => goToPage(current + 1);
    pager.appendChild(btn);
  }
}

function goToPage(p) {
  currentPage = p;
  saveStateToUrl();
  renderPage();
}

/* ── Загрузка фото на карточках ── */
let photoObserver = null;
const loadQueue = [];
const CONCURRENT = 8;
let activeLoads = 0;
const apiCache = new Map();

/* localStorage кэш с TTL 24 часа */
function getCachedData(url) {
  try {
    const key = 'photos_' + url;
    const cached = localStorage.getItem(key);
    if (!cached) return null;
    const {data, ts} = JSON.parse(cached);
    if (Date.now() - ts > 24 * 3600 * 1000) {
      localStorage.removeItem(key);
      return null;
    }
    return data;
  } catch { return null; }
}

function setCachedData(url, data) {
  try {
    const key = 'photos_' + url;
    localStorage.setItem(key, JSON.stringify({data, ts: Date.now()}));
  } catch {}
}

function observePhotos() {
  if (photoObserver) photoObserver.disconnect();
  loadQueue.length = 0;
  photoObserver = new IntersectionObserver(entries => {
    entries.forEach(e => {
      if (e.isIntersecting) {
        photoObserver.unobserve(e.target);
        loadQueue.push(e.target);
        processQueue();
      }
    });
  }, { rootMargin: '200px' });
  document.querySelectorAll('.photo-preview[data-photo-url]').forEach(el => photoObserver.observe(el));
}

function processQueue() {
  while (activeLoads < CONCURRENT && loadQueue.length > 0) {
    const el = loadQueue.shift();
    activeLoads++;
    loadPhoto(el).finally(() => { activeLoads--; processQueue(); });
  }
}

async function loadPhoto(el, attempt) {
  attempt = attempt || 1;
  const url = el.dataset.photoUrl;
  if (!url) return;

  /* Не-Yandex ссылки — показываем как внешнюю ссылку */
  if (!url.includes('disk.yandex')) {
    showPhotoError(el, url);
    return;
  }

  /* Проверяем кэш */
  if (apiCache.has(url)) {
    buildPhoto(el, apiCache.get(url));
    return;
  }
  const cached = getCachedData(url);
  if (cached) {
    apiCache.set(url, cached);
    buildPhoto(el, cached);
    return;
  }

  try {
    const resp = await fetch(YANDEX_API + '?public_key=' + encodeURIComponent(url) + '&limit=50&preview_size=400x300');
    if (resp.status === 429 || resp.status >= 500) {
      if (attempt <= 3) {
        await new Promise(r => setTimeout(r, 300 * attempt));
        return loadPhoto(el, attempt + 1);
      }
    }
    if (!resp.ok) throw new Error(resp.status);
    const data = await resp.json();
    let previews = [];
    if (data.type === 'file' && data.preview) {
      previews.push(data.preview);
    } else if (data.type === 'dir') {
      const items = (data._embedded && data._embedded.items) || [];
      for (const item of items) {
        if (item.type === 'file' && item.preview && !item.name.toLowerCase().endsWith('.pdf')) {
          previews.push(item.preview);
        }
      }
    }
    if (previews.length === 0) { showPhotoError(el, url); return; }

    /* Сохраняем в кэш */
    apiCache.set(url, previews);
    setCachedData(url, previews);

    buildPhoto(el, previews);
  } catch {
    if (attempt <= 3) {
      await new Promise(r => setTimeout(r, 300 * attempt));
      return loadPhoto(el, attempt + 1);
    }
    showPhotoError(el, url);
  }
}

function buildPhoto(el, previews) {
  const loader = el.querySelector('.img-loader');
  if (loader) loader.remove();

  /* Показываем только первое фото */
  const img = new window.Image();
  img.onload = () => el.appendChild(img);
  img.onerror = () => showPhotoError(el, previews[0]);
  img.src = previews[0];
  img.alt = 'Фото';
  img.draggable = false;

  /* Счётчик если фото больше 1 */
  if (previews.length > 1) {
    const counter = document.createElement('div');
    counter.className = 'photo-count';
    counter.textContent = previews.length + ' фото';
    el.appendChild(counter);
  }

  /* Сохраняем все preview для модального окна */
  el.dataset.previews = JSON.stringify(previews);
}

function showPhotoError(el, url) {
  const loader = el.querySelector('.img-loader');
  if (loader) {
    const label = url.includes('disk.yandex') ? 'Открыть на Яндекс.Диске' : 'Открыть фото';
    loader.innerHTML = '<a href="' + url + '" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="color:var(--primary);font-size:.82rem">' + label + '</a>';
  }
}

/* ── Модальная галерея (как на drom.ru) ── */
let currentGalleryIndex = 0;
let galleryPhotos = [];

function showGalleryPhoto(index) {
  if (index < 0 || index >= galleryPhotos.length) return;
  currentGalleryIndex = index;

  const mainImg = document.querySelector('.modal-gallery-main img');
  if (mainImg) mainImg.src = galleryPhotos[index];

  const counter = document.querySelector('.modal-gallery-main .photo-counter');
  if (counter) counter.textContent = (index + 1) + ' / ' + galleryPhotos.length;

  document.querySelectorAll('.modal-gallery-thumb').forEach((thumb, i) => {
    thumb.classList.toggle('active', i === index);
  });

  /* Автопрокрутка галереи миниатюр */
  const activeThumb = document.querySelector('.modal-gallery-thumb.active');
  if (activeThumb) {
    activeThumb.scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'center' });
  }
}

function nextGalleryPhoto() {
  showGalleryPhoto((currentGalleryIndex + 1) % galleryPhotos.length);
}

function prevGalleryPhoto() {
  showGalleryPhoto((currentGalleryIndex - 1 + galleryPhotos.length) % galleryPhotos.length);
}

async function loadModalGallery(url) {
  const container = document.getElementById('modalCarousel');
  if (!url) {
    container.innerHTML = '<div class="modal-gallery-main"><div class="m-no-photo"><svg width="40" height="40" fill="none" stroke="currentColor" stroke-width="1.5" viewBox="0 0 24 24"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><path d="m21 15-5-5L5 21"/></svg><span>Нет фото</span></div></div>';
    return;
  }

  const linkLabel = url.includes('disk.yandex') ? 'Открыть на Яндекс.Диске' : 'Открыть фото';

  if (!url.includes('disk.yandex')) {
    container.innerHTML = '<div class="modal-gallery-main"><div class="m-no-photo"><a href="' + url + '" target="_blank" rel="noopener" style="color:var(--primary)">' + linkLabel + '</a></div></div>';
    return;
  }

  container.innerHTML = '<div class="modal-gallery-main"><div class="m-loader"><div class="spinner"></div><span>Загрузка фото...</span></div></div>';

  /* Проверяем кэш */
  let previews;
  if (apiCache.has(url)) {
    previews = apiCache.get(url);
  } else {
    const cached = getCachedData(url);
    if (cached) {
      previews = cached;
      apiCache.set(url, cached);
    }
  }

  if (!previews) {
    try {
      const resp = await fetch(YANDEX_API + '?public_key=' + encodeURIComponent(url) + '&limit=50&preview_size=1200x800');
      if (!resp.ok) throw new Error();
      const data = await resp.json();
      previews = [];
      if (data.type === 'file' && data.preview) previews.push(data.preview);
      else if (data.type === 'dir') {
        for (const item of (data._embedded && data._embedded.items) || []) {
          if (item.type === 'file' && item.preview && !item.name.toLowerCase().endsWith('.pdf'))
            previews.push(item.preview);
        }
      }
      if (!previews.length) throw new Error('No photos');
      apiCache.set(url, previews);
      setCachedData(url, previews);
    } catch {
      container.innerHTML = '<div class="modal-gallery-main"><div class="m-no-photo"><a href="' + url + '" target="_blank" rel="noopener" style="color:var(--primary)">' + linkLabel + '</a></div></div>';
      return;
    }
  }

  galleryPhotos = previews;
  currentGalleryIndex = 0;

  /* Главное фото */
  let mainHtml = '<div class="modal-gallery-main">';
  mainHtml += '<img src="' + previews[0] + '" alt="Фото">';
  if (previews.length > 1) {
    mainHtml += '<button class="arr arr-l" onclick="event.stopPropagation();prevGalleryPhoto()">&#8249;</button>';
    mainHtml += '<button class="arr arr-r" onclick="event.stopPropagation();nextGalleryPhoto()">&#8250;</button>';
    mainHtml += '<div class="photo-counter">1 / ' + previews.length + '</div>';
  }
  mainHtml += '</div>';

  /* Галерея миниатюр */
  if (previews.length > 1) {
    mainHtml += '<div class="modal-gallery-thumbs">';
    previews.forEach((src, i) => {
      mainHtml += '<div class="modal-gallery-thumb' + (i === 0 ? ' active' : '') + '" onclick="event.stopPropagation();showGalleryPhoto(' + i + ')"><img src="' + src + '" alt=""></div>';
    });
    mainHtml += '</div>';
  }

  container.innerHTML = mainHtml;
}

function showModal(globalIdx) {
  const c = filteredCards[globalIdx];
  if (!c) return;
  loadModalGallery(c.photo_url);

  let priceHtml = '';
  if (c.has_discount) {
    priceHtml = '<span class="modal-price sale">' + esc(c.discount_price) + '</span><span class="modal-price-old">' + esc(c.price) + '</span><span class="modal-discount-badge">' + esc(c.discount_pct) + '</span>';
  } else {
    priceHtml = '<span class="modal-price">' + esc(c.price) + '</span>';
  }

  let statusHtml = '';
  if (c.status) {
    const cls = c.has_discount ? ' sale' : '';
    statusHtml = '<span class="modal-status' + cls + '">' + esc(c.status) + '</span>';
  }

  const specs = [
    ['Тип ТС', c.vehicle_type], ['Год выпуска', c.year ? c.year + ' г.' : ''],
    ['Пробег', c.mileage], ['Цвет кузова', c.color],
    ['VIN', c.vin], ['Код', c.code],
  ];
  let specsHtml = '';
  specs.forEach(([label, val]) => {
    if (val) specsHtml += '<div class="spec-item"><span class="spec-label">' + esc(label) + '</span><span class="spec-value">' + esc(val) + '</span></div>';
  });

  let commentHtml = '';
  if (c.comment) {
    commentHtml = '<div class="modal-section"><div class="modal-section-title">Комментарий по оценке</div><div class="modal-comment">' + esc(c.comment) + '</div></div>';
  }

  let addressHtml = '';
  if (c.address) {
    addressHtml = '<div class="modal-section"><div class="modal-section-title">Адрес стоянки</div><div class="modal-address">' + esc(c.address) + (c.location ? ' <span style="color:var(--text-light)">(' + esc(c.location) + ')</span>' : '') + '</div></div>';
  }

  let linkHtml = '';
  if (c.photo_url) {
    linkHtml = '<a class="modal-link" href="' + esc(c.photo_url) + '" target="_blank" rel="noopener" onclick="event.stopPropagation()"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>Все фото на Яндекс.Диске</a>';
  }

  document.getElementById('modalBody').innerHTML =
    '<div class="modal-header"><h2>' + esc(c.title) + '</h2>' + statusHtml + '</div>' +
    '<div class="modal-price-row">' + priceHtml + '</div>' +
    (specsHtml ? '<div class="modal-specs">' + specsHtml + '</div>' : '') +
    commentHtml + addressHtml + linkHtml;

  document.getElementById('modalOverlay').classList.add('active');
}

function closeModal(e) {
  if (e.target === document.getElementById('modalOverlay'))
    document.getElementById('modalOverlay').classList.remove('active');
}

document.addEventListener('keydown', e => {
  if (e.key === 'Escape') document.getElementById('modalOverlay').classList.remove('active');
});

window.addEventListener('hashchange', () => {
  loadStateFromUrl();
  applyFilters();
});

/* ── Запуск ── */
init();
</script>

</body>
</html>
"""


# ─── Генерация ───────────────────────────────────────────────────────────────

def generate_site(cards: list[dict], output_dir: Path):
    """Генерирует index.html с встроенными данными (для работы без сервера)."""
    # Убеждаемся, что output_dir это Path объект
    if not isinstance(output_dir, Path):
        output_dir = Path(output_dir)

    # Создаём директорию с родительскими папками
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"📁 Создана папка: {output_dir.absolute()}")

    # Сохраняем data.json отдельно для совместимости
    data_path = output_dir / "data.json"
    data_path.write_text(json.dumps(cards, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Сохранён data.json ({len(cards)} карточек)")

    # Встраиваем данные прямо в HTML (для работы без сервера)
    cards_json = json.dumps(cards, ensure_ascii=False)

    # Заменяем в HTML загрузку данных на встроенные данные
    html_with_data = HTML_PAGE.replace(
        "/* ── Инициализация ── */\nasync function init() {\n  try {\n    const resp = await fetch('data.json');\n    if (!resp.ok) throw new Error('Не удалось загрузить data.json');\n    allCards = await resp.json();",
        f"/* ── Инициализация ── */\nasync function init() {{\n  try {{\n    // Данные встроены в HTML для работы без сервера\n    allCards = {cards_json};"
    )

    # Сохраняем HTML с встроенными данными
    index_path = output_dir / "index.html"
    index_path.write_text(html_with_data, encoding="utf-8")
    print(f"✅ Сохранён index.html (с встроенными данными)")

    print(f"\n🎉 Сайт сгенерирован: {output_dir.absolute()}")
    print(f"  Файлы: index.html (standalone) + data.json")
    print(f"  💡 Сайт работает без сервера - просто откройте index.html")
    print(f"  Или запустите локальный сервер:")
    print(f"    cd {output_dir} && python3 -m http.server 8080")


# ─── GUI ──────────────────────────────────────────────────────────────────────

def detect_and_read(path: Path) -> pd.DataFrame:
    """Определяет формат файла и читает его подходящим ридером."""
    sheets = pd.ExcelFile(path).sheet_names
    if "зимние скидки" in sheets:
        return read_file2(path)
    return read_file1(path)


class ParserGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Парсер каталога ТС")
        self.root.geometry("720x520")
        self.root.minsize(600, 420)
        self._server_proc = None
        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self):
        pad = {"padx": 10, "pady": 5}

        # --- Файл ---
        tk.Label(
            self.root,
            text="Excel-файл (сток или зимние скидки):",
            font=("sans-serif", 11, "bold"),
            fg="#1e293b"
        ).pack(anchor="w", **pad)
        ff = tk.Frame(self.root)
        ff.pack(fill="x", padx=10)
        self.entry_file = tk.Entry(ff, font=("sans-serif", 11), bg="white", relief="solid", bd=1)
        self.entry_file.pack(side="left", fill="x", expand=True)
        tk.Button(
            ff, text="Обзор…", width=10,
            command=self._pick_file,
            bg="#64748b", fg="white",
            font=("sans-serif", 10, "bold"),
            relief="flat", cursor="hand2"
        ).pack(side="right", padx=(6, 0))

        # --- Папка вывода ---
        tk.Label(
            self.root,
            text="Папка вывода:",
            font=("sans-serif", 11, "bold"),
            fg="#1e293b"
        ).pack(anchor="w", **pad)
        fo = tk.Frame(self.root)
        fo.pack(fill="x", padx=10)
        self.entry_output = tk.Entry(fo, font=("sans-serif", 11), bg="white", relief="solid", bd=1)
        self.entry_output.pack(side="left", fill="x", expand=True)
        self.entry_output.insert(0, str(OUTPUT_DIR))
        tk.Button(
            fo, text="Обзор…", width=10,
            command=self._pick_dir,
            bg="#64748b", fg="white",
            font=("sans-serif", 10, "bold"),
            relief="flat", cursor="hand2"
        ).pack(side="right", padx=(6, 0))

        # --- Кнопки ---
        btn_frame = tk.Frame(self.root)
        btn_frame.pack(fill="x", padx=10, pady=(15, 6))
        self.btn_generate = tk.Button(
            btn_frame, text="🚀 Сгенерировать каталог",
            command=self._on_generate,
            bg="#0369a1", fg="white",
            font=("sans-serif", 13, "bold"),
            height=2, relief="flat", cursor="hand2"
        )
        self.btn_generate.pack(side="left", fill="x", expand=True)
        self.btn_open = tk.Button(
            btn_frame, text="📁 Открыть папку", width=16,
            command=self._open_output, state="disabled",
            bg="#475569", fg="white",
            font=("sans-serif", 10, "bold"),
            relief="flat", cursor="hand2"
        )
        self.btn_open.pack(side="right", padx=(8, 0))

        # --- Кнопка сервера ---
        srv_frame = tk.Frame(self.root)
        srv_frame.pack(fill="x", padx=10, pady=(6, 0))
        self.btn_server = tk.Button(
            srv_frame, text="▶ Запустить сервер",
            command=self._toggle_server, state="disabled",
            bg="#059669", fg="white",
            font=("sans-serif", 11, "bold"),
            relief="flat", cursor="hand2"
        )
        self.btn_server.pack(fill="x")

        # --- Лог ---
        tk.Label(
            self.root,
            text="Лог выполнения:",
            font=("sans-serif", 11, "bold"),
            fg="#1e293b"
        ).pack(anchor="w", padx=10, pady=(10, 4))
        self.log = scrolledtext.ScrolledText(
            self.root, height=12, state="disabled",
            font=("Menlo", 10), bg="#f8fafc",
            fg="#1e293b", relief="solid", bd=1
        )
        self.log.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    # --- Диалоги выбора ---
    def _pick_file(self):
        path = filedialog.askopenfilename(
            title="Выберите Excel-файл",
            filetypes=[("Excel", "*.xlsx *.xls"), ("Все файлы", "*.*")],
        )
        if path:
            self.entry_file.delete(0, "end")
            self.entry_file.insert(0, path)

    def _pick_dir(self):
        # Получаем текущий путь из поля ввода
        current_path = self.entry_output.get().strip()
        initial_dir = current_path if current_path and os.path.isdir(current_path) else os.path.expanduser("~")

        path = filedialog.askdirectory(
            title="Выберите папку вывода",
            initialdir=initial_dir,
            mustexist=False
        )
        if path:
            # Нормализуем путь для macOS
            path = os.path.normpath(path)
            self.entry_output.delete(0, "end")
            self.entry_output.insert(0, path)

    # --- Логирование ---
    def _log(self, text: str):
        self.log.configure(state="normal")
        self.log.insert("end", text + "\n")
        self.log.see("end")
        self.log.configure(state="disabled")

    # --- Генерация ---
    def _on_generate(self):
        filepath = Path(self.entry_file.get().strip())
        output = Path(self.entry_output.get().strip())

        if not filepath.is_file():
            messagebox.showerror("Ошибка", f"Файл не найден:\n{filepath}")
            return

        # === НОВАЯ ЛОГИКА: Автомаппинг + диалог ===

        self._log("Анализ структуры файла...")

        try:
            # 1. Извлекаем столбцы из Excel
            import config_manager
            import column_mapper
            from mapping_dialog import ColumnMappingDialog

            excel_file = pd.ExcelFile(filepath)
            sheets = excel_file.sheet_names

            # Определяем sheet и header
            if "зимние скидки" in sheets:
                sheet_name = "зимние скидки"
                header = 0
            else:
                sheet_name = sheets[0] if sheets else "Sheet1"
                header = 2

            # Читаем только заголовки (nrows=0 для скорости)
            df_headers = pd.read_excel(
                filepath,
                sheet_name=sheet_name,
                header=header,
                nrows=0
            )
            excel_columns = list(df_headers.columns)

            self._log(f"  Найдено столбцов: {len(excel_columns)}")

            # 2. Загружаем конфигурацию
            config = config_manager.load_config()

            # 3. Проверяем сохранённые шаблоны по имени файла
            template_name = filepath.stem  # Имя файла без расширения
            saved_mapping = config_manager.get_mapping_template(template_name)

            mapping = None

            if saved_mapping:
                # Нашли сохранённый шаблон - спрашиваем пользователя
                use_saved = messagebox.askyesno(
                    "Найден шаблон",
                    f"Для файла найден сохранённый шаблон '{template_name}'.\n"
                    "Использовать его?",
                    icon="question"
                )

                if use_saved:
                    # Проверяем, все ли столбцы из шаблона есть в файле
                    missing = [
                        col for col in saved_mapping.keys()
                        if col not in excel_columns
                    ]

                    if missing:
                        messagebox.showwarning(
                            "Несоответствие шаблона",
                            f"В файле не найдены столбцы из шаблона:\n" +
                            "\n".join(f"  • {col}" for col in missing[:5]) +
                            ("\n  ..." if len(missing) > 5 else "") +
                            "\n\nОткроется диалог для проверки маппинга."
                        )
                        mapping = None  # Покажем диалог
                    else:
                        mapping = saved_mapping
                        self._log("  Использован сохранённый шаблон")

            # 4. Если нет готового маппинга - запускаем автоопределение
            if mapping is None:
                self._log("  Автоматическое определение столбцов...")

                target_fields = list(config["fuzzy_keywords"].keys())
                auto_result = column_mapper.auto_map_columns(
                    excel_columns,
                    target_fields,
                    config["fuzzy_keywords"]
                )

                matched = len(auto_result["mapping"])
                total = len(target_fields)
                self._log(f"  Сопоставлено автоматически: {matched} из {total}")

                # 5. Показываем диалог для проверки
                dialog = ColumnMappingDialog(
                    self.root,
                    excel_columns,
                    auto_result,
                    config
                )

                mapping = dialog.show()

                if mapping is None:
                    # Пользователь отменил
                    self._log("  Операция отменена пользователем")
                    return

                self._log("  Маппинг подтверждён пользователем")

            # 6. Продолжаем обычный процесс с использованием mapping

            self.btn_generate.configure(state="disabled", text="Генерация…")
            self.btn_open.configure(state="disabled")
            self.btn_server.configure(state="disabled")

            threading.Thread(
                target=self._run_with_mapping,
                args=(filepath, output, mapping),
                daemon=True
            ).start()

        except Exception as e:
            self._log(f"  ОШИБКА при анализе файла: {e}")
            import traceback
            self._log(traceback.format_exc())
            messagebox.showerror(
                "Ошибка",
                f"Не удалось проанализировать файл:\n{e}"
            )

    def _run(self, filepath: Path, output: Path):
        try:
            self._log("Определение формата файла…")
            df = detect_and_read(filepath)
            self._log(f"  Загружено записей: {len(df)}")

            self._log("Подготовка карточек…")
            cards = prepare_cards(df)
            self._log(f"  Карточек создано: {len(cards)}")

            self._log("Генерация сайта…")
            generate_site(cards, output)

            self._log(f"\nГотово! Результат в папке: {output}")
            self.root.after(0, lambda: self.btn_open.configure(state="normal"))
            self.root.after(0, lambda: self.btn_server.configure(state="normal"))
        except Exception as e:
            self._log(f"\nОШИБКА: {e}")
        finally:
            self.root.after(0, lambda: self.btn_generate.configure(
                state="normal", text="Сгенерировать"))

    def _run_with_mapping(self, filepath: Path, output: Path, mapping: dict):
        """Новая версия _run с использованием гибкого маппинга."""
        try:
            self._log("Чтение файла с применением маппинга...")
            df = read_flexible(filepath, mapping)
            self._log(f"  Загружено записей: {len(df)}")

            self._log("Подготовка карточек…")
            cards = prepare_cards(df)
            self._log(f"  Карточек создано: {len(cards)}")

            self._log("Генерация сайта…")
            generate_site(cards, output)

            self._log(f"\n✅ Готово! Результат в папке: {output}")
            self._log("Открытие сайта в браузере...")

            # Автоматическое открытие сайта в браузере
            index_path = output / "index.html"
            if index_path.exists():
                webbrowser.open(f"file://{index_path}")
                self._log("  Сайт открыт в браузере")
            else:
                self._log("  ⚠️ Файл index.html не найден")

            self.root.after(0, lambda: self.btn_open.configure(state="normal"))
            self.root.after(0, lambda: self.btn_server.configure(state="normal"))

        except Exception as e:
            self._log(f"\n❌ ОШИБКА: {e}")
            import traceback
            self._log(traceback.format_exc())
        finally:
            self.root.after(0, lambda: self.btn_generate.configure(
                state="normal", text="Сгенерировать"))

    # --- Открытие папки ---
    def _open_output(self):
        path = self.entry_output.get().strip()
        if os.path.isdir(path):
            subprocess.Popen(["open", path])

    # --- Локальный сервер ---
    def _toggle_server(self):
        if self._server_proc and self._server_proc.poll() is None:
            self._server_proc.terminate()
            self._server_proc = None
            self.btn_server.configure(text="Запустить сервер", bg="#16a34a")
            self._log("Сервер остановлен.")
        else:
            output = self.entry_output.get().strip()
            if not os.path.isdir(output):
                messagebox.showerror("Ошибка", "Сначала сгенерируйте сайт.")
                return
            self._server_proc = subprocess.Popen(
                [sys.executable, "-m", "http.server", "8080"],
                cwd=output,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            self.btn_server.configure(text="Остановить сервер", bg="#dc2626")
            self._log("Сервер запущен: http://localhost:8080")
            webbrowser.open("http://localhost:8080")

    def _on_close(self):
        if self._server_proc and self._server_proc.poll() is None:
            self._server_proc.terminate()
        self.root.destroy()

    def run(self):
        self.root.mainloop()


# ─── Main ────────────────────────────────────────────────────────────────────

def main_cli():
    """Запуск из командной строки (--cli)."""
    print("Чтение файла 1...")
    df1 = read_file1(FILE1)
    print(f"  Загружено записей: {len(df1)}")

    print("Чтение файла 2...")
    df2 = read_file2(FILE2)
    print(f"  Загружено записей: {len(df2)}")

    print("Объединение данных...")
    df = merge_data(df1, df2)
    print(f"  Уникальных записей: {len(df)}")

    print("Подготовка карточек...")
    cards = prepare_cards(df)
    print(f"  Карточек создано: {len(cards)}")

    print("Генерация сайта...")
    generate_site(cards, OUTPUT_DIR)


if __name__ == "__main__":
    if "--cli" in sys.argv:
        main_cli()
    else:
        if tk is None:
            raise RuntimeError(
                "tkinter не установлен. Для GUI установите tkinter или запустите parser.py с флагом --cli"
            )
        ParserGUI().run()
