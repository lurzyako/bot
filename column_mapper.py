#!/usr/bin/env python3
"""
Система автоматического определения столбцов с использованием fuzzy matching.
"""

from difflib import SequenceMatcher
from typing import List, Dict


def levenshtein_similarity(s1: str, s2: str) -> float:
    """
    Вычисляет similarity score между двумя строками (0-1).

    Args:
        s1: Первая строка
        s2: Вторая строка

    Returns:
        float: Similarity score (0 = нет совпадений, 1 = идентичные)
    """
    return SequenceMatcher(None, s1, s2).ratio()


def normalize_column_name(name: str) -> str:
    """
    Нормализует название столбца для сравнения.

    Args:
        name: Название столбца

    Returns:
        str: Нормализованное название (lowercase, stripped)
    """
    if not isinstance(name, str):
        name = str(name)
    return name.lower().strip()


def calculate_match_score(excel_col: str, keywords_dict: dict) -> int:
    """
    Вычисляет score совпадения столбца с ключевыми словами.

    Args:
        excel_col: Название столбца из Excel (уже нормализованное)
        keywords_dict: Словарь с ключами 'primary' и 'synonyms'

    Returns:
        int: Score от 0 до 100
    """
    primary = keywords_dict.get("primary", [])
    synonyms = keywords_dict.get("synonyms", [])

    best_score = 0

    # 1. Exact match в primary - 100%
    for keyword in primary:
        norm_keyword = normalize_column_name(keyword)
        if excel_col == norm_keyword:
            return 100

    # 2. Partial match в primary - 90%
    for keyword in primary:
        norm_keyword = normalize_column_name(keyword)
        if norm_keyword in excel_col or excel_col in norm_keyword:
            best_score = max(best_score, 90)

    # 3. Exact match в synonyms - 70%
    for keyword in synonyms:
        norm_keyword = normalize_column_name(keyword)
        if excel_col == norm_keyword:
            best_score = max(best_score, 70)

    # 4. Partial match в synonyms - 60%
    for keyword in synonyms:
        norm_keyword = normalize_column_name(keyword)
        if norm_keyword in excel_col or excel_col in norm_keyword:
            best_score = max(best_score, 60)

    # 5. Levenshtein similarity для опечаток - 40-50%
    if best_score == 0:
        for keyword in primary + synonyms:
            norm_keyword = normalize_column_name(keyword)
            similarity = levenshtein_similarity(excel_col, norm_keyword)
            if similarity > 0.7:
                score = int(similarity * 50)
                best_score = max(best_score, score)

    return best_score


def auto_map_columns(
    excel_columns: List[str],
    target_fields: List[str],
    keywords: dict
) -> dict:
    """
    Автоматически определяет маппинг столбцов из Excel на целевые поля.

    Args:
        excel_columns: Список названий столбцов из Excel
        target_fields: Список целевых полей (24 поля)
        keywords: Словарь ключевых слов из config.json

    Returns:
        dict: {
            "mapping": {"Код предложения": "code", ...},
            "confidence": {"code": 95, "brand": 100, ...},
            "unmatched_excel": ["Какой-то столбец"],
            "unmatched_target": ["keys", "pts_type"]
        }
    """
    # Нормализуем названия столбцов
    normalized = {col: normalize_column_name(col) for col in excel_columns}

    # Маппинг: excel_column -> target_field
    mapping = {}
    # Confidence: target_field -> score
    confidence = {}
    # Tracking уже использованных Excel столбцов
    used_excel_cols = set()

    # Для каждого target field ищем лучший match
    for field in target_fields:
        field_keywords = keywords.get(field, {})
        if not field_keywords:
            continue

        best_match = None
        best_score = 0

        for excel_col in excel_columns:
            if excel_col in used_excel_cols:
                continue

            norm_col = normalized[excel_col]
            score = calculate_match_score(norm_col, field_keywords)

            if score > best_score:
                best_score = score
                best_match = excel_col

        # Применяем только если score >= 40 (threshold)
        if best_match and best_score >= 40:
            mapping[best_match] = field
            confidence[field] = best_score
            used_excel_cols.add(best_match)

    # Определяем несопоставленные столбцы
    unmatched_excel = [col for col in excel_columns if col not in mapping]
    unmatched_target = [field for field in target_fields if field not in confidence]

    return {
        "mapping": mapping,
        "confidence": confidence,
        "unmatched_excel": unmatched_excel,
        "unmatched_target": unmatched_target
    }


def validate_mapping(
    mapping: dict,
    critical_fields: List[str] = None
) -> tuple:
    """
    Проверяет валидность маппинга.

    Args:
        mapping: Словарь маппинга {excel_col: target_field}
        critical_fields: Список критических полей (default: code, brand, model, price)

    Returns:
        tuple: (is_valid: bool, missing_critical: List[str])
    """
    if critical_fields is None:
        critical_fields = ["code", "brand", "model", "price"]

    mapped_fields = set(mapping.values())
    missing_critical = [f for f in critical_fields if f not in mapped_fields]

    is_valid = len(missing_critical) == 0

    return is_valid, missing_critical


def detect_duplicate_mappings(mapping: dict) -> List[str]:
    """
    Определяет дублирующиеся target поля в маппинге.

    Args:
        mapping: Словарь маппинга {excel_col: target_field}

    Returns:
        List[str]: Список дублирующихся target полей
    """
    from collections import Counter

    target_values = list(mapping.values())
    counts = Counter(target_values)

    duplicates = [field for field, count in counts.items() if count > 1]

    return duplicates
