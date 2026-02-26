#!/usr/bin/env python3
"""
Менеджер конфигурации для гибкой системы маппинга столбцов.
"""

import json
from pathlib import Path
from typing import Optional


CONFIG_PATH = Path(__file__).parent / "config.json"


def load_config() -> dict:
    """
    Загружает конфигурацию из config.json или возвращает default.

    Returns:
        dict: Конфигурация с ключами 'fuzzy_keywords' и 'mappings'
    """
    if not CONFIG_PATH.exists():
        return get_default_config()

    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Ошибка чтения config.json: {e}")
        return get_default_config()


def save_config(config: dict) -> bool:
    """
    Сохраняет конфигурацию в config.json.

    Args:
        config: Словарь конфигурации

    Returns:
        bool: True если успешно, False при ошибке
    """
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"Ошибка сохранения config.json: {e}")
        return False


def get_default_config() -> dict:
    """
    Возвращает конфигурацию по умолчанию.

    Returns:
        dict: Default конфигурация
    """
    return {
        "version": "1.0",
        "fuzzy_keywords": {},
        "mappings": {}
    }


def add_mapping_template(name: str, mapping: dict) -> bool:
    """
    Добавляет или обновляет шаблон маппинга.

    Args:
        name: Название шаблона
        mapping: Словарь маппинга {excel_column: target_field}

    Returns:
        bool: True если успешно
    """
    config = load_config()

    if "mappings" not in config:
        config["mappings"] = {}

    config["mappings"][name] = mapping

    return save_config(config)


def get_mapping_template(name: str) -> Optional[dict]:
    """
    Получает шаблон маппинга по имени.

    Args:
        name: Название шаблона

    Returns:
        dict или None: Маппинг или None если не найден
    """
    config = load_config()
    return config.get("mappings", {}).get(name)


def list_mapping_templates() -> list:
    """
    Возвращает список названий всех сохранённых шаблонов.

    Returns:
        list: Список названий шаблонов
    """
    config = load_config()
    return list(config.get("mappings", {}).keys())


def delete_mapping_template(name: str) -> bool:
    """
    Удаляет шаблон маппинга.

    Args:
        name: Название шаблона

    Returns:
        bool: True если успешно
    """
    config = load_config()

    if "mappings" in config and name in config["mappings"]:
        del config["mappings"][name]
        return save_config(config)

    return False


def get_keywords_for_field(field: str) -> dict:
    """
    Получает ключевые слова для конкретного поля.

    Args:
        field: Название целевого поля

    Returns:
        dict: Словарь с ключами 'primary' и 'synonyms'
    """
    config = load_config()
    keywords = config.get("fuzzy_keywords", {}).get(field, {})

    return {
        "primary": keywords.get("primary", []),
        "synonyms": keywords.get("synonyms", [])
    }
