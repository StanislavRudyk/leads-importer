import os
import re
import pathlib as ph
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

import pandas as pd
import yaml

from .normalizer import extract_file_date, extract_context_from_path

import logging

logger = logging.getLogger('leads_importer.parser')

SUPPORTED_EXTENSIONS: Set[str] = {'.csv', '.xlsx', '.xls'}
MAX_FILE_SIZE_BYTES: int = 100 * 1024 * 1024
ENCODINGS_TO_TRY: List[str] = ['utf-8', 'utf-8-sig', 'windows-1251', 'latin-1', 'iso-8859-1']

_YAML_CACHE: Optional[Dict[str, str]] = None


def get_mappings_from_yaml() -> Dict[str, str]:
    """Загрузка маппинга колонок из YAML (кешируется)."""
    global _YAML_CACHE
    if _YAML_CACHE is not None:
        return _YAML_CACHE

    yaml_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'column_mappings.yaml')
    if not os.path.exists(yaml_path):
        _YAML_CACHE = {}
        return _YAML_CACHE

    with open(yaml_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    mappings: Dict[str, str] = {}
    if config:
        for standard_field, synonyms in config.items():
            if synonyms:
                for syn in synonyms:
                    mappings[str(syn).strip().lower()] = standard_field
    _YAML_CACHE = mappings
    return _YAML_CACHE


def _read_dataframe(file_path: str) -> Optional[pd.DataFrame]:
    """Быстрое чтение файла в DataFrame."""
    ext = ph.Path(file_path).suffix.lower()
    if ext not in SUPPORTED_EXTENSIONS:
        return None

    df = None
    try:
        if ext in {'.xlsx', '.xls'}:
            # Пробуем максимально быстрый движок calamine, если он установлен
            try:
                df = pd.read_excel(file_path, dtype=str, engine='calamine')
            except Exception:
                df = pd.read_excel(file_path, dtype=str, engine='openpyxl' if ext == '.xlsx' else None)
        elif ext == '.csv':
            for enc in ENCODINGS_TO_TRY:
                try:
                    df = pd.read_csv(file_path, sep=None, engine='python', dtype=str, on_bad_lines='skip', encoding=enc)
                    break
                except:
                    continue
    except Exception as e:
        logger.error(f'Ошибка чтения {file_path}: {e}')
        return None

    if df is None or df.empty:
        return None

    df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
    if df.empty:
        return None

    df = df.reset_index(drop=True)

    # Поиск начала данных 
    for idx in range(min(10, len(df))):
        row = df.iloc[idx]
        found_email = False
        for val in row:
            s = str(val).strip()
            if '@' in s and '.' in s and ' ' not in s and len(s) > 5:
                found_email = True
                break
        if found_email:
            if idx > 0:
                prev = df.iloc[idx - 1]
                has_text = any(isinstance(v, str) and v.strip() and not v.strip().replace('.', '').isdigit() for v in prev)
                if has_text:
                    df.columns = df.iloc[idx - 1]
                else:
                    df.columns = [f'col_{i}' for i in range(len(df.columns))]
                df = df.iloc[idx:]
            elif idx == 0:
                for col_name in df.columns:
                    if '@' in str(col_name) and '.' in str(col_name):
                        df.columns = [f'col_{i}' for i in range(len(df.columns))]
                        break
            break

    return df.dropna(how='all').reset_index(drop=True)


def _build_col_map(df: pd.DataFrame, mappings: Dict[str, str]) -> Dict[int, str]:
    """Определение типа каждой колонки (один раз на файл)."""
    col_map: Dict[int, str] = {}
    mapped = set()

    for i, col in enumerate(df.columns):
        key = str(col).strip().lower()
        if key in mappings:
            col_map[i] = mappings[key]
            mapped.add(mappings[key])

    if mapped:
        return col_map

    from .city_data import KNOWN_CITIES, COUNTRY_ALIASES

    sample = df.head(30)
    for i in range(len(df.columns)):
        if i in col_map:
            continue
        col = sample.iloc[:, i].dropna().astype(str).str.strip()
        if col.empty:
            continue

        if 'email' not in mapped:
            hits = col.str.contains(r'^[^@\s]+@[^@\s]+\.[a-zA-Z]{2,}$', regex=True, na=False).sum()
            if hits >= min(3, max(1, int(len(col) * 0.3))):
                col_map[i] = 'email'
                mapped.add('email')
                continue

        if 'country_iso2' not in mapped:
            hits = sum(1 for v in col.head(15) if (len(v) == 2 and v.isalpha()) or v.lower() in COUNTRY_ALIASES)
            if hits >= min(4, max(1, int(len(col.head(15)) * 0.4))):
                col_map[i] = 'country_iso2'
                mapped.add('country_iso2')
                continue

        if 'city' not in mapped:
            c_sample = col.head(15)
            hits = sum(1 for v in c_sample if v.lower() in KNOWN_CITIES)
            numeric_count = sum(1 for v in c_sample if str(v).strip().isdigit())
            
            # Если хитов достаточно и колонка не забита цифрами (ID/Index)
            if hits >= min(3, max(1, int(len(c_sample) * 0.3))) and numeric_count < (len(c_sample) / 2):
                col_map[i] = 'city'
                mapped.add('city')
                continue

    return col_map


def parse_file_bulk(file_path: str, source_name: Optional[str] = None) -> List[Dict[str, object]]:
    """Быстрый парсинг файла — возвращает список словарей (не генератор)."""
    path = ph.Path(file_path)
    if not path.exists() or path.suffix.lower() not in SUPPORTED_EXTENSIONS:
        return []

    sz = path.stat().st_size
    if sz > MAX_FILE_SIZE_BYTES or sz == 0:
        return []

    file_context = extract_context_from_path(file_path)
    file_date = extract_file_date(file_path)
    mappings = get_mappings_from_yaml()

    df = _read_dataframe(file_path)
    if df is None:
        return []

    col_map = _build_col_map(df, mappings)
    ncols = len(df.columns)
    fname = path.name

    logger.debug(f'{fname}: {len(df)} строк, колонки: {col_map}')

    results = []
    data = df.values  # numpy — быстрый доступ

    for row_idx in range(len(data)):
        row = data[row_idx]
        rec: Dict[str, object] = {}
        meta: Dict[str, str] = {}

        for ci in range(ncols):
            val = row[ci] if ci < len(row) else None
            if val is None:
                continue
            s = str(val).strip()
            if not s or s.lower() in ('nan', 'none', 'null', 'n/a', ''):
                continue

            if ci in col_map:
                rec[col_map[ci]] = s
            elif 'email' not in rec and '@' in s and '.' in s and len(s) > 5 and ' ' not in s:
                rec['email'] = s
            else:
                meta[f'c{ci}'] = s

        if 'email' not in rec:
            continue

        if meta:
            rec['_raw_meta'] = meta
        rec['_file_context'] = file_context
        rec['_file_date'] = file_date
        rec['_source_file'] = fname
        results.append(rec)

    return results


async def parse_file(file_path: str, source_name: Optional[str] = None):
    """Совместимость со старым async API — оборачивает bulk."""
    rows = parse_file_bulk(file_path, source_name)
    for r in rows:
        yield r

async def stream_process_file(file_path: str, source_name: str):
    """Алиас для обратной совместимости."""
    async for row in parse_file(file_path, source_name):
        yield row
