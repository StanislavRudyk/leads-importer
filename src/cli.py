import logging
import asyncio
import argparse
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src.parser import parse_file_bulk
from src.normalizer import (
    normalize_email,
    normalize_phone,
    normalize_city,
    normalize_country,
    normalize_state,
    normalize_nationality,
    extract_file_date,
    extract_context_from_path,
)
from src.merger import (
    merge_lead_fields,
    deduplicate_batch,
    build_import_history_entry,
)
from src.db import AsyncSessionLocal, upsert_leads_batch, create_import_log

logger = logging.getLogger('leads_importer.cli')



BATCH_SIZE = 2500


def _normalize_row(raw_row: Dict[str, Any], source_name: str) -> Optional[Dict[str, Any]]:
    """Нормализация сырой строки в объект лида."""
    email = normalize_email(raw_row.get('email'))
    if not email:
        return None

    file_context = raw_row.get('_file_context', {})
    file_date = raw_row.get('_file_date')
    source_file = raw_row.get('_source_file', '')
    country_hint = file_context.get('folder_country')
    state_hint = file_context.get('folder_state')

    raw_country = raw_row.get('country_iso2') or raw_row.get('country')
    country_iso2 = normalize_country(raw_country) or country_hint

    raw_city = raw_row.get('city') or file_context.get('folder_city')
    city_name, city_country, city_state = normalize_city(raw_city, country_hint=country_iso2 or country_hint, state_hint=state_hint)

    # Фоллбек: попытка найти город в сырых данных
    if not city_name:
        raw_meta = raw_row.get('_raw_meta', {})
        for v in raw_meta.values():
            candidate = str(v).strip()
            if 3 <= len(candidate) <= 50 and '@' not in candidate and not candidate.isdigit():
                test_city, test_country, test_state = normalize_city(candidate, country_hint=country_iso2)
                if test_city and test_country:
                    city_name, city_country, city_state = test_city, test_country, test_state
                    break

    if not country_iso2 and city_country:
        country_iso2 = city_country

    state = normalize_state(state=raw_row.get('state'), city=city_name, country=country_iso2) or city_state or state_hint

    # Телефон: быстрая проверка перед тяжёлым парсингом
    raw_phone = raw_row.get('phone')
    phone_e164, raw_phone_str = None, None
    if raw_phone:
        p = str(raw_phone).strip()
        if p and len(p) >= 7 and p.lower() not in ('nan', 'none', 'null', 'n/a'):
            phone_e164, raw_phone_str = normalize_phone(p, default_region=country_iso2)

    import_entry = build_import_history_entry(source_file=source_file, source_name=source_name, file_date=file_date, raw_data=raw_row.get('_raw_meta', {}))

    return {
        'email': email,
        'phone': phone_e164,
        'phones': [phone_e164] if phone_e164 else [],
        'first_name': _clean_name(raw_row.get('first_name')),
        'last_name': _clean_name(raw_row.get('last_name')),
        'country_iso2': country_iso2,
        'nationality': normalize_nationality(raw_row.get('nationality')),
        'city': city_name,
        'state': state,
        'language': _clean_string(raw_row.get('language')),
        'latest_source': source_name,
        'latest_campaign': None,
        'status': 'new',
        'is_buyer': _parse_boolean(raw_row.get('is_buyer')),
        'tags': _parse_tags(raw_row.get('tags')),
        'meta_info': {'import_history': [import_entry], **({'raw_phones': [raw_phone_str]} if raw_phone_str and not phone_e164 else {})},
        'file_created_at': file_date,
        '_file_date': file_date,
        'import_count': 1,
    }


def _clean_name(value: Any) -> Optional[str]:
    """Очистка имени/фамилии."""
    if value is None: return None
    s = str(value).strip()
    if not s or s.lower() in ('nan', 'none', 'null', 'n/a', '-', '.') or s.replace(' ', '').isdigit(): return None
    return s.title()


def _clean_string(value: Any) -> Optional[str]:
    """Базовая очистка строки."""
    if value is None: return None
    s = str(value).strip()
    if not s or s.lower() in ('nan', 'none', 'null', 'n/a', '-', '.'): return None
    return s


def _parse_boolean(value: Any) -> bool:
    """Преобразование в булево."""
    if value is None: return False
    if isinstance(value, bool): return value
    return str(value).strip().lower() in ('true', '1', 'yes', 'y', 't')


def _parse_tags(value: Any) -> List[str]:
    """Парсинг тегов."""
    if value is None: return []
    if isinstance(value, list): return [str(t).strip() for t in value if str(t).strip()]
    s = str(value).strip()
    if not s or s.lower() in ('nan', 'none', 'null', '[]'): return []
    return [t.strip() for t in s.split(',') if t.strip()]


async def run_import(file_path: str, source_name: str = 'default') -> Dict[str, Any]:
    """Быстрый импорт: bulk-парсинг → нормализация → пакетная запись."""
    start_time = time.time()
    if not os.path.exists(file_path):
        return {'status': 'error', 'message': f'File not found', 'duration': 0}

    filename = os.path.basename(file_path)

    # 1. Bulk-парсинг 
    t0 = time.time()
    import anyio

    # 1. Bulk-парсинг (выносим в поток, так как это CPU-bound синхронная задача)
    t0 = time.time()
    raw_rows = await anyio.to_thread.run_sync(parse_file_bulk, file_path, source_name)
    parse_time = round(time.time() - t0, 2)
    total_parsed = len(raw_rows)

    if total_parsed == 0:
        async with AsyncSessionLocal() as session:
            import_id = await create_import_log(session, filename, source_name, 0, 0, 0, 0, 'empty')
        return {'import_id': import_id, 'filename': filename, 'rows_total': 0, 'rows_inserted': 0, 'rows_updated': 0, 'rows_skipped': 0, 'status': 'empty', 'duration': round(time.time() - start_time, 2)}

    # 2. Bulk-нормализация (выносим в поток)
    t0 = time.time()
    def _bulk_norm(rows, src):
        out = []
        skipped = 0
        for i, r in enumerate(rows):
            n = _normalize_row(r, src)
            if n: out.append(n)
            else: skipped += 1
            # Forcibly yield the GIL every 500 records so FastAPI can serve requests
            if i % 500 == 0:
                time.sleep(0.001)
        return out, skipped

    normalized, skipped_normalize = await anyio.to_thread.run_sync(_bulk_norm, raw_rows, source_name)
    norm_time = round(time.time() - t0, 2)

    # 3. Пакетная запись в БД (одна сессия для upsert + import_log)
    t0 = time.time()
    total_inserted, total_updated, total_skipped_db = 0, 0, 0

    async with AsyncSessionLocal() as session:
        try:
            deduped = deduplicate_batch(normalized)
            total_inserted, total_updated, total_skipped_db = await upsert_leads_batch(session, deduped, source_name)
        except Exception as exc:
            logger.error(f'DB error [{filename}]: {exc}')
            import_id = await create_import_log(session, filename, source_name, total_parsed, 0, 0, total_parsed, 'failed', {'error': str(exc)})
            return {'import_id': import_id, 'status': 'error', 'message': str(exc), 'duration': round(time.time() - start_time, 2)}

        db_time = round(time.time() - t0, 2)
        duration = round(time.time() - start_time, 2)
        total_skipped = skipped_normalize + total_skipped_db

        status = 'failed' if total_inserted + total_updated == 0 and total_parsed > 0 else ('partial' if total_skipped > total_parsed * 0.5 else 'success')

        import_id = await create_import_log(session, filename, source_name, total_parsed, total_inserted, total_updated, total_skipped, status)
    logger.info(f'{filename}: {total_parsed} rows (parse={parse_time}s, norm={norm_time}s, db={db_time}s) → +{total_inserted} new, ~{total_updated} upd')

    results = {'import_id': import_id, 'filename': filename, 'rows_total': total_parsed, 'rows_inserted': total_inserted, 'rows_updated': total_updated, 'rows_skipped': total_skipped, 'status': status, 'duration': duration}

    try:
        from src.notifier import notifier
        await notifier.send_import_summary(results)
    except Exception as exc:
        logger.error(f'Error sending import summary [{filename}]: {exc}')
    return results


async def _flush_batch(session, batch, source_name):
    """Дедупликация и запись батча."""
    deduped = deduplicate_batch(batch)
    return await upsert_leads_batch(session, deduped, source_name)


if __name__ == '__main__':
    logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', required=True)
    parser.add_argument('--source', default=None)
    args = parser.parse_args()
    asyncio.run(run_import(args.file, args.source or os.path.splitext(os.path.basename(args.file))[0]))
