from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import logging

logger = logging.getLogger('leads_importer.merger')

def _is_empty(value: Any) -> bool:
    """Helper to check for null, empty, or placeholder values."""
    if value is None:
        return True
    if isinstance(value, str) and value.strip() in ('', 'nan', 'none', 'null', 'n/a'):
        return True
    if isinstance(value, (list, dict)) and len(value) == 0:
        return True
    return False

def _is_non_empty(value: Any) -> bool:
    """Helper to check for valid non-empty values."""
    return not _is_empty(value)

def merge_lead_fields(
    existing: Dict[str, Any],
    incoming: Dict[str, Any],
    incoming_file_date: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Merge incoming lead data into an existing record using priority and recency rules."""
    existing = existing.copy()
    existing_file_date = existing.get('_file_date')

    incoming_is_newer = True
    if existing_file_date and incoming_file_date:
        incoming_is_newer = incoming_file_date >= existing_file_date
    elif existing_file_date and not incoming_file_date:
        incoming_is_newer = False

    if _is_non_empty(incoming.get('phone')):
        if _is_empty(existing.get('phone')) or incoming_is_newer:
            existing['phone'] = incoming['phone']

    existing_phones = set(existing.get('phones') or [])
    incoming_phones = set(incoming.get('phones') or [])
    if _is_non_empty(existing.get('phone')):
        existing_phones.add(existing['phone'])
    if _is_non_empty(incoming.get('phone')):
        existing_phones.add(incoming['phone'])
    existing['phones'] = [p for p in list(existing_phones | incoming_phones) if p and str(p).strip()]

    for field in ('first_name', 'last_name', 'country_iso2', 'city', 'state', 'nationality', 'language'):
        if _is_empty(existing.get(field)) and _is_non_empty(incoming.get(field)):
            existing[field] = incoming[field]

    if incoming.get('is_buyer') is True:
        existing['is_buyer'] = True

    for field in ('latest_source', 'latest_campaign'):
        if _is_non_empty(incoming.get(field)) and incoming_is_newer:
            existing[field] = incoming[field]
        elif _is_empty(existing.get(field)) and _is_non_empty(incoming.get(field)):
            existing[field] = incoming[field]

    existing_tags = set(existing.get('tags') or [])
    incoming_tags = set(incoming.get('tags') or [])
    existing['tags'] = sorted(list(existing_tags | incoming_tags))

    manual_statuses = {'contacted', 'qualified', 'negotiation', 'won', 'lost', 'archived'}
    if existing.get('status', 'new') not in manual_statuses:
        if _is_non_empty(incoming.get('status')) and incoming.get('status') != 'new':
            existing['status'] = incoming['status']

    existing_meta = existing.get('meta_info') or {}
    incoming_meta = incoming.get('meta_info') or {}
    existing_history = existing_meta.get('import_history', [])
    incoming_history = incoming_meta.get('import_history', [])
    
    existing_raw_phones = set(existing_meta.get('raw_phones', []))
    incoming_raw_phones = set(incoming_meta.get('raw_phones', []))
    
    existing_meta['import_history'] = (existing_history if isinstance(existing_history, list) else []) + (incoming_history if isinstance(incoming_history, list) else [])
    merged_raw = sorted(list(existing_raw_phones | incoming_raw_phones))
    if merged_raw:
        existing_meta['raw_phones'] = merged_raw
    existing['meta_info'] = existing_meta

    if _is_empty(existing.get('brevo_id')) and _is_non_empty(incoming.get('brevo_id')):
        existing['brevo_id'] = incoming['brevo_id']

    if incoming_is_newer and incoming_file_date:
        existing['_file_date'] = incoming_file_date

    existing['import_count'] = (existing.get('import_count') or 1) + 1
    return existing

def deduplicate_batch(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deduplicate a list of leads by email within a single batch."""
    unique: Dict[str, Dict[str, Any]] = {}
    for record in records:
        email = record.get('email')
        if not email:
            continue
        if email in unique:
            unique[email] = merge_lead_fields(unique[email], record, record.get('_file_date'))
        else:
            unique[email] = record.copy()
    return list(unique.values())

def build_import_history_entry(source_file, source_name, file_date=None, raw_data=None) -> Dict[str, Any]:
    """Generate a history entry for a lead's meta_info audit trail."""
    entry = {
        'imported_at': datetime.now(tz=timezone.utc).isoformat(),
        'source_file': source_file,
        'source_name': source_name,
    }
    if file_date:
        entry['file_date'] = file_date.isoformat()
    if raw_data:
        limited = {}
        for idx, (k, v) in enumerate(raw_data.items()):
            if idx >= 20: break
            limited[str(k)[:100]] = str(v)[:500]
        entry['raw_data'] = limited
    return entry
