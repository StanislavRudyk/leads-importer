import os
import re
import asyncio
import pathlib as ph
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set
import pandas as pd
import yaml
import logging
import json
import aiohttp
from .normalizer import normalize_email, normalize_city, normalize_country, extract_file_date, extract_context_from_path, is_garbage

logger = logging.getLogger('leads_importer.parser')

SUPPORTED_EXTENSIONS: Set[str] = {'.csv', '.xlsx', '.xls'}
MAX_FILE_SIZE_BYTES: int = 100 * 1024 * 1024
ENCODINGS_TO_TRY: List[str] = ['utf-8', 'utf-8-sig', 'windows-1251', 'latin-1', 'iso-8859-1']

_YAML_CACHE: Optional[Dict[str, str]] = None

def _build_geography_blacklist() -> set:
    bl = {
        'usa', 'united states', 'europe', 'canada', 'asia', 'africa', 'australia',
        'uk', 'uae', 'middle east', 'south america', 'north america', 'oceania',
        'latin america', 'central america', 'caribbean', 'scandinavia',
        'antarctica', 'worldwide', 'global', 'international', 'domestic',
    }
    try:
        import pycountry
        for c in pycountry.countries:
            bl.add(c.name.lower())
            bl.add(c.alpha_2.lower())
            bl.add(c.alpha_3.lower())
            if hasattr(c, 'common_name'):
                bl.add(c.common_name.lower())
            if hasattr(c, 'official_name'):
                bl.add(c.official_name.lower())
    except: pass
    return bl

GEOGRAPHY_BLACKLIST = _build_geography_blacklist()

GEMINI_API_KEY = "AIzaSyDerQkbPtO3ZczVzQWoXo_TBvTwZVorbIU"
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key={GEMINI_API_KEY}"

async def get_ai_context(file_path: str) -> dict:
    """Uses Gemini to extract city, artist, and dates from filepath."""
    filename = os.path.basename(file_path)
    # Simplify path for AI
    path_str = file_path.replace("\\", "/").split("/")[-3:]
    path_context = " / ".join(path_str)
    
    prompt = f"""
    Analyze the following file path and filename of a leads collection.
    Extract the City, Country (ISO2), US State (if applicable), Artist/Show Name, and Date Range of the collection.
    
    Path Context: {path_context}
    Filename: {filename}
    
    Respond ONLY with a JSON object:
    {{
      "city": "City Name or null",
      "country": "ISO2 or null",
      "state": "2-letter US State or null",
      "artist": "Artist/Show Name or null",
      "start_date": "YYYY-MM-DD or null",
      "end_date": "YYYY-MM-DD or null",
      "status": "done" | "soon" | "active"
    }}
    Default status is 'done' if dates are in the past.
    """
    
    try:
        async with aiohttp.ClientSession() as client:
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "response_mime_type": "application/json",
                    "temperature": 0.1
                }
            }
            async with client.post(GEMINI_URL, json=payload, timeout=12) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    candidates = data.get('candidates', [])
                    if not candidates: return {}
                    text_resp = candidates[0]['content']['parts'][0]['text']
                    return json.loads(text_resp)
                else:
                    err_text = await resp.text()
                    logger.error(f"AI API Error ({resp.status}): {err_text}")
    except Exception as e:
        logger.error(f"AI context extraction failed for {filename}: {e}")
    return {}

def clean_name(name: Optional[str], max_len: int = 50) -> Optional[str]:
    if not name: return None
    n = str(name).strip()
    

    n = re.sub(r'(\.csv|\.xlsx|\.xls)$', '', n, flags=re.I)
    n = re.sub(r'(report|list|database|subscribers|leads|copy|clean|contacts)$', '', n, flags=re.I)
    n = re.sub(r'[\(\)\[\]\{\}]', '', n)
    n = n.strip(' -_+–—')
    
    if len(n) > max_len or len(n) < 2: return None
    if any(char.isdigit() for char in n) and '@' not in n:

        digits = len(re.findall(r'\d', n))
        letters = len(re.findall(r'[a-zA-Z]', n))
        if digits > 6 or (digits > 2 and digits > letters): return None 
    return n

def is_legit_person_name(name: Optional[str]) -> bool:
    if not name: return False
    if is_garbage(name, max_len=40): return False
    n = str(name).strip().lower()
    if n in {'none', 'nan', 'unknown', 'customer', 'patron', 'guest', 'subscriber', 'user', 'n/a', 'null'}:
        return False
    if n in GEOGRAPHY_BLACKLIST:
        return False
    return any(c.isalpha() for c in str(n))

def get_mappings_from_yaml() -> Dict[str, str]:
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
    ext = ph.Path(file_path).suffix.lower()
    if ext not in SUPPORTED_EXTENSIONS:
        return None
    try:
        if ext in {'.xlsx', '.xls', '.xlsm', '.xlsb'}:
            excel_file = pd.ExcelFile(file_path, engine='calamine')
            for sheet in excel_file.sheet_names:
                df = excel_file.parse(sheet, dtype=str, header=None)
                if not df.empty:
                    clean_df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
                    if not clean_df.empty:
                        return df 
            return None
        elif ext == '.csv':
            for enc in ENCODINGS_TO_TRY:
                best_df = None
                for sep in [',', ';', '\t']:
                    try:
                        df = pd.read_csv(file_path, sep=sep, dtype=str, on_bad_lines='skip', encoding=enc, engine='c', low_memory=False)
                        if best_df is None or df.shape[1] > best_df.shape[1]:
                            best_df = df
                        if df.shape[1] > 1:
                            return df
                    except: continue
                if best_df is not None:
                    return best_df
                try:
                    df = pd.read_csv(file_path, sep=None, engine='python', dtype=str, on_bad_lines='skip', encoding=enc)
                    return df
                except: continue
    except Exception as e:
        logger.error(f'Error reading {file_path}: {e}')
    return None

def _is_country_column(col_data) -> bool:
    hits = 0
    for v in col_data:
        low = v.lower().strip()
        if low in GEOGRAPHY_BLACKLIST:
            hits += 1
    return hits >= max(2, int(len(col_data) * 0.5))

def _is_region_column(col_data) -> bool:
    regions = {'asia', 'europe', 'africa', 'oceania', 'north america', 'south america',
               'middle east', 'caribbean', 'latin america', 'central america', 'scandinavia',
               'usa', 'canada'}
    hits = sum(1 for v in col_data if v.lower().strip() in regions)
    return hits >= max(2, int(len(col_data) * 0.5))

def _build_col_map(df: pd.DataFrame) -> Dict[int, str]:
    mappings = get_mappings_from_yaml()
    col_map: Dict[int, str] = {}
    mapped = set()
    for i, col in enumerate(df.columns):
        key = str(col).strip().lower()
        if key in mappings:
            target_field = mappings[key]
            if target_field not in mapped:
                col_map[i] = target_field
                mapped.add(target_field)
    for i, col in enumerate(df.columns):
        if i in col_map: continue
        key = str(col).strip().lower()
        if not key or len(key) < 2: continue
        for synonym, field in mappings.items():
            if field in mapped: continue
            if synonym in key or (len(key) > 3 and key in synonym):
                col_map[i] = field
                mapped.add(field)
                break
    from .city_data import KNOWN_CITIES, COUNTRY_ALIASES
    sample = df.head(50)
    for i in range(len(df.columns)):
        if i in col_map: continue
        col_data = sample.iloc[:, i].dropna().astype(str).str.strip()
        if col_data.empty: continue
        if 'email' not in mapped:
            hits = col_data.str.contains(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$', regex=True, na=False).sum()
            if hits > 0 and hits >= min(2, int(len(col_data) * 0.2)):
                col_map[i] = 'email'
                mapped.add('email')
                continue
        if 'phone' not in mapped:
            hits = col_data.str.contains(r'^\+?\d[\d\s\-()]{7,15}$', regex=True, na=False).sum()
            if hits > 0 and hits >= min(2, int(len(col_data) * 0.3)):
                col_map[i] = 'phone'
                mapped.add('phone')
                continue
        if 'city' not in mapped:
            city_hits = sum(1 for v in col_data if v.lower().strip() in KNOWN_CITIES)
            if city_hits >= max(2, int(len(col_data) * 0.3)):
                col_map[i] = 'city'
                mapped.add('city')
                continue
        if _is_country_column(col_data):
            if 'country_iso2' not in mapped:
                col_map[i] = 'country_iso2'
                mapped.add('country_iso2')
                continue
        if _is_region_column(col_data):
            continue
        if 'first_name' not in mapped or 'last_name' not in mapped:
            valid_hits = 0
            geo_hits = 0
            for v in col_data:
                low = v.lower()
                if low in GEOGRAPHY_BLACKLIST:
                    geo_hits += 1
                elif len(v) > 1 and v[0].isupper() and low not in KNOWN_CITIES and low not in COUNTRY_ALIASES:
                    valid_hits += 1
            if geo_hits > valid_hits:
                continue
            if valid_hits > 0 and valid_hits >= min(5, int(len(col_data) * 0.4)):
                field = 'first_name' if 'first_name' not in mapped else 'last_name'
                col_map[i] = field
                mapped.add(field)
                continue
    return col_map

async def parse_file_bulk(file_path: str, source_name: Optional[str] = None) -> List[Dict[str, object]]:
    ai_ctx = await get_ai_context(file_path)
    
    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, _read_dataframe, file_path)
    if df is None or df.empty:
        return []
    df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
    if df.empty: return []
    df = df.reset_index(drop=True)
    header_idx = -1
    for idx in range(min(100, len(df))):
        row = df.iloc[idx]
        if any(isinstance(v, str) and '@' in v and '.' in v for v in row):
            header_idx = idx
            break
    if header_idx != -1:
        if header_idx > 0:
            potential_header = df.iloc[header_idx - 1]
            if any(isinstance(v, str) and len(v) > 2 for v in potential_header) and not any('@' in str(v) for v in potential_header):
                df.columns = df.iloc[header_idx - 1]
                df = df.iloc[header_idx:]
            else:
                df = df.iloc[header_idx:]
        else:
            df = df.iloc[header_idx:]
    col_map = _build_col_map(df)
    ncols = len(df.columns)
    results = []
    data = df.values
    fname = source_name or os.path.basename(file_path)
    file_context = extract_context_from_path(file_path)
    file_date = extract_file_date(file_path)
    for row_idx in range(len(data)):
        row = data[row_idx]
        rec: Dict[str, object] = {}
        for ci in range(ncols):
            val = row[ci] if ci < len(row) else None
            if val is None: continue
            s = str(val).strip()
            if not s or s.lower() in ('nan', 'none', 'null', 'n/a', ''): continue
            if ci in col_map:
                field = col_map[ci]
                if field == 'city' and s:
                    c, _, _ = normalize_city(s)
                    if not c: s = None
                    else: s = clean_name(c)
                elif field == 'country_iso2' and s:
                    resolved = normalize_country(s)
                    if resolved: s = resolved
                    else: s = None
                elif field in ('first_name', 'last_name') and s:
                    s = clean_name(s, max_len=60)
                    if not s or s.lower() in GEOGRAPHY_BLACKLIST:
                        resolved = normalize_country(s)
                        if resolved:
                            if 'country_iso2' not in rec:
                                rec['country_iso2'] = resolved
                        continue
                elif field == 'email' and s:
                    clean_email = normalize_email(s)
                    if clean_email: rec['email'] = clean_email
                    continue
                
                if s: rec[field] = s
            elif 'email' not in rec and '@' in s and '.' in s and ' ' not in s:
                clean_email = normalize_email(s)
                if clean_email: rec['email'] = clean_email

        if 'email' not in rec: continue
        
        has_name = is_legit_person_name(rec.get('first_name')) or is_legit_person_name(rec.get('last_name'))
        has_phone = bool(str(rec.get('phone') or '').strip() or rec.get('phones'))
        has_geo = bool(rec.get('city') or rec.get('country_iso2'))
        
        # Accept if email + at least one useful field
        if not has_name and not has_phone and not has_geo:
            continue 
        
        if rec.get('first_name') and not rec.get('last_name'):
            fn = str(rec['first_name']).strip()
            if ' ' in fn:
                parts = fn.split(' ', 1)
                rec['first_name'] = parts[0]
                rec['last_name'] = parts[1]
        
        # Capture context from file (AI takes priority, then fallback to path analysis)
        if not rec.get('city'): rec['city'] = ai_ctx.get('city') or file_context.get('folder_city')
        if not rec.get('country_iso2'): rec['country_iso2'] = ai_ctx.get('country') or file_context.get('folder_country')
        if not rec.get('state'): rec['state'] = ai_ctx.get('state') or file_context.get('folder_state')
        
        # New context fields
        rec['show_context'] = ai_ctx.get('artist') or file_context.get('show_context') or fname
        rec['collection_start'] = ai_ctx.get('start_date') or file_context.get('collection_start') or file_date
        rec['collection_end'] = ai_ctx.get('end_date') or file_context.get('collection_end') or file_date
        rec['show_state'] = ai_ctx.get('status', 'done')
        
        if file_date: rec['file_created_at'] = file_date
        rec['_source_file'] = fname
        results.append(rec)
    return results

async def parse_file(file_path: str, source_name: Optional[str] = None):
    rows = await parse_file_bulk(file_path, source_name)
    for r in rows:
        yield r

async def stream_process_file(file_path: str, source_name: str):
    async for row in parse_file(file_path, source_name):
        yield row
