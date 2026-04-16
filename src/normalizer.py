import os
import re
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, List
import phonenumbers
from phonenumbers import PhoneNumberFormat
import pycountry
from email_validator import validate_email, EmailNotValidError
from .iata_codes import IATA_TO_CITY
from .city_data import (
    KNOWN_CITIES,
    CITY_TYPOS,
    NOT_CITIES,
    COUNTRY_ALIASES,
    AMBIGUOUS_CITIES,
    FOLDER_CITY_OVERRIDES,
    FOLDER_TO_CONTEXT,
    FILENAME_NOISE, 
    FILENAME_LANGUAGES
)

_EMAIL_RE = re.compile(
    r'^[a-zA-Z0-9]'
    r'[a-zA-Z0-9._%+\-]*'
    r'@'
    r'[a-zA-Z0-9]'
    r'[a-zA-Z0-9.\-]*'
    r'\.[a-zA-Z]{2,10}$'
)

_JUNK_TAIL_RE = re.compile(r'[.;,\s]+$')
_JUNK_HEAD_RE = re.compile(r'^[.;,\s]+')
_DATE_PATTERN_1 = re.compile(r'(\d{1,2})[-/_](\d{1,2})[-/_](\d{2,4})')
_DATE_PATTERN_2 = re.compile(r'(\d{4})[-/_](\d{1,2})[-/_](\d{1,2})')
_YEAR_RE = re.compile(r'\b(20\d{2})\b')
_PHONE_STRIP_RE = re.compile(r'[^\d+\-() ]')

DEMONYMS = {
    'US': 'American', 'GB': 'British', 'CA': 'Canadian', 'AU': 'Australian',
    'NZ': 'New Zealander', 'IE': 'Irish', 'FR': 'French', 'DE': 'German',
    'IT': 'Italian', 'ES': 'Spanish', 'PT': 'Portuguese', 'NL': 'Dutch',
    'BE': 'Belgian', 'AT': 'Austrian', 'CH': 'Swiss', 'SE': 'Swedish',
    'NO': 'Norwegian', 'DK': 'Danish', 'FI': 'Finnish', 'PL': 'Polish',
    'CZ': 'Czech', 'HU': 'Hungarian', 'RO': 'Romanian', 'GR': 'Greek',
    'TR': 'Turkish', 'RU': 'Russian', 'UA': 'Ukrainian', 'IL': 'Israeli',
    'IR': 'Iranian', 'AE': 'Emirati', 'SA': 'Saudi', 'EG': 'Egyptian',
    'JP': 'Japanese', 'KR': 'Korean', 'CN': 'Chinese', 'IN': 'Indian',
    'BR': 'Brazilian', 'MX': 'Mexican', 'AR': 'Argentine', 'CO': 'Colombian',
    'ZA': 'South African', 'NG': 'Nigerian', 'KE': 'Kenyan', 'PH': 'Filipino',
    'TH': 'Thai', 'MY': 'Malaysian', 'ID': 'Indonesian', 'VN': 'Vietnamese',
    'PK': 'Pakistani', 'BD': 'Bangladeshi', 'LK': 'Sri Lankan',
    'SG': 'Singaporean', 'HK': 'Hong Konger',
}

JUNK_EMAIL_PATTERNS = [
    'sendaninstantmessage',
    'instantmessage',
    'clickhere',
    'unsubscribe',
    'viewinbrowser'
]

def is_garbage(text: object, max_len: int = 45) -> bool:
    if text is None: return True
    s = str(text).strip()
    if not s or len(s) < 2: return True
    if len(s) > max_len: return True
    
    # Technical noise patterns
    lower = s.lower()
    if lower in ('nan', 'none', 'null', 'n/a', 'unknown', 'undefined', 'contacts', 'subscribers', 'leads'):
        return True
    if any(p in lower for p in ('.csv', '.xlsx', '.xls', '.txt', 'copy of', 'export_', 'database_')):
        return True
    
    # Excessive punctuation or non-alphabetic characters
    if len(re.findall(r'[!@#$%^&*()_+={}\[\]|\\:;"<>,.?/]', s)) > 2: return True
    
    # Entire headers or sentences (too many words)
    words = s.split()
    if len(words) > 4: return True
    
    # Too many digits vs letters (likely IDs or dates)
    digits = len(re.findall(r'\d', s))
    letters = len(re.findall(r'[a-zA-Z]', s))
    if digits > 4 and digits > letters: return True
    
    return False

def normalize_email(email: object) -> Optional[str]:
    if email is None: return None
    CORPORATE_PREFIXES = {
        'info', 'admin', 'office', 'support', 'sales', 'contact', 'billing', 'help', 'service',
        'marketing', 'hello', 'mail', 'postmaster', 'webmaster', 'jobs', 'enquiry', 'team',
        'noreply', 'no-reply', 'orders', 'bookings', 'tickets', 'events', 'press', 'media',
        'reception', 'hr', 'humanresources', 'account', 'accounting', 'legal',
        'management', 'manager', 'ceo', 'cto', 'president', 'staff', 'member', 'general'
    }
    email_str = str(email).strip().lower()
    if not email_str: return None
    for p in JUNK_EMAIL_PATTERNS:
        if p in email_str: return None
    try:
        v = validate_email(email_str, check_deliverability=False)
        clean_email = v.normalized
    except EmailNotValidError:
        return None
    prefix = clean_email.split('@')[0]
    if prefix in CORPORATE_PREFIXES:
        return None
    return clean_email

_phone_cache: Dict[str, Tuple[Optional[str], Optional[str]]] = {}

def normalize_phone(phone: object, default_region: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
    if phone is None: return (None, None)
    phone_str = str(phone).strip()
    if not phone_str or phone_str.lower() in ('nan', 'none', 'null', 'n/a', ''):
        return (None, None)
    raw_phone = phone_str
    cache_key = f'{phone_str}|{default_region or ""}'
    if cache_key in _phone_cache: return _phone_cache[cache_key]
    phone_str = phone_str.replace('\u00a0', ' ')
    phone_str = _PHONE_STRIP_RE.sub('', phone_str).strip()
    if not phone_str or len(phone_str) < 7:
        res = (None, raw_phone)
        _phone_cache[cache_key] = res
        return res
    regions = []
    if phone_str.startswith('+'): regions.append(None)
    else:
        if default_region: regions.append(default_region)
        regions.append('US')
        regions.append('GB')
    for region in regions:
        try:
            parsed = phonenumbers.parse(phone_str, region)
            if phonenumbers.is_valid_number(parsed):
                e164 = phonenumbers.format_number(parsed, PhoneNumberFormat.E164)
                res = (e164, raw_phone)
                _phone_cache[cache_key] = res
                return res
        except: continue
    res = (None, raw_phone)
    _phone_cache[cache_key] = res
    return res

def normalize_country(country: object) -> Optional[str]:
    if country is None: return None
    raw = str(country).strip()
    if not raw or raw.lower() in ('nan', 'none', 'null', 'n/a', '', '-', '--', '.'):
        return None
    lower = raw.lower()
    if lower in COUNTRY_ALIASES: return COUNTRY_ALIASES[lower]
    if len(raw) == 2 and raw.isalpha():
        code = raw.upper()
        try:
            if pycountry.countries.get(alpha_2=code): return code
        except: pass
        return code
    try:
        info = pycountry.countries.get(name=raw) or pycountry.countries.get(official_name=raw)
        if info: return info.alpha_2
    except: pass
    try:
        results = pycountry.countries.search_fuzzy(raw)
        if results: return results[0].alpha_2
    except: pass
    return None

def normalize_city(city: object, country_hint: Optional[str] = None, state_hint: Optional[str] = None) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if city is None: return (None, None, None)
    if is_garbage(city, max_len=50): return (None, None, None)
    raw = str(city).strip()
    if not raw or raw.lower() in ('nan', 'none', 'null', 'n/a', '', '-', '--', '.'):
        return (None, None, None)
    NOISE_WORDS = {
        'report', 'list', 'database', 'copy', 'attendee', 'contacts', 'ticketbuyers',
        'members', 'export', 'subscribers', 'signup', 'presales', 'presale', 'subz',
        'ticket', 'tickets', 'buyers', 'industry', 'phone', 'numbers', 'number',
        'email', 'emails', 'data', 'sheet', 'file', 'total', 'full', 'site',
        'ticketmaster', 'mailchimp', 'eventbrite', 'campaign', 'newsletter',
        'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october',
        'november', 'december', 'january', 'february', 'english', 'spanish', 'french',
        'german', 'italian', 'portuguese', 'dutch', 'swedish', 'norwegian', 'danish',
        'finnish', 'polish', 'czech', 'hungarian', 'romanian', 'greek', 'turkish',
        'russian', 'ukrainian', 'iranian', 'arab', 'arabic', 'persian', 'farsi',
    }
    raw_cleaned = re.sub(r'[\d\-()\[\]]+', ' ', raw)
    parts = raw_cleaned.split()
    clean_parts = [p for p in parts if p.lower() not in NOISE_WORDS and len(p) > 1]
    if not clean_parts: return (None, None, None)
    clean_city = ' '.join(clean_parts).strip()
    lower = clean_city.lower()
    if len(lower) < 2: return (None, None, None)
    if country_hint and lower == country_hint.lower(): return (None, None, None)
    if lower in COUNTRY_ALIASES or normalize_country(lower) == lower.upper(): return (None, None, None)
    if lower in CITY_TYPOS: lower = CITY_TYPOS[lower]
    if lower in AMBIGUOUS_CITIES:
        options = AMBIGUOUS_CITIES[lower]
        if country_hint and country_hint in options:
            city_name, state = options[country_hint]
            return (city_name, country_hint, state)
        f_country = next(iter(options))
        city_name, state = options[f_country]
        return (city_name, f_country, state)
    if lower in KNOWN_CITIES:
        known_country, known_state = KNOWN_CITIES[lower]
        if country_hint and country_hint != known_country:
            if lower in AMBIGUOUS_CITIES and country_hint in AMBIGUOUS_CITIES[lower]:
                city_name, state = AMBIGUOUS_CITIES[lower][country_hint]
                return (city_name, country_hint, state)
        return (_title_case_city(lower), known_country, known_state or state_hint)
    
    if re.search(r'[^a-zA-Z\s\-\']', lower): return (None, None, None)
    if len(lower.split()) > 3: return (None, None, None)
    
    return (_title_case_city(lower), country_hint, state_hint)

def _title_case_city(city_lower: str) -> str:
    return city_lower.title()

def normalize_state(state: object, city: Optional[str] = None, country: Optional[str] = None) -> Optional[str]:
    from .iata_codes import US_STATE_TO_ABBR, US_ABBR_SET
    if state is not None:
        raw = str(state).strip()
        if raw and raw.lower() not in ('nan', 'none', 'null', 'n/a', '', '-'):
            lower = raw.lower()
            if lower in US_STATE_TO_ABBR: return US_STATE_TO_ABBR[lower]
            upper = raw.upper()
            if len(upper) == 2 and upper in US_ABBR_SET: return upper
            if country and country != 'US': return raw.title()
    if city and country in ('US', None):
        city_lower = city.lower()
        if city_lower in KNOWN_CITIES:
            known_country, known_state = KNOWN_CITIES[city_lower]
            if known_country == 'US' and known_state: return known_state
    return None

def normalize_nationality(nationality: object) -> Optional[str]:
    if nationality is None: return None
    raw = str(nationality).strip()
    if not raw or raw.lower() in ('nan', 'none', 'null', 'n/a', '', '-'):
        return None
    if len(raw) >= 4 and raw[0].isupper(): return raw
    upper = raw.upper().strip()
    if upper in DEMONYMS: return DEMONYMS[upper]
    code = normalize_country(raw)
    if code and code in DEMONYMS: return DEMONYMS[code]
    return raw.title()

def extract_file_date(file_path: str) -> Optional[datetime]:
    filename = os.path.basename(file_path)
    stem = os.path.splitext(filename)[0]
    date_from_name = _extract_date_from_string(stem)
    if date_from_name: return date_from_name
    try:
        mtime = os.path.getmtime(file_path)
        return datetime.fromtimestamp(mtime, tz=timezone.utc)
    except: return None

def _extract_date_from_string(text: str) -> Optional[datetime]:
    match = _DATE_PATTERN_2.search(text)
    if match:
        try:
            y, m, d = int(match.group(1)), int(match.group(2)), int(match.group(3))
            if 1 <= m <= 12 and 1 <= d <= 31: return datetime(y, m, d, tzinfo=timezone.utc)
        except: pass
    match = _DATE_PATTERN_1.search(text)
    if match:
        try:
            p1, p2, p3 = int(match.group(1)), int(match.group(2)), int(match.group(3))
            year = 2000 + p3 if p3 < 100 else p3
            if p1 <= 12 and p2 <= 31: m, d = p1, p2
            elif p2 <= 12 and p1 <= 31: d, m = p1, p2
            else: return None
            if 2000 <= year <= 2030 and 1 <= m <= 12 and 1 <= d <= 31: return datetime(year, m, d, tzinfo=timezone.utc)
        except: pass
    match = _YEAR_RE.search(text)
    if match:
        try:
            y = int(match.group(1))
            if 2000 <= y <= 2030: return datetime(y, 1, 1, tzinfo=timezone.utc)
        except: pass
    return None

def extract_context_from_path(file_path: str) -> dict:
    res = {'folder_country': None, 'folder_region': None, 'folder_city': None, 'folder_state': None}
    normalized = file_path.replace('\\', '/')
    parts = [p.strip() for p in normalized.split('/') if p.strip()]
    for part in parts:
        part_lower = part.lower()
        if part_lower in FOLDER_TO_CONTEXT:
            ctx = FOLDER_TO_CONTEXT[part_lower]
            if ctx.get('country') and not res['folder_country']: res['folder_country'] = ctx['country']
            if ctx.get('region') and not res['folder_region']: res['folder_region'] = ctx['region']
        if part_lower in FOLDER_CITY_OVERRIDES:
            city_name, country, state = FOLDER_CITY_OVERRIDES[part_lower]
            if not res['folder_city']: res['folder_city'] = city_name
            if not res['folder_country']: res['folder_country'] = country
            if not res['folder_state']: res['folder_state'] = state
    if not res['folder_city']:
        f_city = _extract_city_from_filename(os.path.basename(file_path))
        if f_city:
            city_lower = f_city.lower()
            if city_lower in CITY_TYPOS: city_lower = CITY_TYPOS[city_lower]
            if city_lower in KNOWN_CITIES:
                known_country, known_state = KNOWN_CITIES[city_lower]
                res['folder_city'] = _title_case_city(city_lower)
                if not res['folder_country']: res['folder_country'] = known_country
                if not res['folder_state']: res['folder_state'] = known_state
            else: res['folder_city'] = f_city
    return res

def _extract_city_from_filename(filename: str) -> Optional[str]:
    stem = os.path.splitext(filename)[0]
    stem = _DATE_PATTERN_1.sub('', stem)
    stem = _DATE_PATTERN_2.sub('', stem)
    stem = _YEAR_RE.sub('', stem)
    stem = re.sub(r'[\s\-–—]+$', '', stem) 
    stem = re.sub(r'^[\s\-–—]+', '', stem).strip() 
    if not stem: return None
    split_parts = [p.strip() for p in re.split(r'\s*[-–—]\s+|\s+[-–—]\s*|\s*[-–—](?=[A-Z])', stem) if p.strip()]
    if not split_parts: return None
    city_candidate = split_parts[0].strip(' -–—')
    if not city_candidate: return None
    words = city_candidate.split()
    f_word_upper = words[0].upper().strip('.,;:-()[]{}') if words else ''
    if len(f_word_upper) == 3 and f_word_upper.isalpha() and f_word_upper in IATA_TO_CITY:
        iata_city, _ = IATA_TO_CITY[f_word_upper]
        return iata_city
    res_words = []
    for w in words:
        wl = w.lower().strip('.,;:-()[]{}')
        if not wl or wl in FILENAME_NOISE or wl in FILENAME_LANGUAGES or wl.isdigit() or (len(wl) <= 1 and not wl.isalpha()) or (re.match(r'^[ivxIVX]+$', wl) and len(wl) <= 5):
            continue
        res_words.append(w)
    if not res_words: return None
    cand_lower = ' '.join(res_words).strip(' -–—').lower()
    if len(cand_lower) < 2: return None
    if cand_lower in CITY_TYPOS: cand_lower = CITY_TYPOS[cand_lower]
    return _title_case_city(cand_lower)
