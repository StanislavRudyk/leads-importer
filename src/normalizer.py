import os
import re
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, List

import phonenumbers
from phonenumbers import PhoneNumberFormat
import pycountry

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

_JUNK_EMAIL_PREFIXES = frozenset({
    'noreply', 'no-reply', 'no.reply', 'donotreply', 'do-not-reply',
    'do.not.reply', 'mailer-daemon', 'mailer.daemon', 'postmaster',
    'hostmaster', 'abuse', 'bounce', 'bounces', 'automated',
    'system', 'daemon', 'devnull', 'null', 'nobody',
    'info', 'admin', 'sales', 'support', 'contact', 'hello', 
    'webmaster', 'marketing', 'office', 'service', 'billing', 
    'orders', 'press', 'jobs', 'careers', 'hr', 'management', 
    'help', 'inquiries', 'ask', 'finance', 'accounting', 'legal', 
    'media', 'team', 'events', 'privacy',
})

_JUNK_EMAIL_DOMAINS = frozenset({
    'example.com', 'example.org', 'example.net',
    'test.com', 'test.org', 'test.net', 'localhost',
    'invalid.com', 'invalid.org',
    'mailinator.com', 'guerrillamail.com', 'tempmail.com',
    'throwaway.email', 'sharklasers.com', 'yopmail.com',
    'trashmail.com', 'fakeinbox.com', 'guerrillamail.net',
    'grr.la', 'guerrillamailblock.com', 'tempail.com',
    'dispostable.com', 'maildrop.cc', 'temp-mail.org',
})

_DATE_PATTERN_1 = re.compile(r'(\d{1,2})[-/_](\d{1,2})[-/_](\d{2,4})')
_DATE_PATTERN_2 = re.compile(r'(\d{4})[-/_](\d{1,2})[-/_](\d{1,2})')
_YEAR_RE = re.compile(r'\b(20\d{2})\b')

_PHONE_STRIP_RE = re.compile(r'[^\d+\-() ]')

demonyms = {
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

def normalize_email(email: object) -> Optional[str]:
    """Нормализация email: извлечение адреса, приведение к нижнему регистру и валидация."""
    if email is None:
        return None

    email_str = str(email).strip().lower()
    if not email_str or email_str in ('nan', 'none', 'null', 'n/a', ''):
        return None

    match = re.search(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', email_str)
    if not match:
        return None

    clean_email = match.group(0).replace(' ', '')

    if '..' in clean_email or clean_email.startswith('.') or clean_email.endswith('.'):
        return None

    # Фильтрация мусорных/системных email
    local_part, domain = clean_email.rsplit('@', 1)
    if local_part in _JUNK_EMAIL_PREFIXES:
        return None
    if domain in _JUNK_EMAIL_DOMAINS:
        return None

    return clean_email

_phone_cache: Dict[str, Tuple[Optional[str], Optional[str]]] = {}

def normalize_phone(phone: object, default_region: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
    """Быстрая нормализация телефона в E.164 (с кешем)."""
    if phone is None:
        return (None, None)

    phone_str = str(phone).strip()
    if not phone_str or phone_str.lower() in ('nan', 'none', 'null', 'n/a', ''):
        return (None, None)

    raw_phone = phone_str

    cache_key = f'{phone_str}|{default_region or ""}'
    if cache_key in _phone_cache:
        return _phone_cache[cache_key]

    phone_str = phone_str.replace('\u00a0', ' ')
    phone_str = _PHONE_STRIP_RE.sub('', phone_str).strip()

    if not phone_str or len(phone_str) < 7:
        result = (None, raw_phone)
        _phone_cache[cache_key] = result
        return result

    regions = []
    if phone_str.startswith('+'):
        regions.append(None)  
    else:
        if default_region:
            regions.append(default_region)
        regions.append('US')
        if default_region not in (None, 'US', 'GB'):
            pass
        else:
            regions.append('GB')

    for region in regions:
        try:
            parsed = phonenumbers.parse(phone_str, region)
            if phonenumbers.is_valid_number(parsed):
                e164 = phonenumbers.format_number(parsed, PhoneNumberFormat.E164)
                result = (e164, raw_phone)
                _phone_cache[cache_key] = result
                return result
        except:
            continue

    result = (None, raw_phone)
    _phone_cache[cache_key] = result
    return result

def normalize_country(country: object) -> Optional[str]:
    """Приведение названия страны или кода к стандарту ISO 3166-1 alpha-2."""
    if country is None:
        return None

    raw = str(country).strip()
    if not raw or raw.lower() in ('nan', 'none', 'null', 'n/a', '', '-', '--', '.'):
        return None

    lower = raw.lower()
    if lower in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[lower]

    if len(raw) == 2 and raw.isalpha():
        code = raw.upper()
        try:
            if pycountry.countries.get(alpha_2=code):
                return code
        except:
            pass
        return code

    try:
        info = pycountry.countries.get(name=raw) or pycountry.countries.get(official_name=raw)
        if info:
            return info.alpha_2
    except:
        pass

    try:
        results = pycountry.countries.search_fuzzy(raw)
        if results:
            return results[0].alpha_2
    except:
        pass

    return None

def normalize_city(
    city: object,
    country_hint: Optional[str] = None,
    state_hint: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Нормализация названия города с определением страны и штата на основе контекста."""
    if city is None:
        return (None, None, None)

    raw = str(city).strip()
    if not raw or raw.lower() in ('nan', 'none', 'null', 'n/a', '', '-', '--', '.'):
        return (None, None, None)

    lower = raw.lower().strip('.,;:-')
    if lower in NOT_CITIES:
        return (None, None, None)

    upper = raw.upper().strip()
    if len(upper) == 3 and upper.isalpha() and upper in IATA_TO_CITY:
        iata_city, iata_country = IATA_TO_CITY[upper]
        iata_state = None
        iata_lower = iata_city.lower()
        if iata_lower in KNOWN_CITIES:
            _, iata_state = KNOWN_CITIES[iata_lower]
        return (iata_city, iata_country, iata_state)

    if lower in CITY_TYPOS:
        lower = CITY_TYPOS[lower]

    if lower in AMBIGUOUS_CITIES:
        options = AMBIGUOUS_CITIES[lower]
        if country_hint and country_hint in options:
            city_name, state = options[country_hint]
            return (city_name, country_hint, state)
        first_country = next(iter(options))
        city_name, state = options[first_country]
        return (city_name, first_country, state)

    if lower in KNOWN_CITIES:
        known_country, known_state = KNOWN_CITIES[lower]
        # Если город однозначен (не в AMBIGUOUS_CITIES) — верим справочнику больше, чем хинту папки
        if lower not in AMBIGUOUS_CITIES:
            return (_title_case_city(lower), known_country, known_state)
        # Если город неоднозначен (Лондон, Бирмингем) — используем хинт если он совпадает
        if country_hint and country_hint in AMBIGUOUS_CITIES[lower]:
            city_name, state = AMBIGUOUS_CITIES[lower][country_hint]
            return (city_name, country_hint, state)
        return (_title_case_city(lower), known_country, known_state)

    # Эвристика на "мусор": если в названии нет букв или цифр больше, чем букв — это не город (индекс/ID)
    letters_count = sum(1 for c in lower if c.isalpha())
    digits_count = sum(1 for c in lower if c.isdigit())
    
    if letters_count < 2 or digits_count > letters_count:
        return (None, None, None)

    return (_title_case_city(lower), None, None)

def _title_case_city(city_lower: str) -> str:
    """Форматирование названия города в корректный регистр (Title Case)."""
    return city_lower.title()

def normalize_state(
    state: object,
    city: Optional[str] = None,
    country: Optional[str] = None,
) -> Optional[str]:
    """Приведение названия штата или провинции к стандартному сокращению."""
    from .iata_codes import US_STATE_TO_ABBR, US_ABBR_SET

    if state is not None:
        raw = str(state).strip()
        if raw and raw.lower() not in ('nan', 'none', 'null', 'n/a', '', '-'):
            lower = raw.lower()
            if lower in US_STATE_TO_ABBR:
                return US_STATE_TO_ABBR[lower]
            upper = raw.upper()
            if len(upper) == 2 and upper in US_ABBR_SET:
                return upper
            if country and country != 'US':
                return raw.title()

    if city and country in ('US', None):
        city_lower = city.lower()
        if city_lower in KNOWN_CITIES:
            known_country, known_state = KNOWN_CITIES[city_lower]
            if known_country == 'US' and known_state:
                return known_state

    return None

def normalize_nationality(nationality: object) -> Optional[str]:
    """Приведение национальности к стандартному названию (демониму)."""
    if nationality is None:
        return None

    raw = str(nationality).strip()
    if not raw or raw.lower() in ('nan', 'none', 'null', 'n/a', '', '-'):
        return None

    if len(raw) >= 4 and raw[0].isupper():
        return raw

    upper = raw.upper().strip()
    if upper in demonyms:
        return demonyms[upper]

    code = normalize_country(raw)
    if code and code in demonyms:
        return demonyms[code]

    return raw.title()

def extract_file_date(file_path: str) -> Optional[datetime]:
    """Извлечение даты создания данных из названия файла или метаданных файловой системы."""
    filename = os.path.basename(file_path)
    stem = os.path.splitext(filename)[0]

    date_from_name = _extract_date_from_string(stem)
    if date_from_name:
        return date_from_name

    try:
        mtime = os.path.getmtime(file_path)
        return datetime.fromtimestamp(mtime, tz=timezone.utc)
    except:
        return None

def _extract_date_from_string(text: str) -> Optional[datetime]:
    """Поиск паттернов даты в строке (названии файла)."""
    match = _DATE_PATTERN_2.search(text)
    if match:
        try:
            year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
            if 1 <= month <= 12 and 1 <= day <= 31:
                return datetime(year, month, day, tzinfo=timezone.utc)
        except:
            pass

    match = _DATE_PATTERN_1.search(text)
    if match:
        try:
            p1, p2, p3 = int(match.group(1)), int(match.group(2)), int(match.group(3))
            year = 2000 + p3 if p3 < 100 else p3
            if p1 <= 12 and p2 <= 31:
                month, day = p1, p2
            elif p2 <= 12 and p1 <= 31:
                day, month = p1, p2
            else:
                return None
            if 2000 <= year <= 2030 and 1 <= month <= 12 and 1 <= day <= 31:
                return datetime(year, month, day, tzinfo=timezone.utc)
        except:
            pass

    match = _YEAR_RE.search(text)
    if match:
        try:
            year = int(match.group(1))
            if 2000 <= year <= 2030:
                return datetime(year, 1, 1, tzinfo=timezone.utc)
        except:
            pass
    return None

def extract_context_from_path(file_path: str) -> dict:
    """Извлечение географического контекста из структуры папок, в которых лежит файл."""
    result = {'folder_country': None, 'folder_region': None, 'folder_city': None, 'folder_state': None}
    normalized = file_path.replace('\\', '/')
    parts = [p.strip() for p in normalized.split('/') if p.strip()]

    for part in parts:
        part_lower = part.lower()
        if part_lower in FOLDER_TO_CONTEXT:
            ctx = FOLDER_TO_CONTEXT[part_lower]
            if ctx.get('country') and not result['folder_country']: result['folder_country'] = ctx['country']
            if ctx.get('region') and not result['folder_region']: result['folder_region'] = ctx['region']

        if part_lower in FOLDER_CITY_OVERRIDES:
            city_name, country, state = FOLDER_CITY_OVERRIDES[part_lower]
            if not result['folder_city']: result['folder_city'] = city_name
            if not result['folder_country']: result['folder_country'] = country
            if not result['folder_state']: result['folder_state'] = state

    if not result['folder_city']:
        filename_city = _extract_city_from_filename(os.path.basename(file_path))
        if filename_city:
            city_lower = filename_city.lower()
            if city_lower in CITY_TYPOS: city_lower = CITY_TYPOS[city_lower]
            if city_lower in KNOWN_CITIES:
                known_country, known_state = KNOWN_CITIES[city_lower]
                result['folder_city'] = _title_case_city(city_lower)
                if not result['folder_country']: result['folder_country'] = known_country
                if not result['folder_state']: result['folder_state'] = known_state
            else:
                result['folder_city'] = filename_city
    return result

def _extract_city_from_filename(filename: str) -> Optional[str]:
    """Извлечение названия города из имени файла путём очистки от дат и лишних слов."""
    # Отрезаем системный хэш-префикс (например, cf19e57d_), если он есть
    filename = re.sub(r'^[a-fA-F0-9]{8}_', '', filename)
    
    stem = os.path.splitext(filename)[0]
    # Более агрессивная чистка дат и чисел (например "Miami 10,000")
    stem = _DATE_PATTERN_1.sub(' ', stem)
    stem = _DATE_PATTERN_2.sub(' ', stem)
    stem = _YEAR_RE.sub(' ', stem)
    stem = re.sub(r'\d+[,\.]\d+', ' ', stem) # Убираем "10,000"
    stem = re.sub(r'\d+', ' ', stem) # Убираем любые числа
    stem = re.sub(r'[\s\-–—_]+$', '', stem)
    stem = re.sub(r'^[\s\-–—_]+', '', stem).strip()
    if not stem: return None

    split_parts = [p.strip() for p in re.split(r'\s*[-–—]\s+|\s+[-–—]\s*|\s*[-–—](?=[A-Z])', stem) if p.strip()]
    if not split_parts: return None
    city_candidate = split_parts[0].strip(' -–—')
    if not city_candidate: return None

    words = city_candidate.split()
    first_word_upper = words[0].upper().strip('.,;:-()[]{}') if words else ''
    if len(first_word_upper) == 3 and first_word_upper.isalpha() and first_word_upper in IATA_TO_CITY:
        iata_city, _ = IATA_TO_CITY[first_word_upper]
        return iata_city

    result_words = []
    for w in words:
        wl = w.lower().strip('.,;:-()[]{}')
        if not wl or wl in FILENAME_NOISE or wl in FILENAME_LANGUAGES or wl.isdigit() or (len(wl) <= 1 and not wl.isalpha()) or (re.match(r'^[ivxIVX]+$', wl) and len(wl) <= 5):
            continue
        result_words.append(w)

    if not result_words: return None
    candidate_lower = ' '.join(result_words).strip(' -–—').lower()
    if len(candidate_lower) < 2: return None
    if candidate_lower in CITY_TYPOS: candidate_lower = CITY_TYPOS[candidate_lower]
    return _title_case_city(candidate_lower)

