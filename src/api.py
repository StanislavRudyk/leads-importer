import logging
import os
import shutil
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Union

import jwt
import pandas as pd
from fastapi import (
    BackgroundTasks, Depends, FastAPI, File,
    HTTPException, Query, UploadFile, status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
from sqlalchemy import select, func, text

from .config import settings
from .db import AsyncSessionLocal, Lead, ImportLog, upsert_leads_batch, create_import_log
from .normalizer import (
    normalize_city, normalize_country, normalize_email,
    normalize_nationality, normalize_phone,

)
from .metabase import router as metabase_router


REGION_MAP = {
    'US': 'USA', 'CA': 'Canada', 'AU': 'Oceania', 'NZ': 'Oceania',
    'GB': 'Europe', 'IE': 'Europe', 'FR': 'Europe', 'DE': 'Europe',
    'IT': 'Europe', 'ES': 'Europe', 'PT': 'Europe', 'NL': 'Europe',
    'BE': 'Europe', 'AT': 'Europe', 'CH': 'Europe', 'SE': 'Europe',
    'NO': 'Europe', 'DK': 'Europe', 'FI': 'Europe', 'PL': 'Europe',
    'CZ': 'Europe', 'HU': 'Europe', 'RO': 'Europe', 'GR': 'Europe',
    'TR': 'Europe', 'IS': 'Europe', 'HR': 'Europe', 'RS': 'Europe',
    'SI': 'Europe', 'BG': 'Europe', 'MK': 'Europe', 'AL': 'Europe',
    'MT': 'Europe', 'LV': 'Europe', 'LT': 'Europe', 'EE': 'Europe',
    'AE': 'Middle East', 'SA': 'Middle East', 'QA': 'Middle East',
    'BH': 'Middle East', 'OM': 'Middle East', 'KW': 'Middle East',
    'IL': 'Middle East', 'JO': 'Middle East', 'LB': 'Middle East',
    'IR': 'Middle East', 'IQ': 'Middle East', 'EG': 'Middle East',
    'JP': 'Asia', 'KR': 'Asia', 'CN': 'Asia', 'HK': 'Asia',
    'TW': 'Asia', 'SG': 'Asia', 'TH': 'Asia', 'MY': 'Asia',
    'ID': 'Asia', 'PH': 'Asia', 'VN': 'Asia', 'IN': 'Asia',
    'PK': 'Asia', 'BD': 'Asia', 'LK': 'Asia', 'MN': 'Asia',
    'ZA': 'Africa', 'NG': 'Africa', 'KE': 'Africa', 'MA': 'Africa',
    'EG': 'Africa', 'GH': 'Africa', 'ET': 'Africa',
    'BR': 'South America', 'AR': 'South America', 'CO': 'South America',
    'PE': 'South America', 'CL': 'South America', 'MX': 'South America',
    'RU': 'Russia/CIS', 'UA': 'Russia/CIS', 'BY': 'Russia/CIS',
    'KZ': 'Russia/CIS', 'GE': 'Russia/CIS', 'AM': 'Russia/CIS',
    'AZ': 'Russia/CIS',
        }

logger = logging.getLogger('leads_importer.api')
if not logger.handlers:
    logger.addHandler(logging.StreamHandler())

app = FastAPI(title='Leads Importer API', version='2.0.0')
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=False,
    allow_methods=['*'],
    allow_headers=['*'],
)
app.include_router(metabase_router)

api_key_header = APIKeyHeader(name='API-Key', auto_error=False)
API_KEY = os.getenv('API_KEY')
if not API_KEY:
    raise RuntimeError("API_KEY is not set in environment variables!")

async def get_api_key(api_key: str = Depends(api_key_header)):
    """Проверка API ключа."""
    if api_key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Invalid API Key',
        )

class LeadSchema(BaseModel):
    """Схема данных для импорта лида через JSON."""
    email: str
    phone: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    country: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    is_buyer: bool = False
    tags: List[str] = []
    metadata: dict = {}

    class Config:
        extra = 'allow'

@app.get('/health')
async def health():
    """Проверка работоспособности API."""
    return {'status': 'ok', 'version': '2.0.0'}

@app.post('/api/v1/import/upload')
async def upload_leads(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    source_name: str = Query(None),
    api_key: str = Depends(get_api_key),
):
    """Загрузка файла CSV/XLSX для импорта лидов."""
    filename = os.path.basename(file.filename or 'upload.csv')
    from tempfile import gettempdir

    clean_name = os.path.basename(file.filename)
    tmp_path = os.path.join(gettempdir(), clean_name)
    MAX_SIZE = 100 * 1024 * 1024

    file_size = 0
    try:
        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(0)
    except Exception:
        pass

    if file_size > MAX_SIZE:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f'File too large ({file_size} bytes). Max limit is 100 MB.',
        )

    try:
        with open(tmp_path, 'wb') as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        # Всегда запускаем в фоне, чтобы не блокировать Event Loop проверкой телефонов и парсингом
        background_tasks.add_task(_run_and_clean, tmp_path, source_name or filename)
        return {'status': 'processing_background', 'filename': filename}
    except Exception as e:
        logger.error(f'Upload error: {e}')
        raise HTTPException(status_code=500, detail=str(e))
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise HTTPException(status_code=500, detail=str(e))

import asyncio

IMPORT_SEMAPHORE = asyncio.Semaphore(50)

async def _run_and_clean(file_path: str, source_name: str):
    """Фоновая задача для обработки файлов."""
    try:
        from .cli import run_import
        async with IMPORT_SEMAPHORE:
            await run_import(file_path, source_name)
    except Exception as exc:
        logger.error(f'Background import failed: {exc}')
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

@app.post('/api/v1/notify/weekly-digest')
async def trigger_weekly_digest(api_key: str = Depends(get_api_key)):
    """Запуск еженедельного дайджеста."""
    from .notifier import notifier
    await notifier.send_weekly_digest()
    return {'status': 'success'}



@app.get('/api/v1/dashboard/metrics')
async def get_dashboard_metrics():
    """Получение ключевых метрик для дашборда."""
    async with AsyncSessionLocal() as session:
        total = (await session.execute(select(func.count()).select_from(Lead))).scalar() or 0
        seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
        leads_7d = (await session.execute(
            select(func.count()).select_from(Lead).where(Lead.created_at >= seven_days_ago)
        )).scalar() or 0
        markets = (await session.execute(
            select(func.count(func.distinct(Lead.city))).where(
                Lead.city.isnot(None), Lead.city != ''
            )
        )).scalar() or 0
        completed = (await session.execute(
            select(func.count()).select_from(Lead).where(Lead.status == 'done')
        )).scalar() or 0
        upcoming = (await session.execute(
            select(func.count()).select_from(Lead).where(Lead.status != 'done')
        )).scalar() or 0
        return {
            'totalLeads': total,
            'leads7d': leads_7d,
            'markets': markets,
            'completed': completed,
            'upcoming': upcoming,
        }

@app.get('/api/v1/dashboard/overview')
async def get_dashboard_overview():
    """Получение сводки данных по городам."""
    async with AsyncSessionLocal() as session:
        query = text("""
            SELECT
                COALESCE(NULLIF(TRIM(city), ''), 'Unknown') as city,
                MAX(country_iso2) as country_iso2,
                MAX(state) as state,
                COUNT(id) as leads,
                COUNT(id) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') as leads_7d,
                MAX(status) as max_status
            FROM leads
            GROUP BY COALESCE(NULLIF(TRIM(city), ''), 'Unknown')
            ORDER BY leads DESC
        """)
        result = await session.execute(query)
        rows = result.fetchall()

        data = []
        for idx, r in enumerate(rows):
            city = r.city or 'Unknown'
            country = r.country_iso2 or 'XX'
            state = r.state or ''
            region = REGION_MAP.get(country.upper(), 'Other')
            status_val = 'done' if r.max_status == 'done' else 'upcoming'
            data.append({
                'id': idx,
                'city': city,
                'country': country,
                'state': state,
                'region': region,
                'leads': r.leads,
                'leads_7d': r.leads_7d or 0,
                'status': status_val,
            })
        return data

@app.get('/api/v1/dashboard/sources')
async def get_dashboard_sources():
    """Получение статистики по источникам лидов."""
    async with AsyncSessionLocal() as session:
        query = text("""
            SELECT
                COALESCE(latest_source, source, 'Unknown') as source_name,
                COUNT(id) as total_leads,
                COUNT(id) FILTER (WHERE is_buyer = TRUE) as total_buyers,
                COUNT(id) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') as new_leads_7d
            FROM leads
            GROUP BY COALESCE(latest_source, source, 'Unknown')
            ORDER BY total_leads DESC
        """)
        result = await session.execute(query)
        rows = result.fetchall()

        data = []
        for idx, r in enumerate(rows):
            source = r.source_name or 'Unknown/Organic'
            buyers = r.total_buyers or 0
            leads = r.total_leads or 0
            conversion = round(buyers / leads * 100, 2) if leads > 0 else 0
            data.append({
                'id': idx,
                'source': source,
                'total_leads': leads,
                'new_leads_7d': r.new_leads_7d or 0,
                'buyers': buyers,
                'conversion': conversion,
            })
        return data

@app.get('/api/v1/dashboard/imports')
async def get_dashboard_imports():
    """Получение последних логов импорта."""
    async with AsyncSessionLocal() as session:
        query = text("""
            SELECT
                id, filename, source, rows_total,
                rows_inserted, rows_updated, rows_skipped,
                status, imported_at
            FROM import_logs
            ORDER BY imported_at DESC
            LIMIT 100
        """)
        result = await session.execute(query)
        rows = result.fetchall()

        data = []
        for r in rows:
            data.append({
                'id': r.id,
                'filename': r.filename,
                'source': r.source,
                'total_rows': r.rows_total,
                'inserted': r.rows_inserted,
                'updated': r.rows_updated,
                'skipped': r.rows_skipped,
                'status': r.status,
                'created_at': r.imported_at.strftime('%Y-%m-%d %H:%M:%S') if r.imported_at else '',
            })
        return data
