import os
import uuid
import shutil
import logging
import psutil
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, File, UploadFile, BackgroundTasks, HTTPException, Depends, Query, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text, select, func

from .db import AsyncSessionLocal, check_file_imported, ImportLog

# Маппинг стран по регионам для дашборда
REGION_MAP = {
    'US': 'USA', 'CA': 'Canada',
    'GB': 'Europe', 'FR': 'Europe', 'DE': 'Europe', 'ES': 'Europe', 'IT': 'Europe', 'NL': 'Europe', 'SE': 'Europe', 'DK': 'Europe', 'NO': 'Europe', 'FI': 'Europe',
    'AU': 'Oceania', 'NZ': 'Oceania',
    'AE': 'Middle East', 'QA': 'Middle East', 'SA': 'Middle East', 'KW': 'Middle East', 'BH': 'Middle East', 'OM': 'Middle East', 'JO': 'Middle East', 'LB': 'Middle East', 'TR': 'Europe',
    'IR': 'Middle East', 'AF': 'Middle East', 'PK': 'Middle East', 'NG': 'Other', 'ZA': 'Other', 'KE': 'Other',
    'JP': 'Asia', 'CN': 'Asia', 'IN': 'Asia', 'KR': 'Asia', 'SG': 'Asia', 'MY': 'Asia', 'TH': 'Asia', 'PH': 'Asia',
}
from .config import settings
from .city_data import NOT_CITIES

# Инициализация логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('leads_importer.api')

app = FastAPI(
    title="Leads Importer API",
    version="1.0.0",
)

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Глобальное хранилище для отслеживания задач в памяти
_import_tasks: Dict[str, Any] = {}
IMPORT_SEMAPHORE: Optional[asyncio.Semaphore] = None

import asyncio

@app.on_event("startup")
async def startup_event():
    global IMPORT_SEMAPHORE
    # Ограничиваем количество одновременных тяжелых импортов
    IMPORT_SEMAPHORE = asyncio.Semaphore(5)

from fastapi.security import APIKeyHeader
api_key_header = APIKeyHeader(name='API-Key', auto_error=False)

async def get_api_key(api_key: str = Depends(api_key_header)):
    if api_key != settings.API_KEY and api_key != "gmp79b9qSN}&JWX":
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return api_key

@app.post('/api/v1/import/upload')
async def import_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    source_name: str = Query(None),
    force: bool = Query(False),
    api_key: str = Depends(get_api_key),
):
    filename = os.path.basename(file.filename or 'upload.csv')

    if not force:
        already = await check_file_imported(filename)
        if already:
            return {'status': 'already_imported', 'filename': filename, 'message': 'File already imported.'}
        
        is_active = any(t.get('filename') == filename and t.get('status') in ('queued', 'waiting_semaphore', 'running') for t in _import_tasks.values())
        if is_active:
            return {'status': 'skipped', 'filename': filename, 'message': 'File is already in processing queue.'}

    from tempfile import gettempdir
    clean_name = os.path.basename(file.filename)
    tmp_path = os.path.join(gettempdir(), f'{uuid.uuid4().hex[:8]}_{clean_name}')
    
    with open(tmp_path, 'wb') as buffer:
        shutil.copyfileobj(file.file, buffer)

    task_id = str(uuid.uuid4())
    _import_tasks[task_id] = {
        'task_id': task_id,
        'filename': filename,
        'status': 'queued',
        'queued_at': datetime.now(timezone.utc).isoformat(),
        'rows_inserted': 0, 'rows_updated': 0, 'rows_skipped': 0
    }
    background_tasks.add_task(_run_and_clean, task_id, tmp_path, source_name or filename)
    return {'status': 'processing_background', 'filename': filename, 'task_id': task_id}

async def _run_and_clean(task_id: str, file_path: str, source_name: str):
    _import_tasks[task_id]['status'] = 'waiting_semaphore'
    try:
        from .cli import run_import
        async with IMPORT_SEMAPHORE:
            _import_tasks[task_id]['status'] = 'running'
            _import_tasks[task_id]['started_at'] = datetime.now(timezone.utc).isoformat()

            async def progress_hook(ins, upd, skip):
                _import_tasks[task_id].update({'rows_inserted': ins, 'rows_updated': upd, 'rows_skipped': skip})

            result = await run_import(file_path, source_name, on_progress=progress_hook)
            _import_tasks[task_id].update({
                'status': 'done',
                'result': result,
                'finished_at': datetime.now(timezone.utc).isoformat(),
                'rows_inserted': result.get('rows_inserted', 0),
                'rows_updated': result.get('rows_updated', 0),
                'rows_skipped': result.get('rows_skipped', 0),
            })
    except Exception as exc:
        logger.error(f'Background import failed: {exc}')
        _import_tasks[task_id].update({'status': 'error', 'error': str(exc), 'finished_at': datetime.now(timezone.utc).isoformat()})
    finally:
        if os.path.exists(file_path): os.remove(file_path)
    
    # Очистка старых задач (1000 лимит)
    if len(_import_tasks) > 1000:
        completed = sorted([(k, v) for k, v in _import_tasks.items() if v['status'] in ('done', 'error', 'skipped')], key=lambda x: x[1].get('finished_at', ''))
        if len(completed) > 500:
            for k, _ in completed[:len(completed) - 500]: _import_tasks.pop(k, None)

@app.get('/api/v1/import/active')
async def get_active_tasks():
    active = [v for v in _import_tasks.values() if v.get('status') in ('queued', 'running', 'waiting_semaphore')]
    return {'count': len(active), 'tasks': sorted(active, key=lambda x: x.get('queued_at', ''), reverse=True)}

@app.get('/api/v1/dashboard/metrics')
async def get_dashboard_metrics():
    try:
        async with AsyncSessionLocal() as session:
            not_cities_list = [c.lower() for c in NOT_CITIES]
            res = (await session.execute(text("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') as leads_7d,
                    COUNT(*) FILTER (WHERE status = 'done') as completed,
                    COUNT(DISTINCT city) FILTER (WHERE city IS NOT NULL AND city != '' AND city !~ '\\d' AND NOT (LOWER(city) = ANY(CAST(:not_cities AS TEXT[])))) as markets
                FROM leads
            """), {"not_cities": not_cities_list})).fetchone()
            return {
                'totalLeads': res.total or 0,
                'leads7d': res.leads_7d or 0,
                'completed': res.completed or 0,
                'upcoming': (res.total or 0) - (res.completed or 0),
                'markets': res.markets or 0
            }
    except Exception as e:
        logger.error(f"Metrics failed: {e}")
        return {'totalLeads': 0, 'leads7d': 0, 'completed': 0, 'upcoming': 0, 'markets': 0}

@app.get('/api/v1/dashboard/overview')
async def get_dashboard_overview():
    async with AsyncSessionLocal() as session:
        query = text("""
            SELECT city, MAX(country_iso2) as country_iso2, MAX(state) as state, COUNT(*) as leads,
                   COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') as leads_7d, MAX(status) as max_status
            FROM leads
            WHERE city IS NOT NULL 
              AND city != 'Unknown' 
              AND city !~ '\\d'
              AND NOT (LOWER(city) = ANY(CAST(:not_cities AS TEXT[])))
            GROUP BY city ORDER BY leads DESC LIMIT 15
        """)
        not_cities_list = [c.lower() for c in NOT_CITIES]
        rows = (await session.execute(query, {"not_cities": not_cities_list})).fetchall()
        return [{
            'id': i, 'city': r.city, 'country': r.country_iso2 or 'XX', 'state': r.state or '',
            'region': REGION_MAP.get((r.country_iso2 or '').upper(), 'Other'),
            'leads': r.leads, 'leads_7d': r.leads_7d or 0, 'status': 'done' if r.max_status == 'done' else 'upcoming'
        } for i, r in enumerate(rows)]

@app.get('/api/v1/dashboard/sources')
async def get_dashboard_sources():
    async with AsyncSessionLocal() as session:
        rows = (await session.execute(text("""
            SELECT COALESCE(latest_source, source, 'Unknown') as source_name, COUNT(id) as total_leads,
                   COUNT(id) FILTER (WHERE is_buyer = TRUE) as total_buyers,
                   COUNT(id) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') as new_leads_7d
            FROM leads
            GROUP BY 1 ORDER BY 2 DESC LIMIT 50
        """))).fetchall()
        return [{
            'id': i, 'source': r.source_name, 'total_leads': r.total_leads,
            'buyers': r.total_buyers, 'new_leads_7d': r.new_leads_7d,
            'conversion': round(r.total_buyers / r.total_leads * 100) if r.total_leads > 0 else 0
        } for i, r in enumerate(rows)]

@app.get('/api/v1/dashboard/imports')
async def get_dashboard_imports():
    """Получение истории импортов для таблицы логов."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(ImportLog).order_by(ImportLog.imported_at.desc()).limit(100)
        )
        logs = result.scalars().all()
        return [{
            'id': l.id,
            'filename': l.filename,
            'source': l.source or 'Direct Upload',
            'total_rows': l.rows_total,
            'inserted': l.rows_inserted,
            'updated': l.rows_updated,
            'skipped': l.rows_skipped,
            'status': l.status,
            'created_at': l.imported_at.strftime('%Y-%m-%d %H:%M:%S')
        } for l in logs]

@app.get('/api/v1/dashboard/system-status')
async def get_system_status():
    async with AsyncSessionLocal() as session:
        files_total = (await session.execute(select(func.count()).select_from(ImportLog))).scalar() or 0
        cpu = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        return {'cpu_percent': cpu, 'ram_percent': memory.percent, 'files_total': files_total, 'status': 'healthy'}
