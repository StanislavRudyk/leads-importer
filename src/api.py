import logging
import os
import shutil
import time
import psutil
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Union
import jwt
import pandas as pd
from fastapi import (
    BackgroundTasks, Depends, FastAPI, File,
    HTTPException, Query, UploadFile, status,
)
from fastapi.responses import StreamingResponse
import io
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
from sqlalchemy import select, func, text, update
from .config import settings
from .db import AsyncSessionLocal, Lead, ImportLog, Market, upsert_leads_batch, create_import_log, upsert_market
from .normalizer import (
    normalize_city, normalize_country, normalize_email,
    normalize_nationality, normalize_phone,
)
from .metabase import router as metabase_router
from .enricher import enricher
from .geocoder import geocoder

import pycountry
import pycountry_convert as pc

def get_region_for_country(iso2: str) -> str:
    if not iso2 or len(iso2) != 2:
        return 'Unknown'
    
    iso2_upper = iso2.upper()
    try:
        continent_code = pc.country_alpha2_to_continent_code(iso2_upper)
        continent_names = {
            'AF': 'Africa',
            'AN': 'Antarctica',
            'AS': 'Asia',
            'EU': 'Europe',
            'NA': 'North America',
            'OC': 'Oceania',
            'SA': 'South America'
        }
        return continent_names.get(continent_code, 'Unknown')
    except Exception:
        return 'Unknown'

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

from fastapi import Header

api_key_query = APIKeyHeader(name='API-Key', auto_error=False)
x_api_key_query = APIKeyHeader(name='X-API-Key', auto_error=False)
API_KEY = os.getenv('API_KEY')

async def get_api_key(
    api_key: Optional[str] = Depends(api_key_query),
    x_api_key: Optional[str] = Depends(x_api_key_query)
):
    provided_key = api_key or x_api_key
    if provided_key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Invalid API Key',
        )

class LeadSchema(BaseModel):
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
    return {'status': 'ok', 'version': '2.0.0'}

@app.post('/api/v1/import/upload')
async def upload_leads(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    source_name: str = Query(None),
    api_key: str = Depends(get_api_key),
):
    filename = os.path.basename(file.filename or 'upload.csv')
    from tempfile import gettempdir
    import uuid
    clean_name = f"{uuid.uuid4().hex}_{os.path.basename(file.filename)}"
    tmp_path = os.path.join(gettempdir(), clean_name)
    MAX_SIZE = 100 * 1024 * 1024
    file_size = 0
    try:
        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(0)
    except: pass
    if file_size > MAX_SIZE:
        raise HTTPException(status_code=413, detail=f'File too large. Max 100 MB.')
    try:
        with open(tmp_path, 'wb') as buffer:
            shutil.copyfileobj(file.file, buffer)
        

        import time
        from .cli import run_import
        

        task_id = f"job_{int(time.time() * 1000000)}_{uuid.uuid4().hex[:6]}"
        background_tasks.add_task(_run_and_clean, tmp_path, source_name or filename, task_id)
        return {"status": "processing", "job_id": task_id, "message": f"Processing file in background."}
    except Exception as e:
        logger.error(f'Upload error: {e}')
        if os.path.exists(tmp_path): os.remove(tmp_path)
        raise HTTPException(status_code=500, detail=str(e))

import asyncio
import traceback
IMPORT_SEMAPHORE = asyncio.Semaphore(2) 
_active_tasks = {}

@app.get('/api/v1/import/active')
async def get_active_imports():
    tasks = []
    for tid, tinfo in _active_tasks.items():
        tasks.append({
            "task_id": tid, "filename": tinfo["filename"], "status": tinfo["status"],
            "error": tinfo.get("error"), "queued_at": tinfo["queued_at"],
            "rows_total": tinfo.get("rows_total", 0),
            "rows_inserted": tinfo.get("rows_inserted", 0), "rows_updated": tinfo.get("rows_updated", 0),
            "rows_skipped": tinfo.get("rows_skipped", 0),
            "phase": tinfo.get("phase", ""),
        })
    return {"tasks": tasks, "count": len(tasks)}

def _cleanup_old_tasks():
    global _active_tasks
    if len(_active_tasks) > 100:
        keys = [k for k, v in _active_tasks.items() if v["status"] in ("done", "failed", "empty", "partial", "success")]
        for k in keys[:50]: _active_tasks.pop(k, None)

async def _run_and_clean(file_path: str, source_name: str, task_id: Optional[str] = None):
    global _active_tasks
    _cleanup_old_tasks()
    tid = task_id or f"task_{int(time.time() * 1000)}"
    _active_tasks[tid] = {"filename": os.path.basename(source_name), "status": "waiting", "queued_at": datetime.now(timezone.utc).isoformat(), "rows_total": 0, "rows_inserted": 0, "rows_updated": 0, "rows_skipped": 0, "phase": "queued"}
    async def on_progress(inserted, updated, skipped, phase="saving", total=0):
        if tid in _active_tasks:
            _active_tasks[tid].update({"rows_inserted": inserted, "rows_updated": updated, "rows_skipped": skipped, "phase": phase, "rows_total": total})
    try:
        from .cli import run_import
        async with IMPORT_SEMAPHORE:
            _active_tasks[tid]["status"] = "running"
            _active_tasks[tid]["phase"] = "parsing"
            stats = await run_import(file_path, source_name, on_progress=on_progress)
            if stats:
                _active_tasks[tid].update({
                    "rows_total": stats.get("rows_total", 0),
                    "rows_inserted": stats.get("rows_inserted", 0),
                    "rows_updated": stats.get("rows_updated", 0),
                    "rows_skipped": stats.get("rows_skipped", 0),
                    "status": "done", "phase": "complete"
                })
    except Exception as exc:
        logger.error(f'Background import failed for {source_name}:\n{traceback.format_exc()}')
        _active_tasks[tid].update({"status": "failed", "error": str(exc), "phase": "error"})
    finally:
        if os.path.exists(file_path):
            try: os.remove(file_path)
            except: pass

@app.post('/api/v1/notify/weekly-digest')
async def trigger_weekly_digest(api_key: str = Depends(get_api_key)):
    from .notifier import notifier
    await notifier.send_weekly_digest()
    return {'status': 'success'}

@app.get('/api/v1/dashboard/metrics')
async def get_dashboard_metrics():
    async with AsyncSessionLocal() as session:
        # Leads stats
        q_leads = text("""
            SELECT 
                COUNT(*) as total, 
                COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') as leads_7d 
            FROM leads
        """)
        res_leads = (await session.execute(q_leads)).fetchone()
        
        # Market stats
        q_markets = text("""
            SELECT 
                COALESCE(SUM(spent), 0) as total_spent,
                COALESCE(SUM(impressions), 0) as total_impr,
                COUNT(*) FILTER (WHERE status = 'done') as completed,
                COUNT(*) FILTER (WHERE status != 'done') as upcoming
            FROM markets
        """)
        res_markets = (await session.execute(q_markets)).fetchone()
        
        total_leads = res_leads.total or 0
        total_spent = res_markets.total_spent or 0
        
        return {
            'totalLeads': total_leads, 
            'leads7d': res_leads.leads_7d or 0, 
            'totalSpent': total_spent,
            'avgCpl': round(total_spent / total_leads, 2) if total_leads > 0 else 0,
            'totalImpressions': res_markets.total_impr or 0,
            'completed': res_markets.completed or 0, 
            'upcoming': res_markets.upcoming or 0
        }

@app.get('/api/v1/dashboard/system-status')
async def get_system_status():
    async with AsyncSessionLocal() as session:
        t_files = (await session.execute(select(func.count()).select_from(ImportLog))).scalar() or 0
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        f_today = (await session.execute(select(func.count()).select_from(ImportLog).where(ImportLog.imported_at >= today))).scalar() or 0
        cpu = psutil.cpu_percent(interval=None)
        mem = psutil.virtual_memory()
        dsk = shutil.disk_usage("/")
        return {
            'cpu_percent': cpu, 'ram_percent': mem.percent, 'disk_percent': round((dsk.used / dsk.total) * 100, 1),
            'files_total': t_files, 'files_today': f_today, 'status': 'healthy' if cpu < 90 else 'stressed'
        }

@app.get('/api/v1/dashboard/overview')
async def get_dashboard_overview(search: Optional[str] = Query(None), region: Optional[str] = Query('all'), status: Optional[str] = Query('all')):
    async with AsyncSessionLocal() as session:
        where, params = [], {}
        # Filter for garbage city names 
        where.append("LENGTH(l.city) < 40")
        where.append("l.city NOT LIKE '%Subscribers%'")
        where.append("l.city NOT LIKE '%List%'")
        where.append("l.city NOT LIKE '%.csv%'")
        where.append("l.city IS NOT NULL")
        where.append("l.city != ''")

        if search and len(search.strip()) >= 2:
            s_val = f"%{search.strip().lower()}%"
            where.append("(LOWER(l.city) LIKE :s OR LOWER(l.email) LIKE :s OR LOWER(l.first_name) LIKE :s OR LOWER(l.last_name) LIKE :s)")
            params['s'] = s_val
        if region != 'all':
            valid_iso2s = []
            for c in list(pycountry.countries):
                if hasattr(c, 'alpha_2') and get_region_for_country(c.alpha_2) == region:
                    valid_iso2s.append(c.alpha_2)
            if valid_iso2s:
                where.append("l.country_iso2 IN :countries")
                params['countries'] = tuple(valid_iso2s)
        if status != 'all':
            where.append("m.status = :status")
            params['status'] = status

        where_str = f"WHERE {' AND '.join(where)}" if where else ""
        
        # Get total leads for share calculation
        total_q = text("SELECT COUNT(*) FROM leads")
        total_leads = (await session.execute(total_q)).scalar() or 1

        q = text(f"""
            SELECT 
                TRIM(l.city) as city, 
                l.country_iso2 as country_iso2, 
                l.state as state, 
                l.show_context as show_context,
                MAX(l.show_state) as show_state,
                COUNT(*) as leads, 
                COUNT(*) FILTER (WHERE l.created_at >= NOW() - INTERVAL '7 days') as leads_7d, 
                MAX(m.status) as market_status,
                MAX(m.spent) as spent,
                MAX(m.notes) as notes,
                MAX(m.impressions) as impressions,
                MAX(m.reach) as reach,
                MAX(m.frequency) as frequency,
                MIN(l.collection_start) as start_date,
                MAX(l.collection_end) as end_date
            FROM leads l
            LEFT JOIN markets m ON l.city = m.city AND l.country_iso2 = m.country_iso2
            {where_str} 
            GROUP BY TRIM(l.city), l.country_iso2, l.state, l.show_context
            ORDER BY leads DESC 
            LIMIT 500
        """)
        rows = (await session.execute(q, params)).fetchall()
        
        # Region counts for sidebar
        sidebar_q = text("""
            SELECT country_iso2, COUNT(*) as c 
            FROM leads 
            WHERE city IS NOT NULL AND city != '' AND LENGTH(city) < 40
            GROUP BY country_iso2
        """)
        sidebar_res = (await session.execute(sidebar_q)).fetchall()
        region_counts = {}
        for r in sidebar_res:
            reg = get_region_for_country(r.country_iso2.upper() if r.country_iso2 else '')
            region_counts[reg] = region_counts.get(reg, 0) + r.c

        data = []
        for idx, r in enumerate(rows):
            country = r.country_iso2 or 'XX'
            leads = r.leads or 0
            spent = r.spent or 0
            display_city = r.city or ''

            data.append({
                'id': idx + 1, 
                'city': display_city, 
                'raw_city': r.city or '',
                'country': country, 
                'state': r.state or '',
                'show_context': r.show_context or '',
                'show_state': r.show_state or '',
                'region': get_region_for_country(country.upper()), 
                'leads': leads,
                'leads_7d': r.leads_7d or 0, 
                'status': r.market_status or 'soon',
                'spent': spent,
                'cpl': round(float(spent) / leads, 2) if leads > 0 else 0,
                'impressions': r.impressions or 0,
                'reach': r.reach or 0,
                'frequency': r.frequency or 0,
                'share': round((leads / total_leads) * 100, 2),
                'notes': r.notes or '',
                'start_date': r.start_date.isoformat() if r.start_date else None,
                'end_date': r.end_date.isoformat() if r.end_date else None
            })
        
        return {"items": data, "region_counts": region_counts}

@app.get('/api/v1/dashboard/metrics')
async def get_dashboard_metrics():
    async with AsyncSessionLocal() as session:
        # Grand totals
        q = text("SELECT COUNT(*) as total, COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') as new_7d FROM leads")
        res = (await session.execute(q)).fetchone()
        
        # Region counts
        q_reg = text("SELECT country_iso2, COUNT(*) as c FROM leads GROUP BY country_iso2")
        reg_rows = (await session.execute(q_reg)).fetchall()
        reg_map = {}
        for r in reg_rows:
            reg = get_region_for_country(r.country_iso2.upper() if r.country_iso2 else '')
            reg_map[reg] = reg_map.get(reg, 0) + r.c
            
        # Markets count
        q_m = text("SELECT COUNT(*) FROM markets")
        m_count = (await session.execute(q_m)).scalar() or 0
        
        return {
            'totalLeads': res.total or 0,
            'leads7d': res.new_7d or 0,
            'markets': m_count or 0,
            'regions': reg_map or {}
        }

@app.get('/api/v1/dashboard/us-stats')
async def get_us_state_stats():
    async with AsyncSessionLocal() as session:
        # Aggregates shows/artists per US state
        q = text("""
            SELECT 
                state, 
                COUNT(*) as lead_count,
                COUNT(DISTINCT show_context) as unique_shows,
                string_agg(DISTINCT show_context, ', ') as shows_list,
                MIN(collection_start) as earliest,
                MAX(collection_end) as latest
            FROM leads
            WHERE country_iso2 = 'US' AND state IS NOT NULL AND state != ''
            GROUP BY state
            ORDER BY lead_count DESC
        """)
        res = await session.execute(q)
        return [dict(r._mapping) for r in res.fetchall()]

@app.get('/api/v1/dashboard/sources')
async def get_dashboard_sources():

    async with AsyncSessionLocal() as session:
        q = text("SELECT COALESCE(latest_source, source, 'Unknown') as src, COUNT(id) as total, COUNT(id) FILTER (WHERE is_buyer = TRUE) as buyers, COUNT(id) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') as new_7d FROM leads GROUP BY src ORDER BY total DESC")
        rows = (await session.execute(q)).fetchall()
        data = []
        for idx, r in enumerate(rows):
            conversion = round(r.buyers / r.total * 100, 2) if r.total > 0 else 0
            data.append({
                'id': idx, 'source': r.src or 'Unknown', 'total_leads': r.total,
                'new_leads_7d': r.new_7d or 0, 'buyers': r.buyers, 'conversion': conversion,
            })
        return data

@app.get('/api/v1/dashboard/imports')
async def get_dashboard_imports():
    async with AsyncSessionLocal() as session:
        q = text("SELECT id, filename, source, rows_total, rows_inserted, rows_updated, rows_skipped, status, imported_at FROM import_logs ORDER BY imported_at DESC LIMIT 100")
        rows = (await session.execute(q)).fetchall()
        return [{
            'id': r.id, 'filename': r.filename, 'source': r.source, 'total_rows': r.rows_total,
            'inserted': r.rows_inserted, 'updated': r.rows_updated, 'skipped': r.rows_skipped,
            'status': r.status, 'created_at': r.imported_at.strftime('%Y-%m-%d %H:%M:%S') if r.imported_at else ''
        } for r in rows]

@app.get('/api/v1/leads')
async def get_leads_list(
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=100),
    search: Optional[str] = Query(None),
    sort_by: str = Query('created_at'),
    order: str = Query('desc')
):
    async with AsyncSessionLocal() as session:
        offset = (page - 1) * size
        where = []
        params = {}
        if search:
            s_val = f"%{search.strip().lower()}%"
            where.append("(LOWER(email) LIKE :s OR LOWER(first_name) LIKE :s OR LOWER(last_name) LIKE :s OR LOWER(city) LIKE :s)")
            params['s'] = s_val
        
        where_str = f"WHERE {' AND '.join(where)}" if where else ""
        
        # Validate sort field to prevent SQL injection
        allowed_sorts = {'id', 'email', 'first_name', 'last_name', 'city', 'state', 'created_at', 'updated_at', 'source', 'status'}
        if sort_by not in allowed_sorts: sort_by = 'created_at'
        if order.lower() not in ('asc', 'desc'): order = 'desc'
        
        q = text(f"SELECT * FROM leads {where_str} ORDER BY {sort_by} {order} LIMIT :limit OFFSET :offset")
        params.update({'limit': size, 'offset': offset})
        
        count_q = text(f"SELECT COUNT(*) FROM leads {where_str}")
        
        res = await session.execute(q, params)
        total = (await session.execute(count_q, params)).scalar() or 0
        
        leads = [dict(r._mapping) for r in res.fetchall()]
        return {"items": leads, "total": total, "page": page, "size": size}

@app.patch('/api/v1/leads/{lead_id}')
async def update_lead(lead_id: int, data: dict, api_key: str = Depends(get_api_key)):
    async with AsyncSessionLocal() as session:
        # Filter data to only allow certain fields
        allowed = {'first_name', 'last_name', 'city', 'state', 'status', 'is_buyer', 'phone'}
        update_data = {k: v for k, v in data.items() if k in allowed}
        if not update_data:
            raise HTTPException(status_code=400, detail="No valid fields to update")
        
        update_data['updated_at'] = datetime.now(timezone.utc)
        stmt = update(Lead).where(Lead.id == lead_id).values(**update_data)
        await session.execute(stmt)
        await session.commit()
        return {"status": "success"}

@app.post('/api/v1/leads/enrich')
async def trigger_enrichment(background_tasks: BackgroundTasks, api_key: str = Depends(get_api_key)):
    background_tasks.add_task(enricher.run_enrichment)
    return {"status": "started", "message": "Enrichment job added to background tasks"}

@app.post('/api/v1/markets/geocode')
async def trigger_geocoding(background_tasks: BackgroundTasks, api_key: str = Depends(get_api_key)):
    background_tasks.add_task(geocoder.enrich_market_coordinates)
    return {"status": "started", "message": "Geocoding job started"}

@app.get('/api/v1/dashboard/map-data')
async def get_map_data():
    async with AsyncSessionLocal() as session:
        q = text("""
            SELECT 
                l.city, 
                l.country_iso2, 
                l.state, 
                COUNT(*) as lead_count,
                MIN(l.collection_start) as start_date,
                MAX(l.collection_end) as end_date,
                l.show_context,
                MAX(m.latitude) as latitude,
                MAX(m.longitude) as longitude
            FROM leads l
            LEFT JOIN markets m ON l.city = m.city AND l.country_iso2 = m.country_iso2
            WHERE l.city IS NOT NULL AND l.city != ''
            GROUP BY l.city, l.country_iso2, l.state, l.show_context
            ORDER BY l.show_context, start_date ASC
            LIMIT 1000
        """)
        rows = (await session.execute(q)).fetchall()
        return [dict(r._mapping) for r in rows]

@app.patch('/api/v1/markets')
async def update_market_stats(data: dict, api_key: str = Depends(get_api_key)):
    city = data.get('city')
    country = data.get('country_iso2')
    if not city or not country:
        raise HTTPException(status_code=400, detail="City and country required")
    
    async with AsyncSessionLocal() as session:
        # Filter allowed update fields
        allowed = {'spent', 'status', 'notes', 'impressions', 'reach', 'frequency'}
        update_data = {k: v for k, v in data.items() if k in allowed}
        
        await upsert_market(session, city, country, update_data)
        return {"status": "success"}

@app.delete('/api/v1/markets')
async def delete_market(city: str = Query(...), country_iso2: str = Query(...), api_key: str = Depends(get_api_key)):
    async with AsyncSessionLocal() as session:
        stmt = text("DELETE FROM markets WHERE city = :city AND country_iso2 = :country")
        await session.execute(stmt, {"city": city, "country": country_iso2})
        await session.commit()
        return {"status": "deleted"}

@app.patch('/api/v1/dashboard/bulk-update')
async def bulk_update_dashboard(data: dict, api_key: str = Depends(get_api_key)):
    """
    Updates all leads matching a specific city, country, and optionally current artist/state.
    This is used for 'cleaning' data from the dashboard.
    """
    city = data.get('old_city')
    country = data.get('old_country')
    old_show = data.get('old_show_context')
    
    if not city or not country:
        raise HTTPException(status_code=400, detail="old_city and old_country required")
    
    new_vals = {}
    if 'city' in data: new_vals['city'] = data['city']
    if 'state' in data: new_vals['state'] = data['state']
    if 'show_context' in data: new_vals['show_context'] = data['show_context']
    if 'show_state' in data: new_vals['show_state'] = data['show_state']
    
    if not new_vals:
        raise HTTPException(status_code=400, detail="No new values provided")
    
    async with AsyncSessionLocal() as session:
        where_clause = [text("TRIM(city) = :oc"), Lead.country_iso2 == country]
        w_params = {"oc": city.strip(), "c": country}
        if old_show:
            where_clause.append(Lead.show_context == old_show)
            
        stmt = update(Lead).where(*where_clause).values(**new_vals, updated_at=datetime.now(timezone.utc))
        await session.execute(stmt, w_params)
        await session.commit()
        return {"status": "success", "updated_fields": list(new_vals.keys())}

@app.get('/api/v1/dashboard/export-csv')
async def export_dashboard_csv(api_key: str = Depends(get_api_key)):
    async with AsyncSessionLocal() as session:
        # Re-use the aggregation logic from get_dashboard_overview
        q = text("""
            SELECT 
                TRIM(l.city) as city, 
                MAX(l.country_iso2) as country, 
                MAX(l.state) as state,
                COUNT(*) as leads, 
                COUNT(*) FILTER (WHERE l.created_at >= NOW() - INTERVAL '7 days') as leads_7d, 
                MAX(m.status) as status,
                MAX(m.spent) as spent,
                MAX(m.impressions) as impressions,
                MAX(m.notes) as notes
            FROM leads l
            LEFT JOIN markets m ON l.city = m.city AND l.country_iso2 = m.country_iso2
            WHERE LENGTH(l.city) < 40
            GROUP BY TRIM(l.city)
            ORDER BY leads DESC
        """)
        rows = (await session.execute(q)).fetchall()
        
        data = []
        for r in rows:
            data.append({
                "City": r.city,
                "Country": r.country,
                "State": r.state or "",
                "Region": get_region_for_country(r.country.upper() if r.country else ''),
                "Leads": r.leads,
                "New_7d": r.leads_7d,
                "Spent_USD": r.spent or 0,
                "CPL_USD": round(float(r.spent or 0) / float(r.leads or 1), 2),
                "Impressions": r.impressions or 0,
                "Status": r.status or "soon",
                "Notes": r.notes or ""
            })
            
        df = pd.DataFrame(data)
        stream = io.StringIO()
        df.to_csv(stream, index=False)
        response = StreamingResponse(
            iter([stream.getvalue()]),
            media_type="text/csv"
        )
        response.headers["Content-Disposition"] = f"attachment; filename=leads_export_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        return response
