import logging
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Text, Float,
    func, text, Index, select, update, BigInteger, literal_column,
)
from sqlalchemy.dialects.postgresql import JSONB, ARRAY, insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
import os

logger = logging.getLogger('leads_importer.db')

DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql+asyncpg://postgres:postgres@localhost:5432/leads',
)

engine = create_async_engine(
    DATABASE_URL, 
    echo=False, 
    pool_size=30, 
    max_overflow=15,
    pool_timeout=60,
    pool_pre_ping=False
)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

class Lead(Base):
    __tablename__ = 'leads'

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    phone = Column(String(100))
    phones = Column(JSONB, default=list)
    first_name = Column(String(255))
    last_name = Column(String(255))
    country_iso2 = Column(String(10))
    nationality = Column(String(255))
    city = Column(String(255))
    state = Column(String(100))
    show_context = Column(String(255))
    collection_start = Column(DateTime(timezone=True))
    collection_end = Column(DateTime(timezone=True))
    language = Column(String(100))
    source = Column(String(255))
    latest_source = Column(String(255))
    latest_campaign = Column(String(255))
    status = Column(String(50), default='new')
    is_buyer = Column(Boolean, default=False)
    tags = Column(JSONB, default=list)
    meta_info = Column(JSONB, default=dict)
    brevo_id = Column(String(100))
    file_created_at = Column(DateTime(timezone=True))
    show_state = Column(String(100)) # Status or region
    import_count = Column(Integer, default=1)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index('ix_leads_country', 'country_iso2'),
        Index('ix_leads_city', 'city'),
        Index('ix_leads_source', 'source'),
        Index('ix_leads_status', 'status'),
        Index('ix_leads_created_at', 'created_at'),
    )

class Market(Base):
    __tablename__ = 'markets'

    city = Column(String(255), primary_key=True)
    country_iso2 = Column(String(10), primary_key=True)
    region = Column(String(100))
    spent = Column(BigInteger, default=0)
    cpl = Column(BigInteger, default=0)
    impressions = Column(BigInteger, default=0)
    reach = Column(BigInteger, default=0)
    frequency = Column(BigInteger, default=0)
    status = Column(String(50), default='soon')  # Show state: done, soon, active
    notes = Column(Text)
    launch_date = Column(DateTime(timezone=True))
    latitude = Column(Float)
    longitude = Column(Float)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class ImportLog(Base):
    __tablename__ = 'import_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(Text, nullable=False)
    source = Column(String(255))
    rows_total = Column(Integer, default=0)
    rows_inserted = Column(Integer, default=0)
    rows_updated = Column(Integer, default=0)
    rows_skipped = Column(Integer, default=0)
    status = Column(String(50), default='success')
    error_details = Column(JSONB)
    imported_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class DigestRecipient(Base):
    __tablename__ = 'digest_recipients'

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), unique=True, nullable=False)
    full_name = Column(String(255))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class DashboardPermission(Base):
    __tablename__ = 'dashboard_permissions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    role = Column(Text, unique=True, nullable=False)
    dashboard_ids = Column(ARRAY(Integer), nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(Text, unique=True, nullable=False)
    password_hash = Column(Text, nullable=False)
    role = Column(Text, nullable=False, default='viewer')
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

async def upsert_market(session: AsyncSession, city: str, country_iso2: str, data: Dict):
    stmt = pg_insert(Market).values({
        'city': city,
        'country_iso2': country_iso2,
        **data,
        'updated_at': datetime.now(timezone.utc)
    })
    update_dict = {k: v for k, v in data.items() if k not in ('city', 'country_iso2')}
    update_dict['updated_at'] = datetime.now(timezone.utc)
    
    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=['city', 'country_iso2'],
        set_=update_dict
    )
    await session.execute(upsert_stmt)
    await session.commit()

async def upsert_leads_batch(session: AsyncSession, leads_data: List[Dict], source_name: str, batch_size: int = 1000):
    total_inserted = 0
    total_updated = 0
    total_skipped = 0
    
    def is_valid_name(n):
        return n and len(str(n).strip()) >= 2 and any(c.isalpha() for c in str(n))

    filtered_leads = []
    for d in leads_data:
        has_name = is_valid_name(d.get('first_name')) or is_valid_name(d.get('last_name'))
        has_phone = bool(str(d.get('phone') or '').strip() or d.get('phones'))
        has_geo = bool(d.get('city') or d.get('country_iso2'))
        if has_name or has_phone or has_geo:
            filtered_leads.append(d)
        else:
            total_skipped += 1

    leads_data = filtered_leads
    if not leads_data:
        return total_inserted, total_updated, total_skipped

    for i in range(0, len(leads_data), batch_size):
        chunk = leads_data[i:i + batch_size]
        values = []
        for d in chunk:
            email = d.get('email')
            if not email:
                total_skipped += 1
                continue
            
            phones = d.get('phones', [])
            if not isinstance(phones, (list, tuple)): phones = []
            
            meta_info = d.get('meta_info', {})
            if not isinstance(meta_info, dict): meta_info = {}
            
            tags = d.get('tags', [])
            if not isinstance(tags, (list, tuple)): tags = []

            values.append({
                'email': email,
                'phone': d.get('phone'),
                'phones': phones,
                'first_name': d.get('first_name'),
                'last_name': d.get('last_name'),
                'country_iso2': d.get('country_iso2'),
                'nationality': d.get('nationality'),
                'city': d.get('city'),
                'state': d.get('state'),
                'show_context': d.get('show_context'),
                'collection_start': d.get('collection_start'),
                'collection_end': d.get('collection_end'),
                'language': d.get('language'),
                'source': source_name,
                'latest_source': d.get('latest_source', source_name),
                'latest_campaign': d.get('latest_campaign'),
                'status': d.get('status', 'new'),
                'is_buyer': bool(d.get('is_buyer', False)),
                'tags': tags,
                'meta_info': {
                    'import_history': [{
                        'file': source_name,
                        'imported_at': datetime.now(timezone.utc).isoformat(),
                        'raw_row': {str(k)[:50]: str(v)[:200] for k, v in list(d.items())[:10]}
                    }]
                },
                'brevo_id': d.get('brevo_id'),
                'file_created_at': d.get('file_created_at'),
                'import_count': d.get('import_count', 1),
                'created_at': datetime.now(timezone.utc),
                'updated_at': datetime.now(timezone.utc),
            })

        if not values:
            continue

        unique_values = {}
        for v in values:
            unique_values[v['email']] = v
        total_skipped += (len(values) - len(unique_values))
        values = sorted(list(unique_values.values()), key=lambda x: x['email'])
        
        update_dict = {
            'phone': text("CASE WHEN EXCLUDED.phone IS NOT NULL AND EXCLUDED.phone != '' THEN EXCLUDED.phone ELSE COALESCE(leads.phone, EXCLUDED.phone) END"),
            'phones': text("(SELECT jsonb_agg(DISTINCT x) FROM jsonb_array_elements(COALESCE(NULLIF(leads.phones, 'null'::jsonb), '[]'::jsonb) || COALESCE(NULLIF(EXCLUDED.phones, 'null'::jsonb), '[]'::jsonb)) x)"),
            'first_name': text("COALESCE(NULLIF(leads.first_name, ''), EXCLUDED.first_name)"),
            'last_name': text("COALESCE(NULLIF(leads.last_name, ''), EXCLUDED.last_name)"),
            'country_iso2': text("COALESCE(NULLIF(leads.country_iso2, ''), EXCLUDED.country_iso2)"),
            'city': text("COALESCE(NULLIF(leads.city, ''), EXCLUDED.city)"),
            'state': text("COALESCE(NULLIF(leads.state, ''), EXCLUDED.state)"),
            'show_context': text("COALESCE(leads.show_context, EXCLUDED.show_context)"),
            'collection_start': text("LEAST(COALESCE(leads.collection_start, EXCLUDED.collection_start), EXCLUDED.collection_start)"),
            'collection_end': text("GREATEST(COALESCE(leads.collection_end, EXCLUDED.collection_end), EXCLUDED.collection_end)"),
            'nationality': text("COALESCE(NULLIF(leads.nationality, ''), EXCLUDED.nationality)"),
            'language': text("COALESCE(NULLIF(leads.language, ''), EXCLUDED.language)"),
            'source': text("COALESCE(leads.source, EXCLUDED.source)"),
            'latest_source': text("EXCLUDED.latest_source"),
            'latest_campaign': text("COALESCE(NULLIF(EXCLUDED.latest_campaign, ''), leads.latest_campaign)"),
            'is_buyer': text("leads.is_buyer OR EXCLUDED.is_buyer"),
            'tags': text("(SELECT jsonb_agg(DISTINCT x) FROM jsonb_array_elements(COALESCE(NULLIF(leads.tags, 'null'::jsonb), '[]'::jsonb) || COALESCE(NULLIF(EXCLUDED.tags, 'null'::jsonb), '[]'::jsonb)) x)"),
            'status': text("CASE WHEN leads.status IN ('contacted', 'qualified', 'won', 'lost') THEN leads.status ELSE EXCLUDED.status END"),
            'meta_info': text("leads.meta_info || jsonb_build_object('import_history', (SELECT jsonb_agg(x) FROM (SELECT DISTINCT jsonb_array_elements(COALESCE(leads.meta_info->'import_history', '[]'::jsonb) || COALESCE(EXCLUDED.meta_info->'import_history', '[]'::jsonb))) t(x)), 'raw_phones', (SELECT COALESCE(jsonb_agg(x), '[]'::jsonb) FROM (SELECT DISTINCT jsonb_array_elements(COALESCE(leads.meta_info->'raw_phones', '[]'::jsonb) || COALESCE(EXCLUDED.meta_info->'raw_phones', '[]'::jsonb))) t(x)))"),
            'brevo_id': text("COALESCE(leads.brevo_id, EXCLUDED.brevo_id)"),
            'file_created_at': text("GREATEST(COALESCE(leads.file_created_at, '1970-01-01'::timestamp), COALESCE(EXCLUDED.file_created_at, '1970-01-01'::timestamp))"),
            'import_count': Lead.import_count + 1,
            'updated_at': text("NOW()"),
        }

        try:
            stmt = pg_insert(Lead).values(values)
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=['email'],
                set_=update_dict,
            ).returning(literal_column("(xmax = 0)").label("was_inserted"))
            res = await session.execute(upsert_stmt)
            rows = res.fetchall()
            batch_inserted = sum(1 for r in rows if r.was_inserted)
            total_inserted += batch_inserted
            total_updated += (len(rows) - batch_inserted)
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.error(f'UPSERT batch failure [{source_name}]: {e}')
            total_skipped += len(values)
    return (total_inserted, total_updated, total_skipped)

async def create_import_log(session: AsyncSession, filename, source, total, inserted, updated, skipped, status='success', error_details=None):
    log = ImportLog(
            filename=filename, source=source, rows_total=total, 
            rows_inserted=inserted, rows_updated=updated, rows_skipped=skipped, 
            status=status, error_details=error_details
        )
    session.add(log)
    await session.commit()
    await session.refresh(log)
    return log.id

async def check_file_imported(filename: str) -> bool:
    async with AsyncSessionLocal() as session:
        res = await session.execute(select(ImportLog.id).where(ImportLog.filename == filename, ImportLog.status.in_(['success', 'partial'])).limit(1))
        return res.scalar() is not None

async def get_weekly_stats(session: AsyncSession) -> Dict[str, Any]:
    stats_q = text("""
            SELECT 
                COUNT(*) as total_leads,
                COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') as new_this_week,
                COUNT(*) FILTER (WHERE updated_at >= NOW() - INTERVAL '7 days' AND created_at < NOW() - INTERVAL '7 days') as updated_this_week,
                COUNT(*) FILTER (WHERE phone IS NOT NULL AND phone != '') as with_phone,
                COUNT(*) FILTER (WHERE first_name IS NOT NULL AND first_name != '') as with_name,
                COUNT(*) FILTER (WHERE country_iso2 IS NOT NULL AND country_iso2 != '') as with_country,
                COUNT(*) FILTER (WHERE is_buyer = TRUE) as buyers
            FROM leads
        """)
    res = (await session.execute(stats_q)).fetchone()
    total = res.total_leads or 1 
    countries_q = text("SELECT country_iso2, COUNT(*) as cnt FROM leads WHERE country_iso2 IS NOT NULL AND country_iso2 != '' GROUP BY country_iso2 ORDER BY cnt DESC LIMIT 5")
    countries_res = await session.execute(countries_q)
    top_countries = [{'name': r[0], 'count': r[1]} for r in countries_res.fetchall()]
    sources_q = text("SELECT COALESCE(latest_source, source, 'Unknown') as src, COUNT(*) as cnt FROM leads WHERE created_at >= NOW() - INTERVAL '7 days' OR updated_at >= NOW() - INTERVAL '7 days' GROUP BY src ORDER BY cnt DESC LIMIT 5")
    sources_res = await session.execute(sources_q)
    top_sources = [{'name': r[0], 'count': r[1]} for r in sources_res.fetchall()]
    imports_q = text("SELECT COUNT(*) as processed, COUNT(*) FILTER (WHERE status = 'success') as success, COUNT(*) FILTER (WHERE status IN ('failed', 'error')) as failed FROM import_logs WHERE imported_at >= NOW() - INTERVAL '7 days'")
    imports_res = (await session.execute(imports_q)).fetchone()
    return {
        'total_leads': res.total_leads or 0,
        'new_this_week': res.new_this_week or 0,
        'updated_this_week': res.updated_this_week or 0,
        'data_quality': {
            'phone_count': res.with_phone or 0,
            'phone_pct': round((res.with_phone or 0) / total * 100, 1),
            'name_count': res.with_name or 0,
            'name_pct': round((res.with_name or 0) / total * 100, 1),
            'country_count': res.with_country or 0,
            'country_pct': round((res.with_country or 0) / total * 100, 1),
            'buyer_count': res.buyers or 0,
            'buyer_pct': round((res.buyers or 0) / total * 100, 1),
        },
        'top_countries': top_countries, 'top_sources': top_sources,
        'imports': {'processed': imports_res.processed or 0, 'success': imports_res.success or 0, 'failed': imports_res.failed or 0}
    }

async def get_dashboard_stats(session: AsyncSession) -> Dict[str, Any]:
    total = (await session.execute(select(func.count()).select_from(Lead))).scalar() or 0
    with_city = (await session.execute(select(func.count()).select_from(Lead).where(Lead.city.isnot(None), Lead.city != ''))).scalar() or 0
    with_phone = (await session.execute(select(func.count()).select_from(Lead).where(Lead.phone.isnot(None), Lead.phone != ''))).scalar() or 0
    countries = await session.execute(text("SELECT country_iso2, COUNT(*) as cnt FROM leads WHERE country_iso2 IS NOT NULL AND country_iso2 != '' GROUP BY country_iso2 ORDER BY cnt DESC LIMIT 5"))
    return {
        'total_leads': total, 'has_city': with_city, 'has_phone': with_phone,
        'top_countries': [{'name': r[0] or 'Unknown', 'count': r[1]} for r in countries.fetchall()],
    }

async def get_dashboard_imports(session: AsyncSession, limit: int = 10) -> List[Dict]:
    res = await session.execute(text("SELECT id, filename, source, rows_total, rows_inserted, rows_updated, rows_skipped, status, imported_at FROM import_logs ORDER BY imported_at DESC LIMIT :limit").bindparams(limit=limit))
    return [dict(r._mapping) for r in res.fetchall()]
