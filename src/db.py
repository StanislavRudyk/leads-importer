import logging
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Text,
    func, text, Index, select, update, BigInteger,
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

engine = create_async_engine(DATABASE_URL, echo=False, pool_size=5, max_overflow=10)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

class Base(DeclarativeBase):
    """Базовый класс для моделей SQLAlchemy."""
    pass

class Lead(Base):
    """Модель таблицы лидов."""
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
    state = Column(String(10))
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

class ImportLog(Base):
    """Модель логов импорта."""
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
    """Модель получателей еженедельного дайджеста."""
    __tablename__ = 'digest_recipients'

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(255), unique=True, nullable=False)
    full_name = Column(String(255))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

class DashboardPermission(Base):
    """Модель прав доступа к дашбордам."""
    __tablename__ = 'dashboard_permissions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    role = Column(Text, unique=True, nullable=False)
    dashboard_ids = Column(ARRAY(Integer), nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

async def upsert_leads_batch(
    session: AsyncSession,
    leads_data: List[Dict[str, Any]],
    source_name: str,
) -> Tuple[int, int, int]:
    """Массовое добавление или обновление лидов в базе данных."""
    if not leads_data:
        return (0, 0, 0)

    total_inserted = 0
    total_updated = 0
    total_skipped = 0
    BATCH_SIZE = 2500

    for i in range(0, len(leads_data), BATCH_SIZE):
        chunk = leads_data[i:i + BATCH_SIZE]
        values = []
        for d in chunk:
            email = d.get('email')
            if not email:
                total_skipped += 1
                continue
            meta_info = d.get('meta_info', {})
            phones = d.get('phones', [])
            phones = [str(p) for p in phones if p and str(p).strip()]
            tags = d.get('tags', [])
            file_date = d.get('file_created_at')
            if not isinstance(file_date, datetime):
                file_date = None

            raw_phone = d.get('phone')
            phone = str(raw_phone).strip() if raw_phone else None
            values.append({
                'email': email,
                'phone': phone,
                'phones': phones,
                'first_name': d.get('first_name'),
                'last_name': d.get('last_name'),
                'country_iso2': d.get('country_iso2'),
                'nationality': d.get('nationality'),
                'city': d.get('city'),
                'state': d.get('state'),
                'language': d.get('language'),
                'source': source_name,
                'latest_source': d.get('latest_source', source_name),
                'latest_campaign': d.get('latest_campaign'),
                'status': d.get('status', 'new'),
                'is_buyer': bool(d.get('is_buyer', False)),
                'tags': tags,
                'meta_info': meta_info or {},
                'file_created_at': file_date,
                'import_count': d.get('import_count', 1),
                'created_at': datetime.now(timezone.utc),
                'updated_at': datetime.now(timezone.utc),
            })

        if not values:
            continue

        emails_in_chunk = [v['email'] for v in values]
        update_dict = {
            'phone': text(
                "CASE WHEN EXCLUDED.phone IS NOT NULL AND EXCLUDED.phone != '' "
                "AND (leads.phone IS NULL OR leads.phone = '' OR EXCLUDED.file_created_at >= COALESCE(leads.file_created_at, '1970-01-01'::timestamptz)) "
                "THEN EXCLUDED.phone ELSE COALESCE(leads.phone, EXCLUDED.phone) END"
            ),
            'phones': text(
                "(SELECT COALESCE(jsonb_agg(DISTINCT elem), '[]'::jsonb) FROM ( "
                "SELECT jsonb_array_elements(COALESCE(leads.phones, '[]'::jsonb)) AS elem "
                "UNION SELECT jsonb_array_elements(COALESCE(EXCLUDED.phones, '[]'::jsonb)) AS elem "
                ") sub WHERE elem IS NOT NULL AND elem != 'null'::jsonb AND elem != '\"\"'::jsonb)"
            ),
            'first_name': text("CASE WHEN leads.first_name IS NOT NULL AND leads.first_name != '' THEN leads.first_name ELSE EXCLUDED.first_name END"),
            'last_name': text("CASE WHEN leads.last_name IS NOT NULL AND leads.last_name != '' THEN leads.last_name ELSE EXCLUDED.last_name END"),
            'country_iso2': text("CASE WHEN leads.country_iso2 IS NOT NULL AND leads.country_iso2 != '' THEN leads.country_iso2 ELSE EXCLUDED.country_iso2 END"),
            'city': text("CASE WHEN leads.city IS NOT NULL AND leads.city != '' THEN leads.city ELSE EXCLUDED.city END"),
            'state': text("CASE WHEN leads.state IS NOT NULL AND leads.state != '' THEN leads.state ELSE EXCLUDED.state END"),
            'nationality': text("CASE WHEN leads.nationality IS NOT NULL AND leads.nationality != '' THEN leads.nationality ELSE EXCLUDED.nationality END"),
            'language': text("CASE WHEN leads.language IS NOT NULL AND leads.language != '' THEN leads.language ELSE EXCLUDED.language END"),
            'source': text("COALESCE(leads.source, EXCLUDED.source)"),
            'latest_source': text("EXCLUDED.latest_source"),
            'latest_campaign': text("CASE WHEN EXCLUDED.latest_campaign IS NOT NULL AND EXCLUDED.latest_campaign != '' THEN EXCLUDED.latest_campaign ELSE COALESCE(leads.latest_campaign, EXCLUDED.latest_campaign) END"),
            'is_buyer': text("leads.is_buyer OR EXCLUDED.is_buyer"),
            'tags': text(
                "(SELECT COALESCE(jsonb_agg(DISTINCT elem), '[]'::jsonb) FROM ( "
                "SELECT jsonb_array_elements(COALESCE(leads.tags, '[]'::jsonb)) AS elem "
                "UNION SELECT jsonb_array_elements(COALESCE(EXCLUDED.tags, '[]'::jsonb)) AS elem "
                ") sub WHERE elem IS NOT NULL AND elem != 'null'::jsonb)"
            ),
            'status': text("CASE WHEN leads.status IN ('contacted', 'qualified', 'negotiation', 'won', 'lost', 'archived') THEN leads.status ELSE COALESCE(EXCLUDED.status, leads.status) END"),
            'meta_info': text(
                "jsonb_build_object('import_history', COALESCE(leads.meta_info->'import_history', '[]'::jsonb) || COALESCE(EXCLUDED.meta_info->'import_history', '[]'::jsonb), "
                "'raw_phones', (SELECT COALESCE(jsonb_agg(DISTINCT elem), '[]'::jsonb) FROM ( "
                "SELECT jsonb_array_elements(COALESCE(leads.meta_info->'raw_phones', '[]'::jsonb)) AS elem "
                "UNION SELECT jsonb_array_elements(COALESCE(EXCLUDED.meta_info->'raw_phones', '[]'::jsonb)) AS elem "
                ") sub WHERE elem IS NOT NULL AND elem != 'null'::jsonb))"
            ),
            'file_created_at': text("GREATEST(COALESCE(leads.file_created_at, EXCLUDED.file_created_at), COALESCE(EXCLUDED.file_created_at, leads.file_created_at))"),
            'import_count': text("COALESCE(leads.import_count, 0) + 1"),
            'updated_at': text("NOW()"),
        }

        try:
            existing_result = await session.execute(
                select(Lead.email).where(Lead.email.in_(emails_in_chunk))
            )
            existing_emails = {row[0] for row in existing_result.fetchall()}
            stmt = pg_insert(Lead).values(values)

            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=['email'],
                set_=update_dict,
            )
            await session.execute(upsert_stmt)
            await session.commit()
            total_inserted += len([e for e in emails_in_chunk if e not in existing_emails])
            total_updated += len([e for e in emails_in_chunk if e in existing_emails])
        except Exception as e:
            await session.rollback()
            logger.error(f'UPSERT batch failure, falling back to single inserts. Error: {e}')

            total_skipped += len(chunk)
            for val in values:
                try:
                    single_stmt = pg_insert(Lead).values([val]).on_conflict_do_update(index_elements=['email'], set_=update_dict)
                    await session.execute(single_stmt)
                    await session.commit()
                    total_skipped -= 1
                    if val['email'] in existing_emails: total_updated += 1
                    else: total_inserted += 1
                except Exception as single_err:
                    await session.rollback()
                    logger.warning(f"Failed to insert single lead {val.get('email')}: {single_err}")
    return (total_inserted, total_updated, total_skipped)

async def create_import_log(session: AsyncSession,filename, source, total, inserted, updated, skipped, status='success', error_details=None):
    """Создание записи в логе импорта."""
    log = ImportLog(
            filename=filename, 
            source=source, 
            rows_total=total, 
            rows_inserted=inserted, 
            rows_updated=updated, 
            rows_skipped=skipped, 
            status=status, 
            error_details=error_details
        )
    session.add(log)
    await session.commit()
    await session.refresh(log)
    return log.id

async def get_weekly_stats(session: AsyncSession) -> Dict[str, Any]:
    """Получение статистики для еженедельного дайджеста."""
    stats_query = text("""
            SELECT 
                COUNT(*) as total_leads,
                COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days') as new_this_week,
                COUNT(*) FILTER (WHERE updated_at >= NOW() - INTERVAL '7 days' AND created_at < NOW() - INTERVAL '7 days') as updated_this_week,
                COUNT(*) FILTER (WHERE phone IS NOT NULL AND phone != '') as with_phone,
                COUNT(*) FILTER (WHERE first_name IS NOT NULL AND first_name != '') as with_name,
                COUNT(*) FILTER (WHERE country_iso2 IS NOT NULL AND country_iso2 != '') as with_country,
                COUNT(*) FILTER (WHERE city IS NOT NULL AND city != '') as with_city,
                COUNT(*) FILTER (WHERE is_buyer = TRUE) as buyers
            FROM leads
        """)
    stats_result = (await session.execute(stats_query)).fetchone()

    countries_query = text("""
        SELECT country_iso2, COUNT(*) as cnt 
        FROM leads 
        WHERE country_iso2 IS NOT NULL AND country_iso2 != '' 
        GROUP BY country_iso2 
        ORDER BY cnt DESC LIMIT 5
    """)
    countries = await session.execute(countries_query)

    return {
        'total_leads': stats_result.total_leads or 0,
        'has_city': stats_result.with_city or 0,   
        'has_phone': stats_result.with_phone or 0,   
        'new_this_week': stats_result.new_this_week or 0,
        'top_countries': [{'name': r[0] or 'Unknown', 'count': r[1]} for r in countries.fetchall()],
    }


async def get_dashboard_stats(session: AsyncSession) -> Dict[str, Any]:
    """Получение базовой статистики для дашборда."""
    total = (await session.execute(select(func.count()).select_from(Lead))).scalar() or 0
    with_city = (await session.execute(select(func.count()).select_from(Lead).where(Lead.city.isnot(None), Lead.city != ''))).scalar() or 0
    with_phone = (await session.execute(select(func.count()).select_from(Lead).where(Lead.phone.isnot(None), Lead.phone != ''))).scalar() or 0
    countries = await session.execute(text("SELECT country_iso2, COUNT(*) as cnt FROM leads WHERE country_iso2 IS NOT NULL AND country_iso2 != '' GROUP BY country_iso2 ORDER BY cnt DESC LIMIT 5"))
    return {
        'total_leads': total, 'has_city': with_city, 'has_phone': with_phone,
        'top_countries': [{'name': r[0] or 'Unknown', 'count': r[1]} for r in countries.fetchall()],
    }

async def get_dashboard_imports(session: AsyncSession, limit: int = 10) -> List[Dict]:
    """Получение последних записей логов импорта."""
    result = await session.execute(text("SELECT id, filename, source, rows_total, rows_inserted, rows_updated, rows_skipped, status, imported_at FROM import_logs ORDER BY imported_at DESC LIMIT :limit").bindparams(limit=limit))
    return [dict(r._mapping) for r in result.fetchall()]
