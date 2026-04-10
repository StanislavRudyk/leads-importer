import logging
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Text,
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
    pool_size=400, 
    max_overflow=100,
    pool_timeout=60,
    pool_pre_ping=True
)
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
    BATCH_SIZE = 1000  # Уменьшили для более гладких обновлений в UI

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

            first_name = d.get('first_name')
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

        # Дедупликация внутри пачки + подсчёт потерянных строк
        unique_values = {}
        for v in values:
            unique_values[v['email']] = v
        dedup_lost = len(values) - len(unique_values)
        total_skipped += dedup_lost
        values = list(unique_values.values())
        update_dict = {
            'phone': text(
                "CASE WHEN EXCLUDED.phone IS NOT NULL AND EXCLUDED.phone != '' "
                "THEN EXCLUDED.phone ELSE COALESCE(leads.phone, EXCLUDED.phone) END"
            ),
            'phones': text(
                "COALESCE(leads.phones, '[]'::jsonb) || COALESCE(EXCLUDED.phones, '[]'::jsonb)"
            ),
            'first_name': text("COALESCE(NULLIF(leads.first_name, ''), EXCLUDED.first_name)"),
            'last_name': text("COALESCE(NULLIF(leads.last_name, ''), EXCLUDED.last_name)"),
            'country_iso2': text("COALESCE(NULLIF(leads.country_iso2, ''), EXCLUDED.country_iso2)"),
            'city': text("COALESCE(NULLIF(leads.city, ''), EXCLUDED.city)"),
            'state': text("COALESCE(NULLIF(leads.state, ''), EXCLUDED.state)"),
            'nationality': text("COALESCE(NULLIF(leads.nationality, ''), EXCLUDED.nationality)"),
            'language': text("COALESCE(NULLIF(leads.language, ''), EXCLUDED.language)"),
            'source': text("COALESCE(leads.source, EXCLUDED.source)"),
            'latest_source': text("EXCLUDED.latest_source"),
            'latest_campaign': text("COALESCE(NULLIF(EXCLUDED.latest_campaign, ''), leads.latest_campaign)"),
            'is_buyer': text("leads.is_buyer OR EXCLUDED.is_buyer"),
            'tags': text(
                "COALESCE(leads.tags, '[]'::jsonb) || COALESCE(EXCLUDED.tags, '[]'::jsonb)"
            ),
            'status': text("CASE WHEN leads.status IN ('contacted', 'qualified', 'won', 'lost') THEN leads.status ELSE EXCLUDED.status END"),
            'meta_info': text(
                "leads.meta_info || jsonb_build_object("
                "'import_history', CASE WHEN jsonb_array_length(COALESCE(leads.meta_info->'import_history', '[]'::jsonb)) < 5 "
                "THEN COALESCE(leads.meta_info->'import_history', '[]'::jsonb) || EXCLUDED.meta_info->'import_history' "
                "ELSE EXCLUDED.meta_info->'import_history' END)"
            ),
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
            
            result = await session.execute(upsert_stmt)
            rows = result.fetchall()
            
            batch_inserted = sum(1 for r in rows if r.was_inserted)
            total_inserted += batch_inserted
            total_updated += (len(rows) - batch_inserted)
            
            # Коммитим каждую пачку отдельно для освобождения локов
            await session.commit()
            
        except Exception as e:
            await session.rollback()
            logger.error(f'UPSERT batch failure [{source_name}]: {e}')
            total_skipped += len(values)
            
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


async def check_file_imported(filename: str) -> bool:
    """Проверка, был ли файл уже успешно импортирован."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(ImportLog.id).where(
                ImportLog.filename == filename,
                ImportLog.status.in_(['success', 'partial'])
            ).limit(1)
        )
        return result.scalar() is not None


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
