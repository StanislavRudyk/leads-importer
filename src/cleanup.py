"""Скрипт очистки базы лидов от мусорных/системных записей.

Использование:
    python -m src.cleanup                     # Показать что будет удалено (dry-run)
    python -m src.cleanup --execute           # Выполнить очистку
    python -m src.cleanup --execute --trim    # Очистка + обрезка meta_info
    python -m src.cleanup --execute --dedupe-imports  # + удаление дублей из import_logs
"""
import asyncio
import os
import sys
import argparse
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
if os.path.exists(env_path):
    with open(env_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, v = line.split('=', 1)
                os.environ.setdefault(k.strip(), v.strip())

from src.db import AsyncSessionLocal
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
logger = logging.getLogger('cleanup')

JUNK_CONDITIONS = """
    email LIKE 'noreply@%' OR
    email LIKE 'no-reply@%' OR
    email LIKE 'no.reply@%' OR
    email LIKE 'do-not-reply@%' OR
    email LIKE 'donotreply@%' OR
    email LIKE 'do.not.reply@%' OR
    email LIKE 'mailer-daemon@%' OR
    email LIKE 'mailer.daemon@%' OR
    email LIKE 'postmaster@%' OR
    email LIKE 'hostmaster@%' OR
    email LIKE 'abuse@%' OR
    email LIKE 'bounce@%' OR
    email LIKE 'bounces@%' OR
    email LIKE 'automated@%' OR
    email LIKE 'system@%' OR
    email LIKE 'daemon@%' OR
    email LIKE 'devnull@%' OR
    email LIKE 'null@%' OR
    email LIKE 'nobody@%' OR
    email LIKE '%@example.com' OR
    email LIKE '%@example.org' OR
    email LIKE '%@example.net' OR
    email LIKE '%@test.com' OR
    email LIKE '%@test.org' OR
    email LIKE '%@localhost' OR
    email LIKE '%@invalid.com' OR
    email LIKE '%@mailinator.com' OR
    email LIKE '%@guerrillamail.com' OR
    email LIKE '%@guerrillamail.net' OR
    email LIKE '%@tempmail.com' OR
    email LIKE '%@yopmail.com' OR
    email LIKE '%@trashmail.com' OR
    email LIKE '%@fakeinbox.com' OR
    email LIKE '%@throwaway.email' OR
    email LIKE '%@sharklasers.com' OR
    email LIKE '%@grr.la' OR
    email LIKE '%@maildrop.cc' OR
    email LIKE '%@dispostable.com' OR
    email LIKE '%@temp-mail.org'
"""


async def main():
    ap = argparse.ArgumentParser(description='Очистка базы лидов от мусора')
    ap.add_argument('--execute', action='store_true', help='Выполнить удаление (по умолчанию dry-run)')
    ap.add_argument('--trim', action='store_true', help='Обрезать meta_info.import_history до 10 записей')
    ap.add_argument('--dedupe-imports', action='store_true', help='Удалить дубли из import_logs')
    args = ap.parse_args()

    dry_run = not args.execute

    async with AsyncSessionLocal() as session:
        # ── Общая статистика ──
        total = (await session.execute(text("SELECT COUNT(*) FROM leads"))).scalar()
        logger.info(f"📊 Всего лидов: {total:,}")

        # ── Мусорные email ──
        junk_count = (await session.execute(text(f"SELECT COUNT(*) FROM leads WHERE {JUNK_CONDITIONS}"))).scalar()
        logger.info(f"🗑️  Мусорных email: {junk_count:,}")

        if junk_count > 0:
            junk_sample = await session.execute(
                text(f"SELECT email FROM leads WHERE {JUNK_CONDITIONS} LIMIT 20")
            )
            logger.info("   Примеры:")
            for r in junk_sample.fetchall():
                logger.info(f"     {r[0]}")

        # ── Дубли в import_logs ──
        dupes = await session.execute(text("""
            SELECT filename, COUNT(*) as cnt
            FROM import_logs
            WHERE status IN ('success', 'partial')
            GROUP BY filename
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC LIMIT 15
        """))
        dupe_rows = dupes.fetchall()
        if dupe_rows:
            total_dupes = sum(r[1] - 1 for r in dupe_rows)
            logger.info(f"📋 Файлы, импортированные повторно ({total_dupes} лишних записей):")
            for r in dupe_rows:
                logger.info(f"   {r[0]}: {r[1]} раз")

        # ── Раздутые meta_info ──
        bloated = (await session.execute(text("""
            SELECT COUNT(*) FROM leads
            WHERE jsonb_array_length(COALESCE(meta_info->'import_history', '[]'::jsonb)) > 10
        """))).scalar()
        logger.info(f"📦 Лидов с раздутой import_history (>10 записей): {bloated:,}")

        # ── import_count статистика ──
        stats = await session.execute(text("""
            SELECT
                ROUND(AVG(import_count), 1) as avg_ic,
                MAX(import_count) as max_ic,
                ROUND(AVG(pg_column_size(meta_info))) as avg_meta_bytes,
                MAX(pg_column_size(meta_info)) as max_meta_bytes
            FROM leads
        """))
        s = stats.fetchone()
        if s and s[0] is not None:
            logger.info(f"📈 import_count: avg={s[0]}, max={s[1]}")
            logger.info(f"📈 meta_info: avg={s[2]} bytes, max={s[3]:,} bytes")

        # ── Распределение по источникам (TOP-10) ──
        sources = await session.execute(text("""
            SELECT COALESCE(latest_source, source, 'Unknown') as src, COUNT(*) as cnt
            FROM leads
            GROUP BY src
            ORDER BY cnt DESC
            LIMIT 10
        """))
        logger.info("📡 ТОП-10 источников:")
        for r in sources.fetchall():
            logger.info(f"   {r[0]}: {r[1]:,}")

        if dry_run:
            logger.info("\n⚠️  DRY RUN — изменения НЕ применены. Используйте --execute для выполнения.")
            return

        # ════════════════════════════════════════
        # ВЫПОЛНЕНИЕ ОЧИСТКИ
        # ════════════════════════════════════════
        logger.info("\n🔧 Выполняется очистка...")

        # 1. Удаление мусорных email
        if junk_count > 0:
            result = await session.execute(text(f"DELETE FROM leads WHERE {JUNK_CONDITIONS}"))
            await session.commit()
            logger.info(f"✅ Удалено мусорных email: {result.rowcount:,}")

        # 2. Очистка дублей в import_logs
        if args.dedupe_imports and dupe_rows:
            result = await session.execute(text("""
                DELETE FROM import_logs WHERE id IN (
                    SELECT id FROM (
                        SELECT id, ROW_NUMBER() OVER (PARTITION BY filename ORDER BY imported_at DESC) as rn
                        FROM import_logs
                        WHERE status IN ('success', 'partial')
                    ) sub WHERE rn > 1
                )
            """))
            await session.commit()
            logger.info(f"✅ Удалено дублей из import_logs: {result.rowcount:,}")

        # 3. Обрезка meta_info.import_history до 10 записей
        if args.trim and bloated > 0:
            result = await session.execute(text("""
                UPDATE leads SET meta_info = jsonb_set(
                    meta_info,
                    '{import_history}',
                    (SELECT COALESCE(jsonb_agg(elem), '[]'::jsonb) FROM (
                        SELECT elem FROM jsonb_array_elements(meta_info->'import_history')
                        WITH ORDINALITY AS t(elem, ord)
                        ORDER BY ord DESC LIMIT 10
                    ) sub)
                )
                WHERE jsonb_array_length(COALESCE(meta_info->'import_history', '[]'::jsonb)) > 10
            """))
            await session.commit()
            logger.info(f"✅ Обрезана import_history: {result.rowcount:,} лидов")

        # ── Финальная статистика ──
        new_total = (await session.execute(text("SELECT COUNT(*) FROM leads"))).scalar()
        logger.info(f"\n📊 Лидов после очистки: {new_total:,} (удалено: {total - new_total:,})")


if __name__ == '__main__':
    asyncio.run(main())
