import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, func, text

from src.db import AsyncSessionLocal, Lead, ImportLog
from src.notifier import notifier

logger = logging.getLogger('leads_importer.reports')

async def generate_weekly_report() -> None:
    """Generate and dispatch the weekly analytical report via Telegram."""
    one_week_ago = datetime.now(timezone.utc) - timedelta(days=7)

    async with AsyncSessionLocal() as session:
        total_leads = (await session.execute(select(func.count(Lead.id)))).scalar() or 0
        count_new = (await session.execute(select(func.count(Lead.id)).where(Lead.created_at >= one_week_ago))).scalar() or 0
        
        sources_result = await session.execute(
            select(func.coalesce(Lead.latest_source, Lead.source, 'Unknown'), func.count(Lead.id))
            .where(Lead.created_at >= one_week_ago)
            .group_by(func.coalesce(Lead.latest_source, Lead.source, 'Unknown'))
            .order_by(func.count(Lead.id).desc())
            .limit(10)
        )
        sources_data = sources_result.all()

        count_imports = (await session.execute(select(func.count(ImportLog.id)).where(ImportLog.imported_at >= one_week_ago))).scalar() or 0
        
        phone_count = (await session.execute(select(func.count(Lead.id)).where(Lead.phone.isnot(None), Lead.phone != ''))).scalar() or 0
        city_count = (await session.execute(select(func.count(Lead.id)).where(Lead.city.isnot(None), Lead.city != ''))).scalar() or 0
        country_count = (await session.execute(select(func.count(Lead.id)).where(Lead.country_iso2.isnot(None), Lead.country_iso2 != ''))).scalar() or 0
        buyer_count = (await session.execute(select(func.count(Lead.id)).where(Lead.is_buyer.is_(True)))).scalar() or 0

        report = [
            f'📊 <b>Weekly Leads Report</b>',
            f'Period: {one_week_ago.strftime("%b %d")} — {datetime.now(timezone.utc).strftime("%b %d, %Y")}\n',
            f'<b>Database Overview</b>',
            f'Total leads: {total_leads:,}',
            f'New this week: +{count_new:,}',
            f'Imports this week: {count_imports}\n',
            f'<b>Data Quality</b>',
            f'With phone: {phone_count:,} ({round(phone_count/max(total_leads,1)*100)}%)',
            f'With city: {city_count:,} ({round(city_count/max(total_leads,1)*100)}%)',
            f'With country: {country_count:,} ({round(country_count/max(total_leads,1)*100)}%)',
            f'Buyers: {buyer_count:,} ({round(buyer_count/max(total_leads,1)*100)}%)',
            f'\n<b>Top Sources (New This Week)</b>',
        ]

        for source, count in sources_data:
            report.append(f'  • {source}: {count:,}')
        if not sources_data:
            report.append('  No new leads this week')

        await notifier.send_telegram('\n'.join(report))
        logger.info('Weekly report sent.')

if __name__ == '__main__':
    asyncio.run(generate_weekly_report())
