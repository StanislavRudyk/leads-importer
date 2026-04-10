
import asyncio
import re
import logging
from sqlalchemy import text
from src.db import AsyncSessionLocal, Lead
from src.city_data import KNOWN_CITIES, NOT_CITIES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('cleanup')

async def run_cleanup():
    logger.info("Starting database cleanup...")
    async with AsyncSessionLocal() as session:
        # 1. Исправляем Lagos и London (неправильно помеченные как US)
        logger.info("Fixing misclassified global cities (London/Lagos/Berlin)...")
        await session.execute(text("""
            UPDATE leads 
            SET country_iso2 = 'NG' 
            WHERE city = 'Lagos' AND country_iso2 = 'US';
        """))
        await session.execute(text("""
            UPDATE leads 
            SET country_iso2 = 'GB' 
            WHERE city = 'London' AND country_iso2 = 'US';
        """))
        await session.execute(text("""
            UPDATE leads 
            SET country_iso2 = 'DE' 
            WHERE city = 'Berlin' AND country_iso2 = 'US';
        """))
        await session.execute(text("""
            UPDATE leads 
            SET country_iso2 = 'CA' 
            WHERE city = 'Toronto' AND country_iso2 = 'US';
        """))

        # 2. Очищаем мусорные города (те, что содержат цифры или "Report", "Excel")
        logger.info("Nullifying garbage city names matching noise patterns...")
        garbage_patterns = [
            'Miami 10,000', 'Crm Email Excel', 'Report', 'Excel', 'xlsx', 'csv', 
            'Contacts', '9,000', 'Pt', 'York', 'Raw', 'Peyman', 'Subscribed',
            'Non Persians', 'Thanksgiving', 'Patrons'
        ]
        for pattern in garbage_patterns:
            await session.execute(text(f"UPDATE leads SET city = NULL WHERE city ILIKE '%{pattern}%';"))

        # 3. Дополнительная чистка по регулярке (любые города с цифрами)
        await session.execute(text("UPDATE leads SET city = NULL WHERE city ~ '\\d';"))

        # 4. Попытка пере-нормализовать страну для тех, у кого city известен, но country пуста
        logger.info("Re-aligning countries for known cities...")
        for city_name, (country, state) in KNOWN_CITIES.items():
            formatted_city = city_name.title()
            await session.execute(
                text("""
                    UPDATE leads 
                    SET country_iso2 = :country, state = COALESCE(state, :state)
                    WHERE (city = :formatted_city OR city = :city_name) 
                    AND (country_iso2 IS NULL OR country_iso2 = 'XX');
                """),
                {
                    "country": country,
                    "state": state or '',
                    "formatted_city": formatted_city,
                    "city_name": city_name
                }
            )

        await session.commit()
        logger.info("Cleanup finished successfully.")

if __name__ == "__main__":
    asyncio.run(run_cleanup())
