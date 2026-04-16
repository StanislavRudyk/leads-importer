import logging
import asyncio
import httpx
from typing import Optional, Tuple
from .db import AsyncSessionLocal, Market
from sqlalchemy import select, update

logger = logging.getLogger('leads_importer.geocoder')

class Geocoder:
    def __init__(self):
        self.base_url = "https://nominatim.openstreetmap.org/search"
        self.user_agent = "LeadsImporter/1.0 (admin@maxify.it)"

        self.semaphore = asyncio.Semaphore(1)

    async def get_coordinates(self, city: str, country_code: str) -> Tuple[Optional[float], Optional[float]]:
        async with self.semaphore:
            try:

                await asyncio.sleep(1.0)
                async with httpx.AsyncClient() as client:
                    params = {
                        "city": city,
                        "country": country_code,
                        "format": "json",
                        "limit": 1
                    }
                    headers = {"User-Agent": self.user_agent}
                    response = await client.get(self.base_url, params=params, headers=headers, timeout=10.0)
                    response.raise_for_status()
                    data = response.json()
                    
                    if data and len(data) > 0:
                        lat = float(data[0]["lat"])
                        lon = float(data[0]["lon"])
                        return lat, lon
            except Exception as e:
                logger.error(f"Geocoding failed for {city}, {country_code}: {e}")
        return None, None

    async def enrich_market_coordinates(self, background_tasks=None):
        """Finds markets without coordinates and fetches them."""
        async with AsyncSessionLocal() as session:
            stmt = select(Market).where(Market.latitude.is_(None)).limit(100)
            res = await session.execute(stmt)
            markets = res.scalars().all()
            
            if not markets:
                logger.info("No markets need geocoding right now.")
                return 0
                
            logger.info(f"Geocoding {len(markets)} markets in background...")
            updated = 0
            
            for market in markets:
                if not market.city or not market.country_iso2:
                    continue
                lat, lon = await self.get_coordinates(market.city, market.country_iso2)
                if lat is not None and lon is not None:
                    await session.execute(
                        update(Market)
                        .where(Market.city == market.city, Market.country_iso2 == market.country_iso2)
                        .values(latitude=lat, longitude=lon)
                    )
                    updated += 1
            
            if updated > 0:
                await session.commit()
            return updated

geocoder = Geocoder()
