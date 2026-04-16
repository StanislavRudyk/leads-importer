import logging
import json
import asyncio
from typing import List, Dict, Any, Optional
from .db import AsyncSessionLocal, Lead
from sqlalchemy import select, update, and_, or_, func
from .gemini_service import gemini_service

logger = logging.getLogger('leads_importer.enricher')

class LeadEnricher:
    def __init__(self):
        self.batch_size = 30

    async def run_enrichment(self):
        """Main entry point to enrich leads missing critical state/status info or having messy names."""
        async with AsyncSessionLocal() as session:

            stmt = select(Lead).where(
                or_(
                    and_(Lead.country_iso2 == 'US', or_(Lead.state.is_(None), Lead.state == '', Lead.state == 'Other', func.length(Lead.state) > 2)),
                    Lead.show_state.is_(None),
                    Lead.show_state == '',
                    Lead.show_state == 'Other',
                    ~Lead.show_state.in_(['done', 'soon', 'active']),
                    Lead.city.like('%.csv%'),
                    Lead.city.like('%Subscribers%'),
                    Lead.city.like('%List%'),
                    Lead.city.like('%/%')
                )
            ).order_by(Lead.id.desc()).limit(300)
            
            res = await session.execute(stmt)
            leads = res.scalars().all()
            
            if not leads:
                logger.info("No leads found for AI enrichment.")
                return

            logger.info(f"AI Enrichment: Processing {len(leads)} leads...")
            
            for i in range(0, len(leads), self.batch_size):
                batch = leads[i:i + self.batch_size]
                await self._process_batch(session, batch)

    async def _process_batch(self, session, leads: List[Lead]):
        prompt_data = []
        for l in leads:
            prompt_data.append({
                "id": l.id,
                "city": l.city,
                "country": l.country_iso2,
                "context": l.show_context,
                "state": l.state
            })

        prompt = f"""
        You are an expert data cleaner. Given the following list of musician lead records, identify and CLEAN the data:
        1. 'city': Fix the city name. Remove file extensions (like .csv), remove technical strings like 'Subscribers' or 'List' or 'Newsletter'. Fix capitalization.
        2. 'state': For US (United States) leads, provide the correct 2-letter state code (e.g. 'NY', 'CA', 'TX'). If the 'city' contains the state (e.g. 'Austin, TX'), extract the state. For other countries, leave it NULL.
        3. 'show_state': Identify the status of the show event based on context ('done', 'soon', 'active'). 
           - 'done' if the context mentions a past tour, specific past date, or "previous".
           - 'soon' if it mentions "waiting list", "upcoming", "notified", or a future registration.
           - 'active' if it mentions "on sale", "tickets available", or "current tour".
        4. 'artist': Extract the clean Artist name from the context (e.g., from 'Max Amini - Subscribers' extract 'Max Amini').
        
        List of leads:
        {json.dumps(prompt_data)}
        
        Respond ONLY with a JSON array of objects:
        [{{"id": 123, "city": "Clean City", "state": "TX", "show_state": "done|soon|active", "artist": "Artist Name"}}, ...]
        """

        try:
            result_text = await gemini_service.generate_content(prompt)
            if not result_text:
                return


            if "```json" in result_text:
                result_text = result_text.split("```json")[1].split("```")[0].strip()
            elif "```" in result_text:
                result_text = result_text.split("```")[1].strip()
            
            results = json.loads(result_text)
            
            for res in results:
                lead_id = res.get('id')
                if lead_id:
                    update_vals = {
                        'city': res.get('city'),
                        'state': res.get('state'),
                        'show_state': res.get('show_state'),
                        'show_context': res.get('artist') or res.get('show_context')
                    }
          
                    update_vals = {k: v for k, v in update_vals.items() if v is not None}
                    
                    if update_vals:
                        await session.execute(
                            update(Lead).where(Lead.id == lead_id).values(**update_vals)
                        )
            await session.commit()
        except Exception as e:
            logger.error(f"Enrichment error: {e}")
            await session.rollback()

enricher = LeadEnricher()

