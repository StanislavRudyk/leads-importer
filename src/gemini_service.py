import os
import logging
import httpx
import json
import asyncio
from typing import List, Dict, Optional

logger = logging.getLogger('leads_importer.gemini_service')

GEMINI_API_KEY = "AIzaSyDerQkbPtO3ZczVzQWoXo_TBvTwZVorbIU"
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key={GEMINI_API_KEY}"

class GeminiService:
    def __init__(self):
        self.api_key = GEMINI_API_KEY
        self.client = httpx.AsyncClient(timeout=30.0)

    async def generate_content(self, prompt: str) -> Optional[str]:
        try:
            payload = {
                "contents": [
                    {
                        "parts": [
                            {"text": prompt}
                        ]
                    }
                ],
                "generationConfig": {
                    "temperature": 0.1,
                    "topP": 0.95,
                    "topK": 40,
                    "maxOutputTokens": 2048,
                    "responseMimeType": "application/json"
                }
            }
            response = await self.client.post(GEMINI_URL, json=payload)
            response.raise_for_status()
            data = response.json()
            
            if 'candidates' in data and len(data['candidates']) > 0:
                text = data['candidates'][0]['content']['parts'][0]['text']
                return text
            return None
        except Exception as e:
            logger.error(f"Gemini API error: {e}")
            return None

    def _extract_json(self, text: str) -> str:
        """Extracts JSON string from markdown or raw text."""
        if not text:
            return ""
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].strip()
        return text.strip()

    async def clean_names(self, names: List[str]) -> Dict[str, str]:
        """
        Cleans a list of messy city/show names.
        Returns a dictionary mapping original names to cleaned names.
        """
        if not names:
            return {}
        
        prompt = (
            "I have a list of messy city or show names from a database. "
            "Please clean them up. Remove file extensions (like .csv), remove technical strings like 'Subscribers', "
            "and fix capitalization. Return the result as a JSON dictionary where the key is the original string "
            "and the value is the cleaned string. Only return the JSON object.\n\n"
            f"Names: {json.dumps(names)}"
        )
        
        result_text = await self.generate_content(prompt)
        if not result_text:
            return {}
        
        try:
            result_text = self._extract_json(result_text)
            return json.loads(result_text)
        except Exception as e:
            logger.error(f"Failed to parse Gemini response: {e}")
            return {}

    async def enrich_us_states(self, city_state_pairs: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Enriches US city data with correct state codes.
        Input: list of {'city': '...', 'state': '...'}
        Output: list of enriched pairs.
        """
        if not city_state_pairs:
            return []
        
        prompt = (
            "For the following US cities, provide the correct 2-letter state code if it's missing or incorrect. "
            "If the state is already correct, keep it. Return a JSON array of objects with 'city' and 'state' keys. "
            "Only return the JSON array.\n\n"
            f"Cities: {json.dumps(city_state_pairs)}"
        )
        
        result_text = await self.generate_content(prompt)
        if not result_text:
            return city_state_pairs
        
        try:
            result_text = self._extract_json(result_text)
            return json.loads(result_text)
        except Exception as e:
            logger.error(f"Failed to parse Gemini state response: {e}")
            return city_state_pairs

gemini_service = GeminiService()
