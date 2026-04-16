import os
from typing import Optional, List
import jwt
import time
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.security import APIKeyHeader
from .db import AsyncSessionLocal
from .config import settings
from sqlalchemy import text

router = APIRouter()

api_key_query = APIKeyHeader(name='API-Key', auto_error=False)
x_api_key_query = APIKeyHeader(name='X-API-Key', auto_error=False)

DASHBOARD_TITLES = {
    1: "Leads Overview",
    2: "Campaigns & Sources",
    3: "System & Imports",
}

async def get_api_key(
    api_key: Optional[str] = Depends(api_key_query),
    x_api_key: Optional[str] = Depends(x_api_key_query)
):
    """Authorize using either 'API-Key' or 'X-API-Key' headers."""
    provided_key = api_key or x_api_key
    if provided_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail='Invalid API Key')

async def get_user_dashboard_ids(user_role: str, session) -> list:
    """Resolve allowed dashboard IDs for the given user role."""
    result = await session.execute(text("SELECT dashboard_ids FROM dashboard_permissions WHERE role = :role"), {"role": user_role})
    row = result.fetchone()
    if not row:
        return []
    return row[0]

def generate_metabase_token(dashboard_id: int) -> str:
    """Generate a signed JWT token for secure dashboard embedding."""
    payload = {'resource': {'dashboard': dashboard_id}, 'params': {}, 'exp': round(time.time()) + 600}
    return jwt.encode(payload, settings.METABASE_EMBEDDING_SECRET_KEY, algorithm='HS256')

@router.get('/api/v1/metabase/dashboards')
async def get_available_dashboards(
    user_role: str = Query('viewer'),
    api_key: str = Depends(get_api_key),
):
    """Retrieve the list of authorized Metabase dashboards with signed access tokens."""
    async with AsyncSessionLocal() as session:
        dashboard_ids = await get_user_dashboard_ids(user_role, session)
        dashboards = []
        for d_id in dashboard_ids:
            token = generate_metabase_token(d_id)
            url = f"{settings.METABASE_SITE_URL}/embed/dashboard/{token}#bordered=false&titled=false"
            dashboards.append({
                'id': d_id,
                'title': DASHBOARD_TITLES.get(d_id, f'Dashboard {d_id}'),
                'url': url
            })
        return {'dashboards': dashboards}

