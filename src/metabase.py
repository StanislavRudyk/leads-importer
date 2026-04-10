import os
import jwt
import time
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.security import APIKeyHeader
from .db import AsyncSessionLocal
from .config import settings
from sqlalchemy import text

router = APIRouter()

api_key_header = APIKeyHeader(name='API-Key', auto_error=False)

async def get_api_key(api_key: str = Depends(api_key_header)):
    """Проверка API ключа."""
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail='Invalid API Key')

async def get_user_dashboard_ids(user_role: str, session) -> list:
    """Получение списка разрешенных ID дашбордов для роли пользователя."""
    result = await session.execute(text("SELECT dashboard_ids FROM dashboard_permissions WHERE role = :role"), {"role": user_role})
    row = result.fetchone()
    if not row:
        return []
    return row[0]

async def get_dashboard_titles(session) -> dict:
    """Получение названий дашбордов из таблицы permissions (кеш на уровне запроса)."""
    result = await session.execute(text(
        "SELECT DISTINCT unnest(dashboard_ids) as did FROM dashboard_permissions"
    ))
    ids = [r[0] for r in result.fetchall()]
    return {d_id: f'Dashboard {d_id}' for d_id in ids}

def generate_metabase_token(dashboard_id: int) -> str:
    """Генерация JWT токена для встраивания дашборда Metabase."""
    payload = {'resource': {'dashboard': dashboard_id}, 'params': {}, 'exp': round(time.time()) + 600}
    return jwt.encode(payload, settings.METABASE_EMBEDDING_SECRET_KEY, algorithm='HS256')

@router.get('/api/v1/metabase/dashboards')
async def get_available_dashboards(
    user_role: str = Query('viewer'),
    api_key: str = Depends(get_api_key),
):
    """Получение списка доступных дашбордов Metabase с токенами доступа."""
    async with AsyncSessionLocal() as session:
        dashboard_ids = await get_user_dashboard_ids(user_role, session)
        titles = await get_dashboard_titles(session)
        dashboards = []
        for d_id in dashboard_ids:
            token = generate_metabase_token(d_id)
            url = f"{settings.METABASE_SITE_URL}/embed/dashboard/{token}#bordered=false&titled=false"
            dashboards.append({
                'id': d_id,
                'title': titles.get(d_id, f'Dashboard {d_id}'),
                'url': url
            })
        return {'dashboards': dashboards}

