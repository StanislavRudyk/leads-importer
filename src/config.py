import os

class Settings:
    METABASE_SITE_URL = '/analytics'
    METABASE_EMBEDDING_SECRET_KEY = os.getenv('METABASE_EMBEDDING_SECRET_KEY')
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql+asyncpg://postgres:postgres@db:5432/leads')
settings = Settings()
