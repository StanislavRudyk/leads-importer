import os
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    """Конфигурация приложения с валидацией через Pydantic."""

    # База данных 
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/leads"
    SYNC_DATABASE_URL: str = "postgresql://postgres:postgres@localhost:5432/leads"

    # Metabase
    METABASE_EMBEDDING_SECRET_KEY: str 
    METABASE_SITE_URL: str = "/analytics"

    # API
    API_KEY: str

    # Telegram
    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_CHAT_ID: Optional[str] = None

    # SMTP (Email)
    SMTP_SERVER: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_USER: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None

    # Настройки импорта
    MAX_FILE_SIZE_MB: int = 100
    BATCH_SIZE: int = 2500

    model_config = SettingsConfigDict(
        env_file=".env", 
        env_file_encoding='utf-8',
        extra='ignore' 
    )


settings = Settings()