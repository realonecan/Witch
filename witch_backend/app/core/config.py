"""
Configuration settings for the Witch application.
Uses Pydantic BaseSettings for environment variable management.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Core Settings
    UPLOAD_DIR: str = "./data"
    
    # API Keys (This was missing!)
    OPENAI_API_KEY: str | None = None

    # Configuration to read from .env file
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"  # This prevents crashes if you have extra keys in .env
    )


settings = Settings()