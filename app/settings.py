import os

class Settings:
    DATABASE_URL: str = os.getenv("DATABASE_URL", "")
    DB_SCHEMA: str = os.getenv("DB_SCHEMA", "catalogo_v2")
    DATERIUM_USERID: str = os.getenv("DATERIUM_USERID", "")
    DATERIUM_BASE_URL: str = os.getenv("DATERIUM_BASE_URL", "https://api.dateriumsystem.com")
    HTTP_TIMEOUT_SECONDS: int = int(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))

settings = Settings()
