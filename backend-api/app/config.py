import os

from dotenv import load_dotenv


load_dotenv()


class Settings:
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    MONGO_DATABASE = os.getenv("MONGO_DATABASE", "steam_analytics")
    CORS_ORIGINS = tuple(
        origin.strip()
        for origin in os.getenv(
            "CORS_ORIGINS", "http://localhost:5173"
        ).split(",")
        if origin.strip()
    )


settings = Settings()