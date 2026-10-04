import os

from dotenv import load_dotenv


load_dotenv()


class Settings:
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    MONGO_DATABASE = os.getenv("MONGO_DATABASE", "steam_analytics")
    ML_MODEL_PATH = os.getenv("ML_MODEL_PATH", "").strip()
    HDFS_DEFAULT_FS = os.getenv("HDFS_DEFAULT_FS", "").strip()
    HADOOP_HOME = os.getenv("HADOOP_HOME", "").strip()
    SPARK_MASTER = os.getenv("SPARK_MASTER", "local[2]").strip()
    CORS_ORIGINS = tuple(
        origin.strip()
        for origin in os.getenv(
            "CORS_ORIGINS",
            "http://localhost:5173,http://127.0.0.1:5173",
        ).split(",")
        if origin.strip()
    )


settings = Settings()