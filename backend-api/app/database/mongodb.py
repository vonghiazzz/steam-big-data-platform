from motor.motor_asyncio import AsyncIOMotorClient
from app.config import settings


client = AsyncIOMotorClient(
    settings.MONGO_URI,
    serverSelectionTimeoutMS=5_000,
)


database = client[
    settings.MONGO_DATABASE
]


def get_collection(name):

    return database[name]