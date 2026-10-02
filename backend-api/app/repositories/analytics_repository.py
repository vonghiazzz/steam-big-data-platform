from app.database.mongodb import get_collection
from app.repositories.mongo_utils import serialize_document



class AnalyticsRepository:
    async def find_all(self, collection_name: str) -> list[dict]:
        collection = get_collection(collection_name)
        return [
            serialize_document(document)
            async for document in collection.find({})
        ]

    async def find_top_games(self, limit: int) -> list[dict]:
        collection = get_collection("game_metrics")
        cursor = collection.find({}).sort(
            [
                ("recommendation_rate", -1),
                ("review_count", -1),
                ("appid", 1),
            ]
        ).limit(limit)
        return [serialize_document(document) async for document in cursor]