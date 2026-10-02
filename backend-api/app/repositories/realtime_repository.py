from app.database.mongodb import get_collection
from app.repositories.mongo_utils import serialize_document


class RealtimeRepository:
    async def find_reviews(
        self,
        appid: int | None,
        page: int,
        page_size: int,
    ) -> tuple[list[dict], int]:
        collection = get_collection("recent_reviews")
        query = {"appid": appid} if appid is not None else {}
        total = await collection.count_documents(query)
        cursor = (
            collection.find(query)
            .sort([("timestamp_created", -1), ("recommendationid", -1)])
            .skip((page - 1) * page_size)
            .limit(page_size)
        )
        documents = [
            serialize_document(document) async for document in cursor
        ]
        return documents, total

    async def find_game_metrics(
        self,
        appid: int | None = None,
    ) -> list[dict]:
        collection = get_collection("realtime_game_metrics")
        query = {"appid": appid} if appid is not None else {}
        cursor = collection.find(query).sort(
            [("window_start", -1), ("appid", 1)]
        )
        return [serialize_document(document) async for document in cursor]