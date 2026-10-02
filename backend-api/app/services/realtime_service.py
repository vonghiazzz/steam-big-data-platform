from app.repositories.realtime_repository import RealtimeRepository
from app.services.exceptions import ResourceNotFoundError


class RealtimeService:
    def __init__(self, repository: RealtimeRepository | None = None):
        self.repo = repository or RealtimeRepository()

    async def get_reviews(
        self,
        appid: int | None,
        page: int,
        page_size: int,
    ) -> dict:
        documents, total = await self.repo.find_reviews(appid, page, page_size)
        return {
            "data": documents,
            "page": page,
            "page_size": page_size,
            "total": total,
        }

    async def get_games(self) -> list[dict]:
        return await self.repo.find_game_metrics()

    async def get_game_metrics(self, appid: int) -> list[dict]:
        documents = await self.repo.find_game_metrics(appid)
        if not documents:
            raise ResourceNotFoundError("Game not found")
        return documents