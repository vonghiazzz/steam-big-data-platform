from app.repositories.analytics_repository import AnalyticsRepository



class AnalyticsService:
    def __init__(self, repository: AnalyticsRepository | None = None):
        self.repo = repository or AnalyticsRepository()

    async def get_games(self) -> list[dict]:
        return await self.repo.find_all("game_metrics")

    async def get_metrics(self, collection_name: str) -> list[dict]:
        return await self.repo.find_all(collection_name)

    async def get_top_games(self, limit: int) -> list[dict]:
        return await self.repo.find_top_games(limit)