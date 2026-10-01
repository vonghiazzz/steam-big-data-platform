from app.repositories.health_repository import HealthRepository


class HealthService:
    def __init__(self, repository: HealthRepository | None = None):
        self.repo = repository or HealthRepository()

    async def check(self) -> None:
        await self.repo.ping()