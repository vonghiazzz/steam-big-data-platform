from app.database.mongodb import database


class HealthRepository:
    async def ping(self) -> None:
        await database.command("ping")