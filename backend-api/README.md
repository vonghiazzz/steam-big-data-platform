# Steam Backend API

Read-only FastAPI interface over the `steam_analytics` MongoDB serving database. The API does not access HDFS, Kafka, or Spark and does not write to MongoDB.

## Setup

From `backend-api/`:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
uvicorn app.main:app --reload
```

The API is available at `http://localhost:8000`; Swagger UI is at `http://localhost:8000/docs`.

Configure `MONGO_URI`, `MONGO_DATABASE`, and comma-separated `CORS_ORIGINS` in `.env`. Defaults are `mongodb://localhost:27017`, `steam_analytics`, and `http://localhost:5173`. The MongoDB server must be started separately. The historical and realtime serving processes must populate their collections before analytics or realtime results are available; an empty collection returns an empty `data` list.

## Endpoints

| Method and path                         | Source                  | Response                                                 |
| --------------------------------------- | ----------------------- | -------------------------------------------------------- |
| `GET /api/health`                       | MongoDB ping            | `{"status":"healthy"}`                                   |
| `GET /api/analytics/games`              | `game_metrics`          | Game metrics                                             |
| `GET /api/analytics/games/top?limit=10` | `game_metrics`          | Ranked game metrics; limit 1-100                         |
| `GET /api/analytics/genres`             | `genre_metrics`         | Genre metrics                                            |
| `GET /api/analytics/playtime`           | `playtime_metrics`      | Playtime metrics                                         |
| `GET /api/analytics/free-paid`          | `free_paid_metrics`     | Free/paid metrics                                        |
| `GET /api/analytics/platforms`          | `platform_metrics`      | Platform metrics                                         |
| `GET /api/analytics/categories`         | `category_metrics`      | Category metrics                                         |
| `GET /api/analytics/purchase`           | `purchase_metrics`      | Purchase-source metrics                                  |
| `GET /api/realtime/reviews`             | `recent_reviews`        | Paginated reviews; optional `appid`, `page`, `page_size` |
| `GET /api/realtime/games`               | `realtime_game_metrics` | Windowed game metrics                                    |
| `GET /api/realtime/games/{appid}`       | `realtime_game_metrics` | Windowed metrics for one game; 404 if absent             |

Review pagination defaults to `page=1` and `page_size=20`; page size is limited to 100. Successful collection responses use a `data` array. Review responses also include `page`, `page_size`, and `total`. Errors use an `error` object with a stable code and sanitized message.

## Tests

From `backend-api/`:

```powershell
python -m pytest -q
```

Repository tests mock MongoDB; controller tests use FastAPI `TestClient` and do not require a running database.
