"""Backend API entrypoint.

Serves:
- the read-only analytics/realtime API under /api/*
- the built frontend (frontend/dist) as static files at /

Run from the repo root or backend-api folder:
    uvicorn app.main:app --host 0.0.0.0 --port 8000
"""

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.exceptions import HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from app.routers import analytics, realtime

REPO_ROOT = Path(__file__).resolve().parents[2]
FRONTEND_DIST = REPO_ROOT / "frontend" / "dist"

app = FastAPI(title="Steam Signal API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"message": str(exc.detail)}},
    )


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


app.include_router(analytics.router)
app.include_router(realtime.router)

if FRONTEND_DIST.exists():
    app.mount("/", StaticFiles(directory=FRONTEND_DIST, html=True), name="frontend")
