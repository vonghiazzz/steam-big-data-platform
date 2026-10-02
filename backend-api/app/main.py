from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


from app.config import settings
from app.middleware.exception_handler import register_exception_handlers
from app.routers import analytics, health, realtime



app = FastAPI(
    title="Steam Analytics API",
    version="1.0.0",
)



app.add_middleware(
    CORSMiddleware,

    allow_origins=list(settings.CORS_ORIGINS),

    allow_methods=["GET"],

    allow_headers=["*"]
)



register_exception_handlers(app)
app.include_router(health.router)
app.include_router(analytics.router)
app.include_router(realtime.router)