from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pymongo.errors import PyMongoError
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.services.exceptions import ResourceNotFoundError


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(RequestValidationError)
    async def invalid_parameter_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={
                "error": {
                    "code": "INVALID_PARAMETER",
                    "message": "Invalid request parameters",
                }
            },
        )

    @app.exception_handler(PyMongoError)
    async def database_error_handler(
        request: Request, exc: PyMongoError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "DATABASE_ERROR",
                    "message": "Database operation failed",
                }
            },
        )

    @app.exception_handler(ResourceNotFoundError)
    async def not_found_handler(
        request: Request, exc: ResourceNotFoundError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=404,
            content={
                "error": {"code": "RESOURCE_NOT_FOUND", "message": str(exc)}
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        code = {
            400: "INVALID_PARAMETER",
            404: "RESOURCE_NOT_FOUND",
        }.get(exc.status_code, "INTERNAL_ERROR")
        message = "Resource not found" if exc.status_code == 404 else str(exc.detail)
        if exc.status_code >= 500:
            message = "Request failed"
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": {"code": code, "message": message}},
        )

    @app.exception_handler(Exception)
    async def internal_error_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "An internal error occurred",
                }
            },
        )