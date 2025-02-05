#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Main module for FastAPI singleton instantiation."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.apis.v1 import api_router as api_router_v1
from app.core.config import settings
from app.core.middlewares import ProcessTimeMiddleware


def get_application() -> FastAPI:
    """Returns the FastAPI singleton instance."""
    _app = FastAPI(title=settings.PROJECT_NAME, openapi_url="/openapi.json")

    _app.add_middleware(
        CORSMiddleware,
        allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    _app.add_middleware(ProcessTimeMiddleware)
    _app.include_router(api_router_v1, prefix="/api/v1")

    return _app


app = get_application()
