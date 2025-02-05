#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Plugin API routes definition."""

from pathlib import Path

from app.core.config import settings
from app.services.plugin import chunked_serve
from fastapi import APIRouter
from fastapi.responses import StreamingResponse

router = APIRouter()


@router.get("/")
async def serve_plugin() -> StreamingResponse:
    """Serves the connector plugin TAR file."""
    headers = {"Content-Disposition": f'attachment; filename="{settings.PLUGIN_FILE_NAME}"'}
    path = Path(settings.PLUGIN_FILE_PATH)
    return StreamingResponse(
        chunked_serve(path), headers=headers, media_type=settings.PLUGIN_MEDIA_TYPE
    )
