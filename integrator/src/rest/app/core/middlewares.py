#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Middlewares definition module."""


import time

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response
from typing_extensions import override


class ProcessTimeMiddleware(BaseHTTPMiddleware):
    """Middleware for calculating API call processing time."""

    @override
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request.state.time_started = time.time()
        response = await call_next(request)
        process_time = time.time() - request.state.time_started
        response.headers["X-Process-Time"] = str(process_time)

        return response
