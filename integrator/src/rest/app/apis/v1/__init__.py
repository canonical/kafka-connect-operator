#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""API routes definition."""


from app.apis.v1 import healthcheck, plugin
from fastapi import APIRouter

api_router = APIRouter()

api_router.include_router(healthcheck.router, prefix="/healthcheck", tags=["healthcheck"])
api_router.include_router(plugin.router, prefix="/plugin", tags=["plugin"])
