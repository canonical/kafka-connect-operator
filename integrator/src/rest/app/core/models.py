#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Domain models definition module."""

from typing import Literal

from pydantic import BaseModel

HealthStatus = Literal["ok", "error"]


class HealthResponse(BaseModel):
    """Response model for health check APIs."""

    status: HealthStatus
