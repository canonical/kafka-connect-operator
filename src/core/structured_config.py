#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Structured configuration for the Kafka Connect charm."""
import logging
from enum import Enum

from charms.data_platform_libs.v0.data_models import BaseConfigModel
from pydantic import validator

logger = logging.getLogger(__name__)


class LogLevel(str, Enum):
    """Enum for the `log_level` field."""

    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    DEBUG = "DEBUG"


class CharmConfig(BaseConfigModel):
    """Manager for the structured configuration."""

    rest_port: int

    @validator("*", pre=True)
    @classmethod
    def blank_string(cls, value):
        """Check for empty strings."""
        if value == "":
            return None
        return value
