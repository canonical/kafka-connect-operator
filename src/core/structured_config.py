#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Structured configuration for the Kafka Connect charm."""
import logging

from charms.data_platform_libs.v0.data_models import BaseConfigModel
from pydantic import validator

from literals import CharmProfile, LogLevel

logger = logging.getLogger(__name__)


class CharmConfig(BaseConfigModel):
    """Manager for the structured configuration."""

    exactly_once_source_support: bool
    key_converter: str
    log_level: LogLevel
    profile: CharmProfile
    rest_port: int
    value_converter: str

    @validator("*", pre=True)
    @classmethod
    def blank_string(cls, value):
        """Check for empty strings."""
        if value == "":
            return None
        return value
