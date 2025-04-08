#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Structured configuration for the Kafka Connect charm."""
import logging
import re

from charms.data_platform_libs.v0.data_models import BaseConfigModel
from pydantic import validator

from literals import CharmProfile, LogLevel

logger = logging.getLogger(__name__)


SECRET_REGEX = re.compile("secret:[a-z0-9]{20}")


class CharmConfig(BaseConfigModel):
    """Manager for the structured configuration."""

    system_users: str | None = None
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

    @validator("system_users")
    @classmethod
    def system_users_secret_validator(cls, value: str) -> str:
        """Check validity of `system-users` field which should be a user secret URI."""
        if not SECRET_REGEX.match(value):
            raise ValueError(
                "Provided value for system-users config is not a valid secret URI, "
                "accepted values are formatted like 'secret:cvnra0b1c2e3f4g5hi6j'"
            )

        return value
