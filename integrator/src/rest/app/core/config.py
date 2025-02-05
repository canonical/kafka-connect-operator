#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""FastAPI configuration module."""

from typing import List, Union

from pydantic import BaseSettings, validator


class Settings(BaseSettings):
    """Application configuration object."""

    PROJECT_NAME: str = "Kafka Connect Integrator REST API"
    PLUGIN_FILE_PATH: str = "./resources/FakeResource.tar"
    PLUGIN_FILE_NAME: str = "plugin.tar"
    PLUGIN_MEDIA_TYPE: str = "application/x-tar"
    SERVE_CHUNK_SIZE: int = 1024 * 1024  # 1MB
    BACKEND_CORS_ORIGINS: Union[List[str], str] = []

    @validator("BACKEND_CORS_ORIGINS", pre=True)
    @classmethod
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> Union[List[str], str]:
        """Validator for CORS origins."""
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)

    class Config:
        """Configuration for environment variable overrides."""

        case_sensitive: bool = True
        env_file: str = ".env"


settings = Settings()
