#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Service (controller or manager) implementation for plugin API."""

import pathlib

from app.core.config import settings


def chunked_serve(file_path: pathlib.Path, chunk_size: int = settings.SERVE_CHUNK_SIZE):
    """Serves a file in a chunked fashion."""
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            yield chunk
