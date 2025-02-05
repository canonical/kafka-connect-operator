#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Entrypoint for the RESTful service."""

import os

import uvicorn

HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", 8000))


if __name__ == "__main__":
    uvicorn.run("app:app", host=HOST, port=PORT, log_level="info")
