#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling operations on Kafka Connect workers."""

import logging
import os
from pathlib import Path

from charms.operator_libs_linux.v2 import snap
from tenacity import RetryError, Retrying, stop_after_attempt, wait_fixed

from core.models import Context
from core.workload import WorkloadBase
from literals import GROUP, PLUGIN_PATH, USER

logger = logging.getLogger(__name__)


class ConnectManager:
    """Manager for functions related to Kafka Connect workers and connector plugins."""

    def __init__(self, context: Context, workload: WorkloadBase):
        self.context = context
        self.workload = workload
        self._plugins_cache = set()

        self.reload_plugins()

    @property
    def plugins_cache(self) -> set:
        """Local cache of available plugins, populated using checksums of loaded plugins."""
        return self._plugins_cache

    def _plugin_checksum(self, plugin_path: Path) -> str:
        """Calculates checksum of a plugin, currently uses SHA-256 algorithm."""
        # Python 3.11+ has hashlib.file_digest(...) method but we use linux utils here for compatibility and better performance.
        raw = self.workload.exec(["sha256sum", str(plugin_path)]).split()
        return raw[0]

    def _create_plugin_dir(self, plugin_path: Path) -> Path:
        """Creates a unique dir under `PLUGIN_PATH` to better organize lib JAR files."""
        path = Path(PLUGIN_PATH) / self._plugin_checksum(plugin_path)
        self.workload.mkdir(f"{path}")
        self.workload.exec(["chown", "-R", f"{USER}:{GROUP}", f"{path}"])
        return path

    def _untar_plugin(self, src_path: Path, dst_path: Path):
        """UnTARs and decompresses a plugin tarball to the `PLUGIN_PATH` folder."""
        match os.path.splitext(src_path)[-1]:
            case ".gz" | ".tgz":
                opts = "xvzf"
            case ".tar":
                opts = "xvf"
            case _:
                opts = "xvf"

        self.workload.exec(["tar", f"-{opts}", str(src_path), "-C", str(dst_path)])

    def init_plugin_path(self) -> bool:
        """Creates `PLUGIN_PATH` folder if not existent, returns False if not successful."""
        if os.path.exists(PLUGIN_PATH):
            return True

        try:
            self.workload.mkdir(f"{PLUGIN_PATH}")
            return True
        except FileNotFoundError:
            pass

        return False

    def reload_plugins(self):
        """Reloads the local `plugins_cache`."""
        try:
            self._plugins_cache = {
                f.name
                for f in os.scandir(PLUGIN_PATH)
                if f.is_dir() and not f.name.startswith(".")
            }
        except FileNotFoundError:  # possibly since plugins folder not created yet.
            return

    def load_plugin(self, resource_path: Path):
        """Loads a plugin from a given `resource_path` to the `PLUGIN_PATH` folder, skips if previously loaded."""
        if self._plugin_checksum(resource_path) in self.plugins_cache:
            logger.debug(f"Plugin {resource_path.name} already loaded, skipping...")
            return

        load_path = self._create_plugin_dir(resource_path)
        self._untar_plugin(resource_path, load_path)
        self.workload.rmdir(f"{resource_path}")
        self.reload_plugins()

    def health_check(self) -> bool:
        """Checks the health of connect service by pinging the Connect API."""
        return self.workload.check_socket(
            self.context.worker_unit.internal_address, self.context.rest_port
        )

    def restart_worker(self):
        """Attempts to restart the connect worker and ensure the service is running."""
        self.workload.restart()

        attempts = 5
        try:
            for attempt in Retrying(wait=wait_fixed(2), stop=stop_after_attempt(attempts)):
                with attempt:
                    self.health_check()
        except (RetryError, snap.SnapError):
            logger.warning(f"Failed to get service PID after {attempts} attempts.")
