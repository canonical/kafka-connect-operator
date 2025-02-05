#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling operations on Kafka Connect workers."""

import glob
import logging
import os
import tempfile
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth
from tenacity import (
    retry,
    retry_any,
    retry_if_exception,
    retry_if_result,
    stop_after_attempt,
    wait_fixed,
)

from core.models import Context
from core.workload import WorkloadBase
from literals import GROUP, USER

logger = logging.getLogger(__name__)


class ConnectManager:
    """Manager for functions related to Kafka Connect workers and connector plugins."""

    KAFKA_CLUSTER_ID = "kafka_cluster_id"
    VERSION = "version"
    REQUEST_TIMEOUT = 2

    def __init__(self, context: Context, workload: WorkloadBase):
        self.context = context
        self.workload = workload
        self._plugins_cache = set()

        self.reload_plugins()

    @property
    def plugins_cache(self) -> set:
        """Local cache of available plugins, populated using checksums of loaded plugins."""
        return self._plugins_cache

    @property
    def plugin_path_initiated(self) -> bool:
        """Checks whether plugin path is initiated or not."""
        return os.path.exists(self.workload.paths.plugins)

    def _plugin_checksum(self, plugin_path: Path) -> str:
        """Calculates checksum of a plugin, currently uses SHA-256 algorithm."""
        # Python 3.11+ has hashlib.file_digest(...) method but we use linux utils here for compatibility and better performance.
        raw = self.workload.exec(["sha256sum", str(plugin_path)]).split()
        return raw[0]

    def _create_plugin_dir(self, plugin_path: Path, path_prefix: str = "") -> Path:
        """Creates a unique dir under `PLUGIN_PATH` to better organize lib JAR files."""
        path_prefix_ = f"{path_prefix}-" if path_prefix else ""
        path = (
            Path(self.workload.paths.plugins)
            / f"{path_prefix_}{self._plugin_checksum(plugin_path)}"
        )
        self.workload.mkdir(f"{path}")
        self.workload.exec(["chown", "-R", f"{USER}:{GROUP}", f"{path}"])
        return path

    def _untar_plugin(self, src_path: Path, dst_path: Path) -> None:
        """UnTARs and decompresses a plugin tarball to the `PLUGIN_PATH` folder."""
        match os.path.splitext(src_path)[-1]:
            case ".gz" | ".tgz":
                opts = "xvzf"
            case ".tar":
                opts = "xvf"
            case _:
                opts = "xvf"

        self.workload.exec(["tar", f"-{opts}", str(src_path), "-C", str(dst_path)])

    def _download_plugin(self, url: str, dst_path: str):
        """Downloads plugin from a given URL and returns the file object."""
        response = requests.get(url, stream=True)
        with open(dst_path, mode="wb") as file:
            for chunk in response.iter_content(chunk_size=10 * 1024):
                file.write(chunk)

    def init_plugin_path(self) -> None:
        """Initiates `PLUGIN_PATH` folder and sets correct ownership and permissions."""
        self.workload.mkdir(self.workload.paths.plugins)
        self.workload.exec(["chmod", "-R", "750", f"{self.workload.paths.plugins}"])
        self.workload.exec(["chown", "-R", f"{USER}:{GROUP}", f"{self.workload.paths.plugins}"])

    def reload_plugins(self) -> None:
        """Reloads the local `plugins_cache`."""
        try:
            self._plugins_cache = {
                f.name
                for f in os.scandir(self.workload.paths.plugins)
                if f.is_dir() and not f.name.startswith(".")
            }
        except FileNotFoundError:  # possibly since plugins folder not created yet.
            return

    def load_plugin(self, resource_path: Path, path_prefix: str = "") -> None:
        """Loads a plugin from a given `resource_path` to the `PLUGIN_PATH` folder, skips if previously loaded."""
        if self._plugin_checksum(resource_path) in self.plugins_cache:
            logger.debug(f"Plugin {resource_path.name} already loaded, skipping...")
            return

        load_path = self._create_plugin_dir(resource_path, path_prefix=path_prefix)
        self._untar_plugin(resource_path, load_path)
        self.workload.rmdir(f"{resource_path}")
        self.reload_plugins()

    def load_plugin_from_url(self, plugin_url: str, path_prefix: str = "") -> None:
        """Loads a plugin from a given `plugin_url` to the `PLUGIN_PATH` folder, skips if previously loaded."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "plugin.tar"
            self._download_plugin(plugin_url, f"{path}")
            self.load_plugin(path, path_prefix=path_prefix)

    def remove_plugin(self, path_prefix: str) -> None:
        """Removes plugins for which the plugin-path starts with `path_prefix`."""
        for path in glob.glob(f"{self.workload.paths.plugins}/{path_prefix}*"):
            self.workload.exec(["rm", "-rf", path])

    def ping_connect_api(self) -> requests.Response:
        """Makes a GET request to the unit's Connect API Endpoint and returns the response."""
        auth = HTTPBasicAuth(
            self.context.peer_workers.ADMIN_USERNAME, self.context.peer_workers.admin_password
        )
        return requests.get(
            self.context.rest_uri, timeout=self.REQUEST_TIMEOUT, auth=auth, verify=False
        )

    @retry(
        wait=wait_fixed(3),
        stop=stop_after_attempt(5),
        retry=retry_any(
            retry_if_result(lambda result: result is False), retry_if_exception(lambda _: True)
        ),
        retry_error_callback=lambda _: False,
    )
    def health_check(self) -> bool:
        """Checks the health of connect service by pinging the Connect API."""
        response = self.ping_connect_api()

        if response.status_code != 200:
            return False

        if self.KAFKA_CLUSTER_ID not in response.json():
            return False

        return True

    def restart_worker(self) -> None:
        """Attempts to restart the connect worker."""
        self.workload.restart()
