#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Kafka Connect workload class and methods."""

import glob as _glob
import logging
import os
import socket
import subprocess
from contextlib import closing
from typing import BinaryIO, Iterable, Mapping

from charms.operator_libs_linux.v2 import snap
from ops import pebble
from tenacity import retry, retry_if_result, stop_after_attempt, wait_fixed
from typing_extensions import override

from core.workload import DirEntry, WorkloadBase
from literals import (
    CHARMED_KAFKA_SNAP_REVISION,
    GROUP,
    SERVICE_NAME,
    SNAP_NAME,
    USER,
    CharmProfile,
)

logger = logging.getLogger(__name__)


class Workload(WorkloadBase):
    """Wrapper for performing common operations specific to the Kafka Connect Snap."""

    service: str

    def __init__(self, profile: CharmProfile = "production") -> None:
        self.kafka = snap.SnapCache()[SNAP_NAME]
        self.service = SERVICE_NAME
        self.profile = profile
        self.log_sensitive_output = profile == "testing"

    @override
    def start(self) -> None:
        try:
            self.kafka.start(services=[self.service])
        except snap.SnapError as e:
            logger.exception(str(e))

    @override
    def stop(self) -> None:
        try:
            self.kafka.stop(services=[self.service])
        except snap.SnapError as e:
            logger.exception(str(e))

    @override
    def restart(self) -> None:
        try:
            self.kafka.restart(services=[self.service])
        except snap.SnapError as e:
            logger.exception(str(e))

    @override
    def read(self, path: str) -> list[str]:
        if not os.path.exists(path):
            return []
        else:
            with open(path) as f:
                content = f.read().split("\n")

        return content

    @override
    def write(self, content: str | BinaryIO, path: str, mode: str = "w") -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, mode) as f:
            f.write(content)

        self.exec(["chown", "-R", f"{USER}:{GROUP}", f"{path}"])

    @override
    def exec(
        self,
        command: list[str] | str,
        env: Mapping[str, str] | None = None,
        working_dir: str | None = None,
        sensitive: bool = True,
    ) -> str:
        should_log = not sensitive or self.log_sensitive_output
        try:
            output = subprocess.check_output(
                command,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                shell=isinstance(command, str),
                env=env,
                cwd=working_dir,
            )
            if should_log:
                logger.debug(f"{output=}")
            return output
        except subprocess.CalledProcessError as e:
            if should_log:
                logger.error(f"cmd failed - cmd={e.cmd}, stdout={e.stdout}, stderr={e.stderr}")
            raise e

    @override
    @retry(
        wait=wait_fixed(1),
        stop=stop_after_attempt(5),
        retry=retry_if_result(lambda result: result is False),
        retry_error_callback=lambda _: False,
    )
    def active(self) -> bool:
        try:
            return bool(self.kafka.services[self.service]["active"])
        except KeyError:
            return False

    def install(self) -> bool:
        """Loads the Kafka snap from LP."""
        try:
            self.kafka.ensure(snap.SnapState.Present, revision=CHARMED_KAFKA_SNAP_REVISION)
            self.kafka.connect(plug="removable-media")
            self.kafka.hold()

            return True
        except snap.SnapError as e:
            logger.error(str(e))
            return False

    @override
    def run_bin_command(
        self, bin_keyword: str, bin_args: list[str], opts: list[str] | None = None
    ) -> str:
        if opts is None:
            opts = []
        opts_str = " ".join(opts)
        bin_str = " ".join(bin_args)
        command = f"{opts_str} {SNAP_NAME}.{bin_keyword} {bin_str}"
        return self.exec(command)

    @override
    def mkdir(self, path: str) -> None:
        self.exec(["mkdir", path])

    @override
    def rmdir(self, path: str) -> None:
        self.exec(["rm", "-r", path])

    @override
    def remove(self, path: str, glob: bool = False) -> None:
        if not glob:
            self.exec(["rm", path])
            return

        for file in _glob.glob(path):
            self.exec(["rm", "-rf", file])

    @override
    def dir_exists(self, path: str) -> bool:
        return os.path.isdir(path)

    @override
    def ls(self, path: str) -> list[DirEntry]:
        return [DirEntry(name=f.name, is_dir=f.is_dir()) for f in os.scandir(path)]

    @override
    def check_socket(self, host: str, port: int) -> bool:
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            return sock.connect_ex((host, port)) == 0

    @override
    def set_environment(self, env_vars: Iterable[str]) -> None:
        raw_current_env = self.read(self.paths.env)
        current_env = self.map_env(raw_current_env)

        updated_env = current_env | self.map_env(env_vars)
        content = "\n".join([f"{key}={value}" for key, value in updated_env.items()])
        self.write(content=content + "\n", path=self.paths.env)

    @property
    @override
    def installed(self) -> bool:
        try:
            return bool(self.kafka.services[self.service])
        except (KeyError, snap.SnapNotFoundError):
            return False

    @property
    @override
    def container_can_connect(self) -> bool:
        return True  # Always True on VM

    @property
    @override
    def layer(self) -> pebble.Layer:
        raise NotImplementedError

    @staticmethod
    def map_env(env: Iterable[str]) -> dict[str, str]:
        """Parse env variables into a dict."""
        map_env = {}
        for var in env:
            key = "".join(var.split("=", maxsplit=1)[0])
            value = "".join(var.split("=", maxsplit=1)[1:])
            if key:
                # only check for keys, as we can have an empty value for a variable
                map_env[key] = value
        return map_env
