#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Kafka Connect workload class and methods."""

import logging
import os
import socket
import subprocess
from contextlib import closing
from typing import Mapping

from charms.operator_libs_linux.v2 import snap
from ops import Container, pebble
from tenacity import retry, retry_if_result, stop_after_attempt, wait_fixed
from typing_extensions import override

from core.workload import WorkloadBase
from literals import (
    CHARMED_KAFKA_SNAP_REVISION,
    GROUP,
    LOG_SENSITIVE_OUTPUT,
    SERVICE_NAME,
    SNAP_NAME,
    USER,
)

logger = logging.getLogger(__name__)


class Workload(WorkloadBase):
    """Wrapper for performing common operations specific to the Kafka Connect Snap."""

    service: str

    def __init__(self, container: Container | None = None) -> None:
        self.container = container
        self.kafka = snap.SnapCache()[SNAP_NAME]
        self.service = SERVICE_NAME

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
    def write(self, content: str, path: str, mode: str = "w") -> None:
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
        sensitive: bool = False,
    ) -> str:
        should_log = not sensitive or LOG_SENSITIVE_OUTPUT
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
    def mkdir(self, path: str):
        self.exec(["mkdir", path])

    @override
    def rmdir(self, path: str):
        self.exec(["rm", "-r", path])

    @override
    def check_socket(self, host: str, port: int) -> bool:
        """Checks whether an IPv4 socket is healthy or not."""
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            return sock.connect_ex((host, port)) == 0

    @property
    @override
    def container_can_connect(self) -> bool:
        return True  # Always True on VM

    @property
    @override
    def layer(self) -> pebble.Layer:
        raise NotImplementedError
