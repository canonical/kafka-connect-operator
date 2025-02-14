#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Abstract base Workload interface definition alongside concrete implementation for VM."""


import logging
import os
import subprocess
from abc import ABC, abstractmethod
from typing import Mapping

from charms.operator_libs_linux.v1.systemd import (
    daemon_reload,
    service_disable,
    service_enable,
    service_restart,
    service_running,
)
from ops import Container
from typing_extensions import override

from literals import GROUP, SERVICE_NAME, USER

logger = logging.getLogger(__name__)


class WorkloadBase(ABC):
    """Base interface for common workload operations."""

    service: str

    @abstractmethod
    def start(self) -> bool:
        """Starts the workload service."""
        ...

    @abstractmethod
    def restart(self) -> bool:
        """Retarts the workload service."""
        ...

    @abstractmethod
    def reload(self) -> bool:
        """Reloads the workload service configuration."""
        ...

    @abstractmethod
    def enable(self) -> bool:
        """Enables the workload service."""
        ...

    @abstractmethod
    def disable(self) -> bool:
        """Disables the workload service."""
        ...

    @abstractmethod
    def read(self, path: str) -> list[str]:
        """Reads a file from the workload.

        Args:
            path: the full filepath to read from

        Returns:
            List of string lines from the specified path
        """
        ...

    @abstractmethod
    def write(self, content: str, path: str, mode: str = "w") -> None:
        """Writes content to a workload file.

        Args:
            content: string of content to write
            path: the full filepath to write to
            mode: the write mode. Usually "w" for write, or "a" for append. Default "w"
        """
        ...

    @abstractmethod
    def exec(
        self,
        command: list[str] | str,
        env: Mapping[str, str] | None = None,
        working_dir: str | None = None,
    ) -> str | None:
        """Executes a command on the workload substrate.

        Returns None if the command failed to be executed.
        """
        ...

    @abstractmethod
    def is_active(self) -> bool:
        """Checks that the workload is active."""
        ...

    def remove(self, path: str) -> None:
        """Remove the specified file."""
        os.remove(path)


class Workload(WorkloadBase):
    """Wrapper for performing common operations specific to the Integrator charm systemd service."""

    service: str = SERVICE_NAME

    def __init__(self, container: Container | None = None) -> None:
        self.container = container

    @override
    def start(self) -> bool:
        return service_restart(self.service)

    @override
    def restart(self) -> bool:
        return service_restart(self.service)

    @override
    def reload(self) -> bool:
        return daemon_reload()

    def enable(self) -> bool:
        """Enables service."""
        return service_enable(self.service)

    def disable(self) -> bool:
        """Disables service."""
        return service_disable(self.service)

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
    ) -> str:
        try:
            output = subprocess.check_output(
                command,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                shell=isinstance(command, str),
                env=env,
                cwd=working_dir,
            )
            logger.debug(f"{output=}")
            return output
        except subprocess.CalledProcessError as e:
            logger.error(f"cmd failed - cmd={e.cmd}, stdout={e.stdout}, stderr={e.stderr}")
            raise e

    @override
    def is_active(self) -> bool:
        """Checks that the workload is active."""
        return service_running(self.service)
