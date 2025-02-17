#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Workload base interface definition."""

import re
import secrets
import string
from abc import ABC, abstractmethod
from typing import Iterable

from ops.pebble import Layer

from literals import CONFIG_DIR, PLUGIN_PATH


class Paths:
    """Object to store common paths for Kafka Connect worker."""

    def __init__(self, config_dir: str = CONFIG_DIR):

        self.config_dir = config_dir

    @property
    def env(self) -> str:
        """Path to environment file."""
        return "/etc/environment"

    @property
    def plugins(self) -> str:
        """Path to plugins folder or storage."""
        return PLUGIN_PATH

    @property
    def worker_properties(self) -> str:
        """Path to distributed connect worker properties file."""
        return f"{self.config_dir}/connect-distributed.properties"

    @property
    def jaas(self) -> str:
        """Path to authentication JAAS config file."""
        return f"{self.config_dir}/jaas.cfg"

    @property
    def keystore(self) -> str:
        """Path to Java Keystore containing service private-key and signed certificates."""
        return f"{self.config_dir}/keystore.p12"

    @property
    def truststore(self):
        """Path to Java Truststore containing trusted CAs + certificates."""
        return f"{self.config_dir}/truststore.jks"

    @property
    def passwords(self) -> str:
        """Path to passwords file store when using PropertyFileLoginModule."""
        return f"{self.config_dir}/connect.password"


class WorkloadBase(ABC):
    """Base interface for common workload operations."""

    paths: Paths = Paths(config_dir=CONFIG_DIR)

    @abstractmethod
    def start(self) -> None:
        """Starts the workload service."""
        ...

    @abstractmethod
    def stop(self) -> None:
        """Stops the workload service."""
        ...

    @abstractmethod
    def restart(self) -> None:
        """Restarts the workload service."""
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
        env: dict[str, str] | None = None,
        working_dir: str | None = None,
        sensitive: bool = False,
    ) -> str:
        """Runs a command on the workload substrate."""
        ...

    @abstractmethod
    def active(self) -> bool:
        """Checks that the workload is active."""
        ...

    @abstractmethod
    def run_bin_command(self, bin_keyword: str, bin_args: list[str], opts: list[str] = []) -> str:
        """Runs kafka bin command with desired args.

        Args:
            bin_keyword: the kafka shell script to run
                e.g `configs`, `topics` etc
            bin_args: the shell command args
            opts: any additional opts args strings

        Returns:
            String of kafka bin command output
        """
        ...

    @abstractmethod
    def mkdir(self, path: str):
        """Creates a new directory at the provided path."""
        ...

    @abstractmethod
    def rmdir(self, path: str):
        """Removes the directory at the provided path."""
        ...

    @abstractmethod
    def remove(self, path: str):
        """Removes the file at the provided path."""
        ...

    @abstractmethod
    def check_socket(self, host: str, port: int) -> bool:
        """Checks whether an IPv4 socket is healthy or not."""
        ...

    @abstractmethod
    def set_environment(self, env_vars: Iterable[str]) -> None:
        """Updates the environment variables with provided iterable of key=value `env_vars`."""

    def get_version(self) -> str:
        """Get the workload version."""
        if not self.active:
            return ""

        try:
            version = re.split(r"[\s\-]", self.run_bin_command("topics", ["--version"]))[0]
        except:  # noqa: E722
            version = ""
        return version

    @property
    @abstractmethod
    def installed(self) -> bool:
        """Whether the workload service is installed or not."""
        ...

    @property
    @abstractmethod
    def layer(self) -> Layer:
        """Gets the Pebble Layer definition for the current workload."""
        ...

    @property
    @abstractmethod
    def container_can_connect(self) -> bool:
        """Flag to check if workload container can connect."""
        ...

    @staticmethod
    def generate_password(length: int = 32) -> str:
        """Creates randomized string of arbitrary `length` (default is 32) for use as app passwords."""
        return "".join(
            [secrets.choice(string.ascii_letters + string.digits) for _ in range(length)]
        )
