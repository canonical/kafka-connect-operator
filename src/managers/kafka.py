#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling operations on Kafka cluster."""

import logging
import socket
from contextlib import closing

from core.models import Context
from core.workload import WorkloadBase

logger = logging.getLogger(__name__)


class KafkaManager:
    """Manager for handling Kafka cluster functions."""

    def __init__(self, context: Context, workload: WorkloadBase):
        self.context = context
        self.workload = workload

    def _check_socket(self, host: str, port: int) -> bool:
        """Checks whether an IPv4 socket is healthy or not."""
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            return sock.connect_ex((host, port)) == 0

    def _parse_bootstrap_servers(self, servers) -> list[tuple[str, int]]:
        """Parses bootstrap servers config entry and returns a list of (host, port) tuples."""
        parsed = []
        for server in servers.split(","):
            parts = server.split(":")
            if len(parts) == 2:
                parsed.append((parts[0], int(parts[1])))
        return parsed

    def health_check(self) -> bool:
        """Checks whether relation to Kafka cluster is healthy or not."""
        if not self.context.kafka_client.ready:
            return False

        # checks whether Apache Kafka cluster is accessible
        for host, port in self._parse_bootstrap_servers(
            self.context.kafka_client.bootstrap_servers
        ):
            if not self._check_socket(host, port):
                return False

        return True
