#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling operations on Kafka cluster."""

import logging

from core.models import Context
from core.workload import WorkloadBase

logger = logging.getLogger(__name__)


class KafkaManager:
    """Manager for handling Kafka cluster functions."""

    def __init__(self, context: Context, workload: WorkloadBase):
        self.context = context
        self.workload = workload

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
            if not self.workload.check_socket(host, port):
                return False

        return True
