#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed Machine Operator for Apache Kafka Connect."""

import logging

import ops
from charms.data_platform_libs.v0.data_models import TypedCharmBase
from charms.grafana_agent.v0.cos_agent import COSAgentProvider
from ops import (
    CollectStatusEvent,
    StatusBase,
)

from core.models import Context
from core.structured_config import CharmConfig
from events.connect import ConnectHandler
from events.kafka import KafkaHandler
from events.tls import TLSHandler
from literals import (
    CHARM_KEY,
    JMX_EXPORTER_PORT,
    METRICS_RULES_DIR,
    SNAP_NAME,
    SUBSTRATE,
    DebugLevel,
    Status,
    Substrates,
)
from managers.auth import AuthManager
from managers.config import ConfigManager
from managers.connect import ConnectManager
from managers.tls import TLSManager
from workload import Workload

logger = logging.getLogger(__name__)


class ConnectCharm(TypedCharmBase[CharmConfig]):
    """Charmed Operator for Apache Kafka Connect."""

    config_type = CharmConfig

    def __init__(self, *args):
        super().__init__(*args)
        self.name = CHARM_KEY
        self.substrate: Substrates = SUBSTRATE
        self.pending_inactive_statuses: list[Status] = []

        self.workload = Workload()
        self.context = Context(self, substrate=SUBSTRATE)
        self.auth_manager = AuthManager(
            context=self.context, workload=self.workload, store_path=self.workload.paths.passwords
        )
        self.config_manager = ConfigManager(
            context=self.context, workload=self.workload, config=self.config
        )
        self.connect_manager = ConnectManager(context=self.context, workload=self.workload)
        self.tls_manager = TLSManager(self.context, self.workload, substrate=SUBSTRATE)

        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.remove, self._on_remove)
        self.framework.observe(self.on.collect_unit_status, self._on_collect_status)
        self.framework.observe(self.on.collect_app_status, self._on_collect_status)

        self.connect = ConnectHandler(self)
        self.kafka = KafkaHandler(self)
        self.tls = TLSHandler(self)

        self.cos_agent = COSAgentProvider(
            self,
            metrics_endpoints=[
                # Endpoint for the kafka and jmx exporters
                # See https://github.com/canonical/charmed-kafka-snap for details
                {"path": "/metrics", "port": JMX_EXPORTER_PORT},
            ],
            metrics_rules_dir=METRICS_RULES_DIR,
            log_slots=[f"{SNAP_NAME}:connect-logs"],
        )

    def _on_install(self, _) -> None:
        """Handler for `install` event."""
        if not self.workload.install():
            self._set_status(Status.SNAP_NOT_INSTALLED)
            return

    def _on_start(self, _) -> None:
        if not self.context.kafka_client.relation:
            self._set_status(Status.MISSING_KAFKA)

    def _on_remove(self, _) -> None:
        """Handler for `stop` event."""
        self.workload.stop()

    def _set_status(self, key: Status) -> None:
        """Sets charm status."""
        status: StatusBase = key.value.status
        log_level: DebugLevel = key.value.log_level

        getattr(logger, log_level.lower())(status.message)
        self.pending_inactive_statuses.append(key)

    def _on_collect_status(self, event: CollectStatusEvent):
        """Handler for `collect-status` event."""
        workload_status = Status.INSTALLING if not self.workload.installed else self.context.status
        for status in self.pending_inactive_statuses + [workload_status]:
            event.add_status(status.value.status)


if __name__ == "__main__":
    ops.main(ConnectCharm)  # pyright: ignore[reportCallIssue]
