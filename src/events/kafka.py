#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""KafkaHandler class and methods."""

import logging
from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.data_interfaces import (
    KafkaRequirerEventHandlers,
)
from ops.charm import RelationBrokenEvent, RelationChangedEvent, RelationCreatedEvent
from ops.framework import Object

from literals import CONFIG_PATH, KAFKA_CLIENT_REL, Status
from managers.kafka import KafkaManager

if TYPE_CHECKING:
    from charm import ConnectCharm


logger = logging.getLogger(__name__)


class KafkaHandler(Object):
    """..."""

    def __init__(self, charm: "ConnectCharm") -> None:
        super().__init__(charm, "kafka_client")
        self.charm: "ConnectCharm" = charm
        self.state = charm.state

        self.kafka_manager = KafkaManager(state=self.state, workload=charm.workload)
        self.event_handler = KafkaRequirerEventHandlers(charm, self.state.kafka_client_interface)

        self.framework.observe(
            self.charm.on[KAFKA_CLIENT_REL].relation_created, self._on_relation_created
        )
        self.framework.observe(
            self.charm.on[KAFKA_CLIENT_REL].relation_broken, self._on_relation_broken
        )
        self.framework.observe(
            self.charm.on[KAFKA_CLIENT_REL].relation_changed, self._on_relation_changed
        )

        self.framework.observe(getattr(self.charm.on, "update_status"), self._on_update_status)
        # self.framework.observe(self.event_handler.on.topic_created, self.on_topic_requested)

    def _on_update_status(self, _):
        """Handler for `update-status` event."""
        if not self.state.kafka_client.relation:
            self.charm._set_status(Status.MISSING_KAFKA)

    def _on_relation_created(self, event: RelationCreatedEvent) -> None:
        """Handler for `kafka-client-relation-created` event."""
        if not self.kafka_manager.health_check():
            self.charm._set_status(Status.NO_KAFKA_CREDENTIALS)
            event.defer()
            return

        self._restart_worker()
        self.charm._set_status(Status.ACTIVE)

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        """Handler for `kafka-client-relation-changed` event."""
        current_config = set(self.charm.workload.read(CONFIG_PATH))
        diff = set(self.charm.config_manager.properties) ^ current_config

        if len(diff) == 0:
            return

        self._restart_worker()

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Handler for `kafka-client-relation-broken` event."""
        self.charm._set_status(Status.MISSING_KAFKA)
        self.charm.workload.stop()

    def _restart_worker(self):
        """Attempts to re-configure and restart the connect worker."""
        self.charm.config_manager.set_properties()
        self.charm.workload.restart()
