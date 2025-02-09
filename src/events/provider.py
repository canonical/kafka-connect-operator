#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Event Handler for Connect Client Provider events."""

import logging
from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.data_interfaces import (
    IntegrationRequestedEvent,
    KafkaConnectProviderEventHandlers,
)
from ops.charm import (
    RelationBrokenEvent,
    RelationChangedEvent,
    RelationJoinedEvent,
)
from ops.framework import Object

from literals import CLIENT_REL
from managers.connect import PluginDownloadFailedError

if TYPE_CHECKING:
    from charm import ConnectCharm


logger = logging.getLogger(__name__)


class ConnectProvider(Object):
    """Handler for events related to Kafka Connect client interface provider."""

    def __init__(self, charm: "ConnectCharm") -> None:
        super().__init__(charm, "connect-worker")
        self.charm: "ConnectCharm" = charm
        self.context = charm.context
        self.workload = charm.workload

        self.connect_provider = KafkaConnectProviderEventHandlers(
            self.charm, self.context.connect_provider_interface
        )

        self.framework.observe(self.charm.on[CLIENT_REL].relation_broken, self._on_relation_broken)
        self.framework.observe(
            self.charm.on[CLIENT_REL].relation_changed, self._on_relation_changed
        )
        self.framework.observe(self.charm.on[CLIENT_REL].relation_joined, self._on_relation_joined)
        self.framework.observe(
            getattr(self.connect_provider.on, "integration_requested"),
            self._on_integration_requested,
        )

    def _on_integration_requested(self, event: IntegrationRequestedEvent) -> None:
        """Handle the `integration_requested` event, fired after an integrator relates, boots up and sets the `plugin-url`."""
        client = self.context.clients.get(event.relation.id)

        if not self.context.peer_workers or client is None:
            event.defer()
            return

        if not client.plugin_url:
            event.defer()
            return

        try:
            self.charm.connect_manager.load_plugin_from_url(
                client.plugin_url, path_prefix=client.username
            )
        except PluginDownloadFailedError as e:
            logger.error(f"Unable to fetch the plugin: {e}")
            return

        if not self.charm.unit.is_leader():
            return

        if not client.password:
            password = self.workload.generate_password()
            self.context.connect_provider_interface.set_credentials(
                event.relation.id, username=client.username, password=password
            )
            self.context.connect_provider_interface.set_endpoints(
                event.relation.id, self.context.rest_endpoints
            )

        # update credentials
        self.charm.auth_manager.update(credentials={client.username: client.password})

        self.context.worker_unit.should_restart = True
        self.charm.on.config_changed.emit()

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        """Handler for `connect-client-relation-changed` event."""
        client = self.context.clients.get(event.relation.id)
        if client is None or not client.password:
            # wait for leader to create the credentials
            event.defer()
            return

        self.charm.auth_manager.update(credentials=self.context.credentials)

        self.context.worker_unit.should_restart = True
        self.charm.on.config_changed.emit()

    def _on_relation_joined(self, _: RelationJoinedEvent) -> None:
        """Handler for `connect-client-relation-joined` event."""
        self.charm.on.config_changed.emit()

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Handler for `connect-client-relation-broken` event."""
        username = f"relation-{event.relation.id}"
        self.charm.auth_manager.remove_user(username)
        self.charm.connect_manager.remove_plugin(path_prefix=username)

        self.context.worker_unit.should_restart = True
        self.charm.on.config_changed.emit()
