#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Event Handler for Kafka Connect related events."""

import logging
from subprocess import CalledProcessError
from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.data_interfaces import (
    KafkaConnectProviderEventHandlers,
)
from ops import ModelError
from ops.charm import (
    ConfigChangedEvent,
    RelationBrokenEvent,
    RelationChangedEvent,
    RelationJoinedEvent,
)
from ops.framework import EventBase, Object

from literals import CLIENT_REL, PLUGIN_RESOURCE_KEY, Status
from managers.connect import ConnectManager

if TYPE_CHECKING:
    from charm import ConnectCharm


logger = logging.getLogger(__name__)


class ConnectHandler(Object):
    """Handler for events related to Kafka Connect worker."""

    def __init__(self, charm: "ConnectCharm") -> None:
        super().__init__(charm, "connect-worker")
        self.charm: "ConnectCharm" = charm
        self.context = charm.context
        self.workload = charm.workload

        self.connect_manager = ConnectManager(context=self.context, workload=charm.workload)

        self.framework.observe(getattr(self.charm.on, "update_status"), self._update_status)
        self.framework.observe(getattr(self.charm.on, "config_changed"), self._on_config_changed)

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

    def _on_integration_requested(self, event):
        """Handle the `integration_requested` event, fired after an integrator relates, boots up and sets the `plugin-url`."""
        client = self.context.clients.get(event.relation.id)

        if not self.context.peer_workers or client is None:
            event.defer()
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

        # TODO: rolling restart maybe?
        self.connect_manager.restart_worker()

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        """Handler for `connect-client-relation-changed` event."""
        client = self.context.clients.get(event.relation.id)
        if client is None or not client.password:
            # wait for leader to create the credentials
            event.defer()
            return

        if self.charm.unit.is_leader():
            self.context.connect_provider_interface.set_endpoints(
                event.relation.id, self.context.rest_endpoints
            )

        self.charm.auth_manager.update(credentials=self.context.credentials)
        self.connect_manager.restart_worker()

    def _on_relation_joined(self, _: RelationJoinedEvent) -> None:
        """Handler for `connect-client-relation-joined` event."""
        # No need to add users here since it's handled in `config-changed` for newcomers, and `relation-changed` for existing units
        for client in self.context.clients.values():
            try:
                self.connect_manager.load_plugin_from_url(
                    client.plugin_url, path_prefix=client.username
                )
            except CalledProcessError as e:
                if "File exists" in e.stderr:
                    continue
                raise e

        self.connect_manager.restart_worker()

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Handler for `connect-client-relation-broken` event."""
        username = f"relation-{event.relation.id}"
        self.charm.auth_manager.remove_user(username)
        self.connect_manager.remove_plugin(path_prefix=username)
        # TODO: rolling restart maybe?
        self.connect_manager.restart_worker()

    def _update_status(self, event: EventBase):
        """Handler for `update-status` event."""
        if self.connect_manager.health_check():
            self.charm._set_status(Status.ACTIVE)
        elif self.context.ready:
            self.charm._set_status(Status.SERVICE_NOT_RUNNING)
        else:
            self.charm._set_status(self.context.status)

    def _on_config_changed(self, event: ConfigChangedEvent):
        """Handler for `config-changed` event."""
        if not self.connect_manager.plugin_path_initiated:
            self.connect_manager.init_plugin_path()

        resource_path = None
        try:
            resource_path = self.model.resources.fetch(PLUGIN_RESOURCE_KEY)
            self.connect_manager.load_plugin(resource_path)
        except RuntimeError:
            logger.error(f"Resource {PLUGIN_RESOURCE_KEY} not defined in the charm build.")
        except (NameError, ModelError):
            logger.debug(
                f"Resource {PLUGIN_RESOURCE_KEY} not found or could not be downloaded, skipping plugin loading."
            )

        current_config = set(self.charm.workload.read(self.workload.paths.worker_properties))
        diff = set(self.charm.config_manager.properties) ^ current_config

        if not diff and not resource_path:
            return

        if not self.context.ready:
            self.charm._set_status(self.context.status)
            event.defer()
            return

        self.enable_auth()
        self.charm.config_manager.configure()
        self.connect_manager.restart_worker()

        self._update_status(event)

    def enable_auth(self) -> None:
        """Sets up authentication, including admin user credentials, and initiates internal credential stores on peer worker units."""
        if self.charm.unit.is_leader() and not self.context.peer_workers.admin_password:
            # create admin password
            admin_password = self.workload.generate_password()
            self.context.peer_workers.update(
                {self.context.peer_workers.ADMIN_PASSWORD: admin_password}
            )

        # Update internal credentials store
        self.charm.auth_manager.update(credentials=self.context.credentials)
