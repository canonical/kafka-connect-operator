#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Event Handler for Kafka Connect related events."""

import logging
from typing import TYPE_CHECKING

from ops import ModelError
from ops.charm import (
    ConfigChangedEvent,
)
from ops.framework import EventBase, Object

from events.provider import ConnectProvider
from literals import PEER_REL, PLUGIN_RESOURCE_KEY, Status
from managers.connect import PluginDownloadFailedError

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

        self.framework.observe(getattr(self.charm.on, "update_status"), self._update_status)
        self.framework.observe(getattr(self.charm.on, "config_changed"), self._on_config_changed)
        self.framework.observe(self.charm.on[PEER_REL].relation_changed, self._on_config_changed)

        # instantiate the provider
        self.provider = ConnectProvider(self.charm)

    def _update_status(self, event: EventBase):
        """Handler for `update-status` event."""
        if self.charm.connect_manager.health_check():
            self.charm._set_status(Status.ACTIVE)
        elif self.context.ready:
            self.charm._set_status(Status.SERVICE_NOT_RUNNING)
        else:
            self.charm._set_status(self.context.status)

        if isinstance(event, ConfigChangedEvent):
            return

        # for plugins update if needed
        self.charm.on.config_changed.emit()

    def _on_config_changed(self, event: ConfigChangedEvent):
        """Handler for `config-changed` event."""
        if not self.charm.connect_manager.plugin_path_initiated:
            self.charm.connect_manager.init_plugin_path()

        resource_path = None
        try:
            resource_path = self.model.resources.fetch(PLUGIN_RESOURCE_KEY)
            self.charm.connect_manager.load_plugin(resource_path)
        except RuntimeError:
            logger.error(f"Resource {PLUGIN_RESOURCE_KEY} not defined in the charm build.")
        except (NameError, ModelError):
            logger.debug(
                f"Resource {PLUGIN_RESOURCE_KEY} not found or could not be downloaded, skipping plugin loading."
            )

        self.update_plugins()
        self.update_clients_data()

        current_config = set(self.charm.workload.read(self.workload.paths.worker_properties))
        diff = set(self.charm.config_manager.properties) ^ current_config

        if not diff and not resource_path and not self.context.worker_unit.should_restart:
            return

        if not self.context.ready:
            self.charm._set_status(self.context.status)
            event.defer()
            return

        self.enable_auth()
        self.charm.config_manager.configure()
        self.charm.connect_manager.restart_worker()

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

    def update_clients_data(self) -> None:
        """Updates all clients with latest relation data."""
        if not self.charm.unit.is_leader():
            return

        loaded_plugins = self.charm.connect_manager.loaded_client_plugins

        for client in self.context.clients.values():
            if not client.password:
                logger.debug(
                    f"Skipping update of {client.username}, user has not yet been added..."
                )
                continue

            if client.username not in loaded_plugins:
                continue

            if set(client.endpoints.split(",")) == set(self.context.rest_endpoints.split(",")):
                continue

            client.update(
                {
                    "endpoints": self.context.rest_endpoints,
                    "username": client.username,
                    "password": client.password,
                    # "tls": self.context.peer_workers.tls_enabled,
                    # "tls-ca": self.context.worker_unit.tls.ca
                }
            )

    def update_plugins(self) -> None:
        """Attempts to update client plugins on this worker."""
        loaded_clients = self.charm.connect_manager.loaded_client_plugins
        update_set = set()

        for client in self.context.clients.values():
            if client.username in loaded_clients:
                continue

            try:
                self.charm.connect_manager.load_plugin_from_url(
                    client.plugin_url, path_prefix=client.username
                )
            except PluginDownloadFailedError as e:
                logger.warning(f"Unable to fetch the plugin for {client.username}: {e}")
                continue

            update_set.add(client)

        if update_set:
            self.context.worker_unit.should_restart = True
