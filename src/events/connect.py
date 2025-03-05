#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Event Handler for Kafka Connect related events."""

import datetime
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
        self.unit_tls_context = self.context.worker_unit.tls

        self.framework.observe(getattr(self.charm.on, "update_status"), self._update_status)
        self.framework.observe(getattr(self.charm.on, "config_changed"), self._on_config_changed)
        self.framework.observe(self.charm.on[PEER_REL].relation_changed, self._on_config_changed)

        # instantiate the provider
        self.provider = ConnectProvider(self.charm)
        self.framework.observe(self.charm.on[PEER_REL].relation_changed, self._on_config_changed)

    def _update_status(self, event: EventBase) -> None:
        """Handler for `update-status` event."""
        if self.charm.connect_manager.health_check():
            self.charm._set_status(Status.ACTIVE)
            self.charm.unit.set_ports(self.context.rest_port)
        elif self.context.ready:
            self.charm._set_status(Status.SERVICE_NOT_RUNNING)
        else:
            self.charm._set_status(self.context.status)

        if isinstance(event, ConfigChangedEvent):
            return

        # for plugins update if needed
        self.charm.on.config_changed.emit()

    def _on_config_changed(self, event: ConfigChangedEvent) -> None:
        """Handler for `config-changed` event."""
        if not self.charm.connect_manager.plugin_path_initiated:
            self.charm.connect_manager.init_plugin_path()

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

        if self.context.peer_workers.tls_enabled and self.charm.tls_manager.sans_change_detected:
            self.charm.tls.certificates.on.certificate_expiring.emit(
                certificate=self.unit_tls_context.certificate,
                expiry=datetime.datetime.now().isoformat(),
            )
            self.charm.context.worker_unit.update(
                {self.unit_tls_context.CERT: ""}
            )  # ensures only single requested new certs, will be replaced on new certificate-available event

            return  # config-changed would be eventually fired on certificate-available, so no need to defer.

        current_config = set(self.charm.workload.read(self.workload.paths.worker_properties))
        diff = set(self.charm.config_manager.properties) ^ current_config

        if not any([diff, self.context.worker_unit.should_restart]):
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
                    "tls": "enabled" if self.context.peer_workers.tls_enabled else "disabled",
                    "tls-ca": self.context.worker_unit.tls.ca
                    if self.context.peer_workers.tls_enabled
                    else "disabled",
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
