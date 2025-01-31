#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Event Handler for Kafka Connect related events."""

import logging
from typing import TYPE_CHECKING

from ops import ModelError
from ops.charm import ConfigChangedEvent, InstallEvent
from ops.framework import EventBase, Object

from literals import CONFIG_PATH, PLUGIN_RESOURCE_KEY, Status
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
        self.framework.observe(getattr(self.charm.on, "install"), self._on_install)

    def _on_install(self, event: InstallEvent) -> None:
        """Handler for `install` event."""
        if not self.connect_manager.init_plugin_path():
            event.defer()
            return

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

        current_config = set(self.charm.workload.read(CONFIG_PATH))
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
        self.charm.auth_manager.update(
            credentials={
                self.context.peer_workers.ADMIN_USERNAME: self.context.peer_workers.admin_password
            }
        )
