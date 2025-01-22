#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Event Handler for Kafka Connect related events."""

import logging
from typing import TYPE_CHECKING

from charms.operator_libs_linux.v2 import snap
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
        super().__init__(charm, "kafka_client")
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
        try:
            self.connect_manager.get_pid()
            self.charm._set_status(Status.ACTIVE)
        except snap.SnapError:
            self.charm._set_status(Status.SERVICE_NOT_RUNNING)

    def _on_config_changed(self, event: ConfigChangedEvent):
        """Handler for `config-changed` event."""
        resource_path = None
        try:
            resource_path = self.model.resources.fetch(PLUGIN_RESOURCE_KEY)
            self.connect_manager.load_plugin(resource_path)
        except Exception as e:
            logger.warning(e)

        current_config = set(self.charm.workload.read(CONFIG_PATH))
        diff = set(self.charm.config_manager.properties) ^ current_config

        if not diff and not resource_path:
            return

        if not self.context.ready_to_start:
            event.defer()
            return

        self.charm.set_properties()
        self.connect_manager.restart_worker()
        self._update_status(event)
