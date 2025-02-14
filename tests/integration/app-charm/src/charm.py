#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Test charm for Apache Kafka Connect integrator (requirer-side) functionality testing."""

import logging
import os
import subprocess
from typing import Optional

from charms.data_platform_libs.v0.data_interfaces import DataPeerUnitData, KafkaConnectRequires
from ops.charm import CharmBase, UpdateStatusEvent
from ops.main import main
from ops.model import ActiveStatus, ModelError, Relation

logger = logging.getLogger(__name__)


PORT = 8080
CHARM_KEY = "integrator"
CHARM_DIR = os.environ.get("CHARM_DIR", "")
RESOURCE_PATH = f"{CHARM_DIR}/src/resources/"
PLUGIN_RESOURCE_KEY = "connect-plugin"

SOURCE_REL = "source"
SINK_REL = "sink"
PEER_REL = "peer"


def start_plugin_server(port: int = PORT):
    """Starts a simple HTTP server for test purposes.

    WARNING: this is in no way meant to be used in production code.
    """
    current_dir = os.getcwd()
    os.chdir(RESOURCE_PATH)
    cmd = f"nohup python3 -m http.server {port} &"
    os.system(cmd)
    os.chdir(current_dir)


class TestIntegratorCharm(CharmBase):
    """Test Integrator Charm which implements the `KafkaConnectRequires` interface."""

    def __init__(self, *args):
        super().__init__(*args)
        self.name = CHARM_KEY

        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.update_status, self._update_status)
        self.framework.observe(self.on.config_changed, self._on_config_changed)

        if not self.server_started:
            return

        self.source_requirer = KafkaConnectRequires(self, SOURCE_REL, self.plugin_url)
        self.sink_requirer = KafkaConnectRequires(self, SINK_REL, self.plugin_url)

    @property
    def peer_relation(self) -> Optional[Relation]:
        """Returns the peer Relation object."""
        return self.model.get_relation(PEER_REL)

    @property
    def peer_unit_interface(self) -> DataPeerUnitData:
        """Returns the peer unit DataPeerUnitData interface."""
        return DataPeerUnitData(self.model, relation_name=PEER_REL)

    @property
    def unit_ip(self) -> str:
        """Returns unit's dynamic IP address."""
        return subprocess.check_output("hostname -i", universal_newlines=True, shell=True).strip()

    @property
    def plugin_url(self) -> str:
        """Returns `plugin-url` path."""
        return f"http://{self.unit_ip}:{PORT}/plugin.tar"

    @property
    def server_started(self) -> bool:
        """Returns True if plugin server is started, False otherwise."""
        if self.peer_relation is None:
            return False

        return bool(
            self.peer_unit_interface.fetch_my_relation_field(self.peer_relation.id, "started")
        )

    @server_started.setter
    def server_started(self, val: bool) -> None:
        if self.peer_relation is None:
            return

        if val:
            self.peer_unit_interface.update_relation_data(
                self.peer_relation.id, data={"started": "true"}
            )
        else:
            self.peer_unit_interface.delete_relation_data(
                self.peer_relation.id, fields=["started"]
            )

    def _on_start(self, _) -> None:
        """Handler for `start` event."""
        if self.server_started:
            return

        start_plugin_server(PORT)
        self.server_started = True
        self.unit.status = ActiveStatus()
        logger.info(f"Plugin server started @ {self.plugin_url}")

    def _update_status(self, event: UpdateStatusEvent) -> None:
        """Handler for `update-status` event."""
        if not self.server_started:
            self.on.start.emit()
            return

        self.unit.status = ActiveStatus()

    def _on_config_changed(self, _) -> None:
        """Handler for `config-changed` event."""
        resource_path = None
        try:
            resource_path = self.model.resources.fetch(PLUGIN_RESOURCE_KEY)
            os.system(f"mv {resource_path} {RESOURCE_PATH}/plugin.tar")
        except RuntimeError as e:
            logger.error(f"Resource {PLUGIN_RESOURCE_KEY} not defined in the charm build.")
            raise e
        except (NameError, ModelError) as e:
            logger.error(f"Resource {PLUGIN_RESOURCE_KEY} not found or could not be downloaded.")
            raise e


if __name__ == "__main__":
    main(TestIntegratorCharm)
