#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Apache Kafka Connect Integrator Charmed Operator."""

import inspect
import logging

from charms.data_platform_libs.v0.data_interfaces import KafkaConnectRequirerEventHandlers
from charms.data_platform_libs.v0.data_models import BaseConfigModel
from models import Context
from ops.charm import CharmBase, UpdateStatusEvent
from ops.main import main
from ops.model import ActiveStatus, ModelError

from literals import (
    CHARM_DIR,
    CHARM_KEY,
    PLUGIN_FILE_PATH,
    PLUGIN_RESOURCE_KEY,
    REST_PORT,
    SERVICE_PATH,
    SUBSTRATE,
)
from workload import Workload, WorkloadBase

logger = logging.getLogger(__name__)


class CharmConfig(BaseConfigModel):
    """Manager for the structured configuration."""

    pass


class ConfigManager:
    """Manager for handling Integrator REST API service configuration."""

    config: CharmConfig
    workload: WorkloadBase
    context: Context

    def __init__(
        self,
        context: Context,
        workload: WorkloadBase,
        config: CharmConfig,
    ):
        self.context = context
        self.workload = workload
        self.config = config

    @property
    def systemd_config(self) -> str:
        """Returns the Systemd configuration for FastAPI service."""
        return inspect.cleandoc(
            f"""
            [Unit]
            Description=Kafka Connect Integrator REST API
            Wants=network.target
            Requires=network.target

            [Service]
            WorkingDirectory={CHARM_DIR}/src/rest
            EnvironmentFile=-/etc/environment
            Environment=PORT={REST_PORT}
            Environment=PLUGIN_FILE_PATH={PLUGIN_FILE_PATH}
            ExecStart={CHARM_DIR}/venv/bin/python {CHARM_DIR}/src/rest/entrypoint.py
            Restart=always
            Type=simple
        """
        )

    def configure_service(self) -> None:
        """Writes the configuration and prepares the Systemd service to start."""
        self.workload.write(content=self.systemd_config + "\n", path=SERVICE_PATH)
        self.workload.reload()


class IntegratorCharm(CharmBase):
    """Generic Integrator Charm."""

    def __init__(self, *args):
        super().__init__(*args)
        self.name = CHARM_KEY
        self.context = Context(self, substrate=SUBSTRATE)
        self.workload = Workload()
        self.charm_config = CharmConfig()

        self.config_manager = ConfigManager(self.context, self.workload, self.charm_config)

        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.on.update_status, self._update_status)
        self.framework.observe(self.on.config_changed, self._on_config_changed)

        self.source_requirer = KafkaConnectRequirerEventHandlers(
            self, self.context.source_requirer_interface
        )
        self.sink_requirer = KafkaConnectRequirerEventHandlers(
            self, self.context.sink_requirer_interface
        )

    def _on_start(self, _) -> None:
        self.config_manager.configure_service()
        self.workload.start()

        self.unit.status = ActiveStatus()

    def _update_status(self, event: UpdateStatusEvent) -> None:
        if not self.workload.is_active():
            self._on_start(event)
            return

        self.unit.status = ActiveStatus()

    def _on_config_changed(self, _) -> None:
        resource_path = None
        try:
            resource_path = self.model.resources.fetch(PLUGIN_RESOURCE_KEY)
            self.workload.exec(["mv", f"{resource_path}", PLUGIN_FILE_PATH])
        except RuntimeError as e:
            logger.error(f"Resource {PLUGIN_RESOURCE_KEY} not defined in the charm build.")
            raise e
        except (NameError, ModelError) as e:
            logger.error(f"Resource {PLUGIN_RESOURCE_KEY} not found or could not be downloaded.")
            raise e


if __name__ == "__main__":
    main(IntegratorCharm)
