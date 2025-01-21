#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Kafka Connect configuration."""

import logging
from typing import cast

from core.models import GlobalState
from core.structured_config import CharmConfig
from core.workload import WorkloadBase
from literals import (
    CONFIG_PATH,
    DEFAULT_CONVERTER_CLASS,
    GROUP_ID,
    REPLICATION_FACTOR,
    TOPICS,
    ClientModes,
    Converters,
    InternalTopics,
)

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_OPTIONS = """
offset.flush.interval.ms=10000
key.converter.schemas.enable=false
value.converter.schemas.enable=false
"""


class ConfigManager:
    """Manager for handling Kafka Connect configuration."""

    config: CharmConfig
    workload: WorkloadBase
    state: GlobalState

    def __init__(
        self,
        state: GlobalState,
        workload: WorkloadBase,
        config: CharmConfig,
        current_version: str = "",
    ):
        self.state = state
        self.workload = workload
        self.config = config
        self.current_version = current_version

    def _add_converter(
        self, converter_mode: Converters, converter_class: str = DEFAULT_CONVERTER_CLASS
    ) -> str:
        """Returns key=value configuration entry for a given converter."""
        return f"{converter_mode}.converter={converter_class}"

    def _add_topic(
        self, mode: str, topic_name: InternalTopics, replication_factor: int = -1
    ) -> list[str]:
        """Returns a list of key=value configuration entries for a given internal topic."""
        return [
            f"{mode}.storage.topic={topic_name}",
            f"{mode}.storage.replication.factor={replication_factor}",
        ]

    def _add_client(self, mode: ClientModes, username: str, password: str) -> list[str]:
        """Returns a list of key=value configuration entries for a given kafka client."""
        prefix_ = "" if mode == "worker" else f"{mode}."

        properties = [
            f"{prefix_}sasl.mechanism={self.state.kafka_client.security_mechanism}",
            f"{prefix_}security.protocol={self.state.kafka_client.security_protocol}",
            f'{prefix_}sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="{username}" password="{password}";',
        ]

        return properties

    @property
    def converter_properties(self) -> list[str]:
        """Returns the list of configuration for all converters."""
        properties = []
        for converter in ("key", "value"):
            properties.append(self._add_converter(converter_mode=converter))
        return properties

    @property
    def topic_properties(self) -> list[str]:
        """Returns the list of configuration for all internal topics."""
        properties = []
        for mode, topic_name in TOPICS.items():
            properties.extend(
                self._add_topic(
                    mode,
                    topic_name=cast(InternalTopics, topic_name),
                    replication_factor=REPLICATION_FACTOR,
                )
            )
        return properties

    @property
    def client_properties(self) -> list[str]:
        """Returns the list of properties for all client modes."""
        username = self.state.kafka_client.username
        password = self.state.kafka_client.password

        properties = []

        for mode in ("worker", "consumer", "producer"):
            properties.extend(self._add_client(mode=mode, username=username, password=password))

        return properties

    @property
    def listeners(self) -> str:
        """Listener(s) for the REST API endpoint."""
        return f"{self.state.rest_protocol}://{self.state.worker_unit.internal_address}:{self.config.rest_port}"

    @property
    def properties(self) -> list[str]:
        """Returns all properties necessary for starting Kafka Connect service."""
        properties = (
            [
                f"bootstrap.servers={self.state.kafka_client.bootstrap_servers}",
                f"group.id={GROUP_ID}",
                f"listeners={self.listeners}",
            ]
            + DEFAULT_CONFIG_OPTIONS.split("\n")
            + self.client_properties
            + self.converter_properties
            + self.topic_properties
        )

        return properties

    def set_properties(self) -> None:
        """Writes all Kafka Connect config properties to the `connect-distributed.properties` path."""
        self.workload.write(content="\n".join(self.properties) + "\n", path=CONFIG_PATH)
