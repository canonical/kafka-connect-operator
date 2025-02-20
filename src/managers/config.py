#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Kafka Connect configuration."""

import inspect
import logging
from typing import cast

from core.models import Context
from core.structured_config import CharmConfig
from core.workload import WorkloadBase
from literals import (
    DEFAULT_AUTH_CLASS,
    DEFAULT_CONVERTER_CLASS,
    GROUP_ID,
    JMX_EXPORTER_PORT,
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
    context: Context

    def __init__(
        self,
        context: Context,
        workload: WorkloadBase,
        config: CharmConfig,
        current_version: str = "",
    ):
        self.context = context
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

        return [
            f"{prefix_}sasl.mechanism={self.context.kafka_client.security_mechanism}",
            f"{prefix_}security.protocol={self.context.kafka_client.security_protocol}",
            f'{prefix_}sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="{username}" password="{password}";',
        ]

    def save_jaas_config(self) -> None:
        """Writes JAAS configuration to `JAAS_PATH`."""
        if not self.jaas_config:
            return

        self.workload.write(content=self.jaas_config + "\n", path=self.workload.paths.jaas)

    def save_properties(self) -> None:
        """Writes all Kafka Connect config properties to the `connect-distributed.properties` path."""
        self.workload.write(
            content="\n".join(self.properties) + "\n", path=self.workload.paths.worker_properties
        )

    def configure(self) -> None:
        """Make all steps necessary to start the Connect service, including setting env vars, JAAS config and service config files."""
        self.workload.set_environment(env_vars=[self.kafka_opts])
        self.save_jaas_config()
        self.save_properties()

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
    def jaas_config(self) -> str:
        """Returns necessary JAAS config for authentication."""
        return inspect.cleandoc(
            f"""
            KafkaConnect {{
                org.apache.kafka.connect.rest.basic.auth.extension.PropertyFileLoginModule required
                file="{self.workload.paths.passwords}";
            }};
            """
        )

    @property
    def jmx_opts(self) -> list[str]:
        """The JMX options for configuring the prometheus exporter."""
        return [
            f"-javaagent:{self.workload.paths.jmx_prometheus_javaagent}={JMX_EXPORTER_PORT}:{self.workload.paths.jmx_prometheus_config}",
        ]

    @property
    def kafka_opts(self) -> str:
        """Returns all necessary options for KAFKA_OPTS env var."""
        opts = [f"-Djava.security.auth.login.config={self.workload.paths.jaas}", *self.jmx_opts]

        return f"KAFKA_OPTS='{' '.join(opts)}'"

    @property
    def client_auth_properties(self) -> list[str]:
        """Returns the list of authentication properties for all client modes."""
        username = self.context.kafka_client.username
        password = self.context.kafka_client.password

        properties = []

        for mode in ("worker", "consumer", "producer"):
            properties.extend(self._add_client(mode=mode, username=username, password=password))

        return properties

    @property
    def rest_auth_properties(self) -> list[str]:
        """Returns authentication config properties on the REST API endpoint."""
        return [f"rest.extension.classes={DEFAULT_AUTH_CLASS}"]

    @property
    def client_tls_properties(self) -> list[str]:
        """Returns the TLS properties for client if TLS is enabled."""
        if not self.context.kafka_client.tls_enabled:
            return []

        return [
            f"ssl.truststore.location={self.workload.paths.truststore}",
            f"ssl.truststore.password={self.context.worker_unit.tls.truststore_password}",
        ]

    @property
    def rest_listener_properties(self) -> list[str]:
        """Returns Listener properties for the REST API endpoint."""
        return [
            f"listeners={self.context.rest_protocol}://{self.context.worker_unit.internal_address}:{self.context.rest_port}",
            f"rest.advertised.listener={self.context.rest_protocol}",
            f"rest.advertised.host.name={self.context.worker_unit.internal_address}",
            f"rest.advertised.host.port={self.context.rest_port}",
        ]

    @property
    def rest_tls_properties(self) -> list[str]:
        """Returns TLS properties for the REST API endpoint."""
        if not self.context.peer_workers.tls_enabled:
            return []

        return [
            "listeners.https.ssl.client.authentication=requested",
            f"listeners.https.ssl.truststore.location={self.workload.paths.truststore}",
            f"listeners.https.ssl.truststore.password={self.context.worker_unit.tls.truststore_password}",
            f"listeners.https.ssl.keystore.location={self.workload.paths.keystore}",
            f"listeners.https.ssl.keystore.password={self.context.worker_unit.tls.keystore_password}",
            "listeners.https.ssl.endpoint.identification.algorithm=HTTPS",
        ]

    @property
    def properties(self) -> list[str]:
        """Returns all properties necessary for starting Kafka Connect service."""
        properties = (
            [
                f"bootstrap.servers={self.context.kafka_client.bootstrap_servers}",
                f"group.id={GROUP_ID}",
                f"plugin.path={self.workload.paths.plugins}",
            ]
            + DEFAULT_CONFIG_OPTIONS.split("\n")
            + self.rest_listener_properties
            + self.rest_tls_properties
            + self.rest_auth_properties
            + self.client_auth_properties
            + self.client_tls_properties
            + self.converter_properties
            + self.topic_properties
        )

        return properties
