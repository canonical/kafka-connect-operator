#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Basic implementation of an Integrator for PostgreSQL."""


from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseRequirerData,
    DatabaseRequirerEventHandlers,
)
from charms.kafka_connect.v0.integrator import BaseConfigFormatter, BaseIntegrator
from ops.charm import CharmBase
from typing_extensions import override


class PostgresConfigFormatter(BaseConfigFormatter):
    """Basic implementation for Aiven JDBC Sink connector configuration."""

    # mapping of charm config keys -> JSON config keys
    psql_topic_regex = "topics.regex"
    psql_mode = "insert.mode"

    DEFAULTS = {
        "connector.class": "io.aiven.connect.jdbc.JdbcSinkConnector",
        "topic.creation.default.replication.factor": -1,
        "topic.creation.default.partitions": 10,
        "tasks.max": "1",
        "auto.create": "true",
        "auto.evolve": "true",
        "pk.mode": "none",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    }


class Integrator(BaseIntegrator):
    """Basic implementation for Kafka Connect PostgreSQL Sink Integrator."""

    name = "postgres-sink-integrator"
    formatter = PostgresConfigFormatter
    mode = "sink"

    DB_CLIENT_REL = "sink"
    DB_NAME = "sink_db"

    def __init__(self, charm: CharmBase, plugin_url: str):

        super().__init__(charm=charm, plugin_url=plugin_url)

        self.database_requirer_data = DatabaseRequirerData(
            self.model, self.DB_CLIENT_REL, self.DB_NAME, extra_user_roles="admin"
        )
        self.database = DatabaseRequirerEventHandlers(self.charm, self.database_requirer_data)

    @override
    def setup(self) -> None:
        db = self.helpers.fetch_all_relation_data(self.DB_CLIENT_REL)
        self.configure(
            {
                "connection.url": f"jdbc:postgresql://{db.get('endpoints')}/{self.DB_NAME}",
                "connection.user": db.get("username"),
                "connection.password": db.get("password"),
            }
        )

    @override
    def teardown(self):
        pass

    @property
    @override
    def ready(self):
        return self.helpers.check_data_interfaces_ready([self.DB_CLIENT_REL])
