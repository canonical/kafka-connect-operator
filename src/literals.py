#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of globals common to the Kafka Connect Charm."""

from dataclasses import dataclass
from enum import Enum
from typing import Literal

from ops.model import ActiveStatus, BlockedStatus, StatusBase, WaitingStatus

CHARM_KEY = "kafka-connect"
SNAP_NAME = "charmed-kafka"
CHARMED_KAFKA_SNAP_REVISION = "49"
CONTAINER = "kafka-connect"
SUBSTRATE = "vm"

# '584788' refers to snap_daemon, which do not exists on the storage-attached hook prior to the
# snap install.
# FIXME (24.04): From snapd 2.61 onwards, snap_daemon is being deprecated and replaced with _daemon_,
# which now possesses a UID of 584792.
# See https://snapcraft.io/docs/system-usernames.
USER = 584788
GROUP = "root"

GROUP_ID = "connect-cluster"
DEFAULT_SECURITY_MECHANISM = "SCRAM-SHA-512"
DEFAULT_CONVERTER_CLASS = "org.apache.kafka.connect.json.JsonConverter"
DEFAULT_API_PROTOCOL = "http"
DEFAULT_API_PORT = 8083

CONFIG_PATH = f"/var/snap/{SNAP_NAME}/current/etc/connect/connect-distributed.properties"
SERVICE_NAME = "connect-distributed"

TOPICS = {"offset": "connect-offset", "config": "connect-config", "status": "connect-status"}
REPLICATION_FACTOR = -1  # -1 uses broker's default replication factor

# Relations
KAFKA_CLIENT_REL = "kafka-client"
PEER_REL = "worker"

LOG_SENSITIVE_OUTPUT = True  # set False for production mode & builds

DebugLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]
DatabagScope = Literal["unit", "app"]
Substrates = Literal["vm", "k8s"]
ClientModes = Literal["worker", "producer", "consumer"]
Converters = Literal["key", "value"]
InternalTopics = Literal["offset", "config", "status"]


@dataclass
class StatusLevel:
    """Status object helper."""

    status: StatusBase
    log_level: DebugLevel


class Status(Enum):
    """Collection of possible statuses for the charm."""

    SNAP_NOT_INSTALLED = StatusLevel(BlockedStatus(f"unable to install {SNAP_NAME} snap"), "ERROR")
    MISSING_KAFKA = StatusLevel(BlockedStatus("Application needs Kafka client relation"), "DEBUG")
    NO_KAFKA_CREDENTIALS = StatusLevel(
        WaitingStatus("Waiting for Kafka cluster credentials"), "DEBUG"
    )

    ACTIVE = StatusLevel(ActiveStatus(), "DEBUG")
