#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of context objects for the Kafka Connect charm relations, apps and units."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING
from typing_extensions import override

from charms.data_platform_libs.v0.data_interfaces import (
    Data,
    DataPeerData,
    DataPeerUnitData,
    KafkaRequirerData,
    RequirerData,
)
from ops import Object
from ops.model import Application, Relation, Unit

from literals import (
    DEFAULT_SECURITY_MECHANISM,
    KAFKA_CLIENT_REL,
    PEER_REL,
    SUBSTRATE,
    TOPICS,
    Status,
    Substrates,
)

if TYPE_CHECKING:
    from charm import ConnectCharm


class WithStatus(ABC):
    """Abstract base mixin class for objects with status."""

    @property
    @abstractmethod
    def status(self) -> Status:
        """Returns status of the object."""
        ...
    
    @property
    def ready(self) -> bool:
        """Returns True if the status is Active and False otherwise."""
        if self.status == Status.ACTIVE:
            return True
        
        return False


class RelationContext(WithStatus):
    """Relation context object."""

    def __init__(
        self,
        relation: Relation | None,
        data_interface: Data,
        component: Unit | Application | None,
        substrate: Substrates = SUBSTRATE,
    ):
        self.relation = relation
        self.data_interface = data_interface
        self.component = component
        self.substrate = substrate
        self.relation_data = self.data_interface.as_dict(self.relation.id) if self.relation else {}

    def __bool__(self) -> bool:
        """Boolean evaluation based on the existence of self.relation."""
        try:
            return bool(self.relation)
        except AttributeError:
            return False

    def update(self, items: dict[str, str]) -> None:
        """Writes to relation_data."""
        delete_fields = [key for key in items if not items[key]]
        update_content = {k: items[k] for k in items if k not in delete_fields}
        self.relation_data.update(update_content)
        for field in delete_fields:
            del self.relation_data[field]


class KafkaClientContext(RelationContext):
    """Context collection metadata for kafka-client relation."""

    def __init__(
        self,
        relation: Relation | None,
        data_interface: RequirerData,
    ):
        super().__init__(relation, data_interface, None)

    @property
    def username(self) -> str:
        """Returns the Kafka client username."""
        if not self.relation:
            return ""

        return self.relation_data.get("username", "")

    @property
    def password(self) -> str:
        """Returns the Kafka client password."""
        if not self.relation:
            return ""

        return self.relation_data.get("password", "")

    @property
    def bootstrap_servers(self) -> str:
        """Returns Kafka bootstrap servers."""
        if not self.relation:
            return ""

        return self.relation_data.get("endpoints", "")

    @property
    def tls_enabled(self) -> bool:
        """Returns True if TLS is enabled on Kafka-Client relation."""
        if not self.relation:
            return False

        tls = self.relation_data.get("tls")

        if tls is not None and tls != "disabled":
            return True

        return False

    @property
    def security_protocol(self) -> str:
        """Returns the security protocol."""
        return "SASL_PLAINTEXT" if not self.tls_enabled else "SASL_SSL"

    @property
    def security_mechanism(self) -> str:
        """Returns the security mechanism in use."""
        return DEFAULT_SECURITY_MECHANISM

    @property
    @override
    def status(self) -> Status:
        if not self.relation:
            return Status.MISSING_KAFKA

        if not self.bootstrap_servers:
            return Status.NO_KAFKA_CREDENTIALS

        return Status.ACTIVE


class WorkerUnitContext(RelationContext):
    """Context collection metadata for a single Kafka Connect worker unit."""

    def __init__(
        self,
        relation: Relation | None,
        data_interface: DataPeerUnitData,
        component: Unit,
    ):
        super().__init__(relation, data_interface, component)
        self.data_interface = data_interface
        self.unit = component

    @property
    def unit_id(self) -> int:
        """The id of the unit from the unit name."""
        return int(self.unit.name.split("/")[1])

    @property
    def internal_address(self) -> str:
        """The IPv4 address or FQDN of the worker unit."""
        addr = ""
        if self.substrate == "vm":
            for key in ["hostname", "ip", "private-address"]:
                if addr := self.relation_data.get(key, ""):
                    break

        if self.substrate == "k8s":
            addr = f"{self.unit.name.split('/')[0]}-{self.unit_id}.{self.unit.name.split('/')[0]}-endpoints"

        return addr

    @property
    @override
    def status(self) -> Status:
        return Status.ACTIVE


class Context(WithStatus, Object):
    """Context model for the Kafka Connect charm."""

    def __init__(self, charm: "ConnectCharm", substrate: Substrates):
        super().__init__(parent=charm, key="charm_context")
        self.substrate = substrate
        self.config = charm.config

        self.peer_app_interface = DataPeerData(self.model, relation_name=PEER_REL)
        self.peer_unit_interface = DataPeerUnitData(self.model, relation_name=PEER_REL)
        self.kafka_client_interface = KafkaRequirerData(
            self.model,
            relation_name=KAFKA_CLIENT_REL,
            topic=TOPICS["offset"],
            extra_user_roles="admin",
        )

    @property
    def kafka_client(self) -> KafkaClientContext:
        """Returns context of the kafka-client relation."""
        return KafkaClientContext(
            self.model.get_relation(KAFKA_CLIENT_REL), self.kafka_client_interface
        )

    @property
    def worker_unit(self) -> WorkerUnitContext:
        """Returns context of the peer unit relation."""
        return WorkerUnitContext(
            self.model.get_relation(PEER_REL),
            self.peer_unit_interface,
            component=self.model.unit,
        )

    @property
    def tls_enabled(self) -> bool:
        """Returns True if TLS is enabled."""
        # TODO: fix after tls support is added
        return False
    
    @property
    def rest_port(self) -> int:
        """Returns the REST API port."""
        return self.config.rest_port

    @property
    def rest_protocol(self) -> str:
        """Returns the REST API protocol, either `http` or `https`."""
        return "http" if not self.tls_enabled else "https"

    @property
    def status(self) -> Status:
        if not self.kafka_client.ready:
            return self.kafka_client.status

        return Status.ACTIVE
