#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of context objects for the Kafka Connect charm relations, apps and units."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.data_interfaces import (
    Data,
    DataPeerData,
    DataPeerOtherUnitData,
    DataPeerUnitData,
    KafkaConnectProviderData,
    KafkaRequirerData,
    ProviderData,
    RequirerData,
)
from ops import Object
from ops.model import Application, Relation, RelationDataAccessError, Unit
from typing_extensions import override

from literals import (
    CLIENT_REL,
    DEFAULT_API_PORT,
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

    def _fetch_from_secrets(self, field) -> str:
        """Fetches a field from secrets defined at the remote unit of the relation."""
        if not self.relation or not self.relation.units:
            return ""

        remote_unit = next(iter(self.relation.units))

        try:
            return self.data_interface._fetch_relation_data_with_secrets(
                remote_unit, [field], self.relation
            ).get(field, "")
        except RelationDataAccessError:
            # remote unit has not the secrets yet.
            pass

        return ""


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


class ConnectClientContext(RelationContext):
    """Context collection metadata for kafka-client relation."""

    def __init__(
        self,
        relation: Relation | None,
        data_interface: ProviderData,
    ):
        super().__init__(relation, data_interface, None)

    @property
    def plugin_url(self) -> str:
        """Returns the client's plugin-url REST endpoint."""
        if not self.relation:
            return ""

        return self.relation_data.get("plugin-url", "")

    @property
    def username(self) -> str:
        """Returns the Kafka client username."""
        if not self.relation:
            return ""

        return f"relation-{self.relation.id}"

    @property
    def password(self) -> str:
        """Returns the Kafka client password."""
        if not self.relation:
            return ""

        return self._fetch_from_secrets("password")

    @property
    @override
    def status(self) -> Status:
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

    def get_rest_endpoint(self, protocol: str = "http", port: int = DEFAULT_API_PORT) -> str:
        """Returns the REST endpoint of the unit.

        Args:
            protocol (str, optional): REST protocol. Defaults to "http".
            port (int, optional): REST port. Defaults to DEFAULT_API_PORT.
        """
        return f"{protocol}://{self.internal_address}:{port}"


class PeerWorkersContext(RelationContext):
    """Context collection metadata for Kafka Connect peer relation."""

    ADMIN_USERNAME = "admin"
    ADMIN_PASSWORD = "admin-password"

    def __init__(self, relation, data_interface):
        super().__init__(relation, data_interface, None)

    @property
    def admin_password(self) -> str:
        """Internal admin user's password."""
        if not self.relation:
            return ""

        return self.relation_data.get(self.ADMIN_PASSWORD, "")

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
        self.peer_app_interface = DataPeerData(
            self.model,
            relation_name=PEER_REL,
            additional_secret_fields=[PeerWorkersContext.ADMIN_PASSWORD],
        )
        self.peer_unit_interface = DataPeerUnitData(self.model, relation_name=PEER_REL)
        self.kafka_client_interface = KafkaRequirerData(
            self.model,
            relation_name=KAFKA_CLIENT_REL,
            topic=TOPICS["offset"],
            extra_user_roles="admin",
        )
        self.connect_provider_interface = KafkaConnectProviderData(
            self.model, relation_name=CLIENT_REL
        )

    @property
    def peer_relation(self) -> Relation | None:
        """The Kafka connect workers peer relation."""
        return self.model.get_relation(PEER_REL)

    @property
    def units(self) -> set[WorkerUnitContext]:
        """Returns a set of peer units' WorkerUnitContext."""
        _set = {self.worker_unit}

        if not self.peer_relation or not self.peer_relation.units:
            return _set

        return _set | {
            WorkerUnitContext(
                relation=self.peer_relation,
                data_interface=DataPeerOtherUnitData(
                    model=self.model, unit=unit, relation_name=PEER_REL
                ),
                component=unit,
            )
            for unit in self.peer_relation.units
        }

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
    def peer_workers(self) -> PeerWorkersContext:
        """Returns the context of peer app relation."""
        return PeerWorkersContext(
            self.model.get_relation(PEER_REL),
            self.peer_app_interface,
        )

    @property
    def client_relations(self) -> set[Relation]:
        """The relations of all client applications."""
        return set(self.model.relations[CLIENT_REL])

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
    def rest_uri(self) -> str:
        """Returns the REST API base URI of current unit."""
        return self.worker_unit.get_rest_endpoint(protocol=self.rest_protocol, port=self.rest_port)

    @property
    def rest_endpoints(self) -> str:
        """Returns all Kafka Connect REST endpoints available on the cluster."""
        return ",".join(
            [
                unit_context.get_rest_endpoint(protocol=self.rest_protocol, port=self.rest_port)
                for unit_context in self.units
            ]
        )

    @property
    def clients(self) -> dict[int, ConnectClientContext]:
        """Returns a mapping of integrator relation-id to the related client context."""
        clients = {}
        for relation in self.client_relations:
            if not relation.app:
                continue

            clients[relation.id] = ConnectClientContext(
                relation=relation, data_interface=self.connect_provider_interface
            )

        return clients

    @property
    def credentials(self) -> dict[str, str]:
        """Returns a dict of all active `username: password`s on the Kafka Connect cluster."""
        cache = {}
        if self.peer_workers.admin_password:
            cache[self.peer_workers.ADMIN_USERNAME] = self.peer_workers.admin_password

        for client in self.clients.values():
            cache[client.username] = client.password

        return cache

    @property
    @override
    def status(self) -> Status:
        if not self.kafka_client.ready:
            return self.kafka_client.status

        return Status.ACTIVE
