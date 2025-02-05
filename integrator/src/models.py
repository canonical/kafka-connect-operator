#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of context objects for the Integrator charm relations, apps and units."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.data_interfaces import (
    Data,
    DataPeerUnitData,
    KafkaConnectRequirerData,
)
from ops import ActiveStatus, Object, StatusBase
from ops.model import Application, Relation, Unit
from typing_extensions import override

from literals import PEER_REL, REST_PORT, SINK_REL, SOURCE_REL, SUBSTRATE, Substrates

if TYPE_CHECKING:
    from charm import IntegratorCharm


ACTIVE = ActiveStatus()


class WithStatus(ABC):
    """Abstract base mixin class for objects with status."""

    @property
    @abstractmethod
    def status(self) -> StatusBase:
        """Returns status of the object."""
        ...

    @property
    def ready(self) -> bool:
        """Returns True if the status is Active and False otherwise."""
        if self.status == ACTIVE:
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


class UnitContext(RelationContext):
    """Context collection metadata for a single unit."""

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
    def status(self) -> StatusBase:
        return ActiveStatus()


class Context(WithStatus, Object):
    """Context model for the Integrator charm."""

    def __init__(self, charm: "IntegratorCharm", substrate: Substrates):
        super().__init__(parent=charm, key="charm_context")
        self.substrate = substrate
        self.config = charm.config

        self.peer_unit_interface = DataPeerUnitData(self.model, relation_name=PEER_REL)

        self.source_requirer_interface = KafkaConnectRequirerData(
            self.model, relation_name=SOURCE_REL, plugin_url=self.plugin_url
        )

        self.sink_requirer_interface = KafkaConnectRequirerData(
            self.model, relation_name=SINK_REL, plugin_url=self.plugin_url
        )

    @property
    def unit(self) -> UnitContext:
        """Returns context of the peer unit relation."""
        return UnitContext(
            self.model.get_relation(PEER_REL),
            self.peer_unit_interface,
            component=self.model.unit,
        )

    @property
    def plugin_url(self) -> str:
        """Returns the plugin serving URL."""
        return f"http://{self.unit.internal_address}:{REST_PORT}/api/v1/plugin"

    @property
    @override
    def status(self) -> StatusBase:
        return ACTIVE
