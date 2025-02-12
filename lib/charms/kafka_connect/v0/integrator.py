#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Requirer-side event handling and charm config management for Kafka Connect integrator charms."""

import json
import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping, MutableMapping
from enum import Enum
from functools import cached_property
from typing import Any, Iterable, Literal, Optional

import requests
from charms.data_platform_libs.v0.data_interfaces import (
    DataPeerUnitData,
    IntegrationCreatedEvent,
    IntegrationEndpointsChangedEvent,
    KafkaConnectRequirerData,
    KafkaConnectRequirerEventHandlers,
    RequirerData,
)
from ops.charm import CharmBase, RelationBrokenEvent
from ops.framework import Object
from ops.model import ConfigData, Relation
from requests.auth import HTTPBasicAuth


# The unique Charmhub library identifier, never change it
LIBID = "77777777777777777777777777777777"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1


logger = logging.getLogger(__name__)


class ConnectClientError(Exception):
    """Exception raised when Kafka Connect client could not be instantiated."""


class ConnectApiError(Exception):
    """Exception raised when Kafka Connect REST API call fails."""


IntegratorMode = Literal["source", "sink"]


class TaskStatus(str, Enum):
    """Enum for Connector task status representation."""

    STOPPED = "STOPPED"
    RUNNING = "RUNNING"
    FAILED = "FAILED"


class ClientContext:
    """Object representing Kafka Connect client relation data."""

    def __init__(
        self,
        relation: Relation | None,
        data_interface: KafkaConnectRequirerData,
    ):
        self.relation = relation
        self.data_interface = data_interface
        self.relation_data = self.data_interface.as_dict(self.relation.id) if self.relation else {}

    def __bool__(self) -> bool:
        """Boolean evaluation based on the existence of self.relation."""
        try:
            return bool(self.relation)
        except AttributeError:
            return False

    @property
    def plugin_url(self) -> str:
        """Returns the client's plugin-url REST endpoint."""
        if not self.relation:
            return ""

        return self.relation_data.get("plugin-url", "")

    @property
    def username(self) -> str:
        """Returns the Kafka Connect client username."""
        if not self.relation:
            return ""

        return self.relation_data.get("username", "")

    @property
    def endpoints(self) -> str:
        """Returns Kafka Connect endpoints set for the client."""
        if not self.relation:
            return ""

        return self.relation_data.get("endpoints", "")

    @property
    def password(self) -> str:
        """Returns the Kafka Connect client password."""
        if not self.relation:
            return ""

        return self.relation_data.get("password", "")


class BaseConfigFormatter:
    """Object used for mapping charm config keys to connector task JSON configuration keys and/or setting default configuration values.

    Mapping of charm config keys to JSON config keys is provided via class variables.
    For example, defining `topic = "connector.config.kafka.topic"` would map the value provided by `charm.config["topic"]` to a key named `connector.config.kafka.topic` in the connector JSON config.

    Default static configuration values should be provided using the `DEFAULTS` special class variable, which is mapping of config keys to their respective values.
    Obviously, charm config would override default configuration provided in `DEFAULTS`.

    Note: dynamic configuration based on relation data should be done by calling BaseIntegrator.configure() method either inside hooks or during BaseIntegrator.setup().
    Dynamic config would override static config if provided.
    """

    DEFAULTS: Mapping[str, Any] = {}

    @classmethod
    def fields(cls) -> list[str]:
        """Returns a list of non-special class variables."""
        return [
            v
            for v in dir(cls)
            if not callable(getattr(cls, v)) and not v.startswith("__") and v != "DEFAULTS"
        ]

    @classmethod
    def to_dict(cls, charm_config: ConfigData) -> dict:
        """Serializes a given charm `ConfigData` object to a Python dictionary based on predefined mappings/defaults."""
        ret = dict(cls.DEFAULTS)
        for k, v in charm_config.items():
            mapped = getattr(cls, k, k)
            ret[mapped] = v
        return ret


class ConnectIntergation:
    """Client object used for interacting with Kafka Connect REST API."""

    def __init__(self, client_context: ClientContext, connector_name: str):
        self.client_context = client_context
        self.connector_name = connector_name

    @cached_property
    def endpoint(self) -> str:
        """Returns the first valid Kafka Connect endpoint.

        Raises:
            ConnectClientError: if no valid endpoints are available

        Returns:
            str: Full URL of Kafka Connect endpoint, e.g. http://host:port
        """
        _endpoints = self.client_context.endpoints.split(",")

        if len(_endpoints) < 1:
            raise ConnectClientError("No connect endpoints available.")

        return _endpoints[0]

    def request(
        self,
        method: str = "GET",
        api: str = "",
        verbose: bool = True,
        **kwargs,
    ) -> requests.Response:
        """Makes a request to Kafka Connect REST endpoint and returns the response.

        Args:
            method (str, optional): HTTP method. Defaults to "GET".
            api (str, optional): Specific Kafka Connect API, e.g. "connector-plugins" or "connectors". Defaults to "".
            verbose (bool, optional): Whether should enable verbose logging or not. Defaults to True.
            kwargs: Keyword arguments which will be passed to `requests.request` method.

        Raises:
            ConnectApiError: If the REST API call is unsuccessful.

        Returns:
            requests.Response: Response object.
        """
        url = f"{self.endpoint}/{api}"

        auth = HTTPBasicAuth(self.client_context.username, self.client_context.password)

        try:
            response = requests.request(method, url, auth=auth, **kwargs)
        except Exception as e:
            raise ConnectApiError(f"Connect API call /{api} failed: {e}")

        if verbose:
            logging.debug(f"{method} - {url}: {response.content}")

        return response

    def start_task(self, task_config: dict) -> None:
        """Starts a connector task by posting `task_config` to the `connectors` endpoint.
        
        Raises:
            ConnectApiError: If unsuccessful.
        """
        _json = {"name": self.connector_name, "config": task_config}
        response = self.request(method="POST", api="connectors", json=_json)

        if response.status_code == 201:
            return

        if response.status_code == 409 and "already exists" in response.json().get("message", ""):
            logger.info("Task has already been submitted, skipping...")
            return
        
        logger.error(response.content)
        raise ConnectApiError(f"Unable to start the task, details: {response.content}")

    def stop_task(self) -> None:
        """Stops a connector by making a request to connectors/CONNECTOR-NAME/stop endpoint.
        
        Raises:
            ConnectApiError: If unsuccessful.
        """
        response = self.request(method="PUT", api=f"connectors/{self.connector_name}/stop")

        if response.status_code != 204:
            raise ConnectApiError(f"Unable to stop the task, details: {response.content}")


class _DataInterfacesHelpers:
    """Helper methods for handling relation data."""

    def __init__(self, charm: CharmBase):
        self.charm = charm

    def fetch_all_relation_data(self, relation_name: str) -> MutableMapping:
        """Returns a MutableMapping of all relation data available to the unit on `relation_name`, either via databag or secrets."""
        relation = self.charm.model.get_relation(relation_name=relation_name)

        if relation is None:
            return {}

        return RequirerData(self.charm.model, relation_name).as_dict(relation.id)

    def check_data_interfaces_ready(
        self,
        relation_names: list[str],
        check_for: Iterable[str] = ("endpoints", "username", "password"),
    ):
        """Checks if all data interfaces are ready, i.e. all the fields provided in `check_for` argument has been set on the respective relations.

        Args:
            relation_names (list[str]): List of relation names to check.
            check_for (Iterable[str], optional): An iterable of field names to check for their existence in relation data. Defaults to ("endpoints", "username", "password").
        """
        for relation_name in relation_names:

            if not (_data := self.fetch_all_relation_data(relation_name)):
                return False

            for key in check_for:
                if not _data.get(key, ""):
                    logger.info(
                        f"Data interfaces readiness check: relation {relation_name} - {key} not set yet."
                    )
                    return False

        return True


class BaseIntegrator(ABC, Object):
    """Basic interface for handling Kafka Connect Requirer side events and functions."""

    name: str
    mode: IntegratorMode
    formatter: type[BaseConfigFormatter]

    CONFIG_SECRET_FIELD = "config"
    SOURCE_REL = "source"
    SINK_REL = "sink"
    PEER_REL = "peer"

    def __init__(
        self,
        /,
        charm: CharmBase,
        plugin_url: str,
    ):
        super().__init__(charm, f"integrator-{self.name}")

        for field in ("name", "mode", "formatter"):
            if not getattr(self, field, None):
                raise AttributeError(f"{field} not defined on BaseIntegrator interface, did you forget to set the {field} class variable?")

        self.charm = charm
        self.plugin_url = plugin_url
        self.config = charm.config
        self.helpers: _DataInterfacesHelpers = _DataInterfacesHelpers(self.charm)

        # init handlers
        if self.mode == "source":
            self.rel_name = self.SOURCE_REL
            self.requirer = KafkaConnectRequirerEventHandlers(
                self.charm, self._source_requirer_interface
            )
        else:
            self.rel_name = self.SINK_REL
            self.requirer = KafkaConnectRequirerEventHandlers(
                self.charm, self._sink_requirer_interface
            )

        # register basic listeners for common hooks
        self.framework.observe(self.requirer.on.integration_created, self._on_integration_created)
        self.framework.observe(
            self.requirer.on.integration_endpoints_changed, self._on_integration_endpoints_changed
        )
        self.framework.observe(
            self.charm.on[self.rel_name].relation_broken, self._on_relation_broken
        )

    @property
    def _peer_relation(self) -> Optional[Relation]:
        """Peer `Relation` object."""
        return self.model.get_relation(self.PEER_REL)

    @cached_property
    def _source_requirer_interface(self) -> KafkaConnectRequirerData:
        """`source` requirer data interface."""
        return KafkaConnectRequirerData(
            self.model, relation_name=self.SOURCE_REL, plugin_url=self.plugin_url
        )

    @cached_property
    def _sink_requirer_interface(self) -> KafkaConnectRequirerData:
        """`sink` requirer data interface."""
        return KafkaConnectRequirerData(
            self.model, relation_name=self.SINK_REL, plugin_url=self.plugin_url
        )

    @cached_property
    def _peer_unit_interface(self) -> DataPeerUnitData:
        """Peer unit data interface."""
        return DataPeerUnitData(
            self.model,
            relation_name=self.PEER_REL,
            additional_secret_fields=[self.CONFIG_SECRET_FIELD],
        )

    @cached_property
    def _client_context(self) -> ClientContext:
        """Kafka Connect client data populated from relation data."""
        if self.mode == "source":
            return ClientContext(
                self.charm.model.get_relation(self.SOURCE_REL), self._source_requirer_interface
            )
        else:
            return ClientContext(
                self.charm.model.get_relation(self.SINK_REL), self._sink_requirer_interface
            )

    @cached_property
    def _client(self) -> ConnectIntergation:
        """Kafka Connect client for handling REST API calls."""
        return ConnectIntergation(self._client_context, self.name)

    # Public properties

    @property
    def started(self) -> bool:
        """Returns True if connector task is started, False otherwise."""
        if self._peer_relation is None:
            return False

        return bool(
            self._peer_unit_interface.fetch_my_relation_field(self._peer_relation.id, "started")
        )

    @started.setter
    def started(self, val: bool) -> None:
        if self._peer_relation is None:
            return

        if val:
            self._peer_unit_interface.update_relation_data(
                self._peer_relation.id, data={"started": "true"}
            )
        else:
            self._peer_unit_interface.delete_relation_data(
                self._peer_relation.id, fields=["started"]
            )

    @property
    def dynamic_config(self) -> dict[str, Any]:
        """Returns dynamic connector configuration, set during runtime inside hooks or method calls."""
        if self._peer_relation is None:
            return {}

        return json.loads(
            self._peer_unit_interface.as_dict(self._peer_relation.id).get(
                self.CONFIG_SECRET_FIELD, "{}"
            )
        )

    # Public methods

    def configure(self, config: dict[str, Any]) -> None:
        """Dynamically configure the connector with provided `config` dictionary.

        Configuration provided using this method will override default config and config provided by juju runtime (i.e. defined using the `BaseConfigFormmatter` interface).
        All configuration provided using this method are persisted using juju secrets.
        Each call would update the previous provided configuration (if any), mimicking the `dict.update()` behavior.
        """
        if self._peer_relation is None:
            return

        updated_config = self.dynamic_config | config

        self._peer_unit_interface.update_relation_data(
            self._peer_relation.id, data={self.CONFIG_SECRET_FIELD: json.dumps(updated_config)}
        )

    def start_task(self) -> None:
        """Starts the connector task."""
        if self.started:
            logger.info("Connector task has already started")
            return

        self.setup()

        try:
            self._client.start_task(self.formatter.to_dict(self.config) | self.dynamic_config)
        except ConnectApiError as e:
            logger.error(f"Task start failed, details: {e}")
            return
        
        self.started = True

    def stop_task(self) -> None:
        """Stops the connector task."""
        
        try:
            self._client.stop_task()
        except ConnectApiError as e:
            logger.error(f"Task stop failed, details: {e}")
            return

        self.teardown()
        self.started = False

    def task_status(self) -> TaskStatus:
        """Returns connector task status."""
        if not self.started:
            return TaskStatus.STOPPED
        
        return TaskStatus.RUNNING

    # Abstract methods

    @property
    @abstractmethod
    def ready(self) -> bool:
        """Should return True if all conditions for startig the task is met, including if all client relations are setup successfully."""
        ...

    @abstractmethod
    def setup(self) -> None:
        """Should perform all necessary actions before connector task is started."""
        ...

    @abstractmethod
    def teardown(self) -> None:
        """Should perform all necessary cleanups after connector task is stopped."""
        ...

    # Event handlers

    def _on_integration_created(self, event: IntegrationCreatedEvent) -> None:
        """Handler for `integration_created` event."""
        if not self.ready:
            logging.debug("Integrator not ready yet, deferring integration_created event...")
            event.defer()
            return

        logger.info(f"Starting {self.name} task...")
        self.start_task()

        if not self.started:
            event.defer()

    def _on_integration_endpoints_changed(self, _: IntegrationEndpointsChangedEvent) -> None:
        """Handler for `integration_endpoints_changed` event."""
        pass

    def _on_relation_broken(self, _: RelationBrokenEvent) -> None:
        """Handler for `relation-broken` event."""
        self.stop_task()
