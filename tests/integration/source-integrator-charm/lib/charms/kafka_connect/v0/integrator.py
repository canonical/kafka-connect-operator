#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Requirer-side event handling and charm config management for Kafka Connect integrator charms."""

# TODO: this would be moved into a separate python package alongside the REST interface implementation for plugin serving.
# The instructions to use the package should be added to README there.

import json
import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping, MutableMapping
from enum import Enum
from functools import cached_property
from typing import Any, Iterable, Literal, Optional, cast

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
from pydantic import BaseModel
from requests.auth import HTTPBasicAuth

# The unique Charmhub library identifier, never change it
LIBID = "77777777777777777777777777777777"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 1


YAML_TYPE_MAPPER = {str: "string", int: "int", bool: "boolean"}


logger = logging.getLogger(__name__)


class ConnectClientError(Exception):
    """Exception raised when Kafka Connect client could not be instantiated."""


class ConnectApiError(Exception):
    """Exception raised when Kafka Connect REST API call fails."""


IntegratorMode = Literal["source", "sink"]


class TaskStatus(str, Enum):
    """Enum for Connector task status representation."""

    UNASSIGNED = "UNASSIGNED"
    PAUSED = "PAUSED"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


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


class BasePluginServer(ABC):
    """Base interface for plugin server service."""

    plugin_url: str

    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def start(self) -> None:
        """Starts the plugin server service."""
        ...

    @abstractmethod
    def stop(self) -> None:
        """Stops the plugin server service."""
        ...

    @abstractmethod
    def configure(self) -> None:
        """Makes all necessary configurations to start the server service."""
        ...

    @abstractmethod
    def health_check(self) -> bool:
        """Checks that the plugin server is active and healthy."""
        ...


class ConfigOption(BaseModel):
    """Model for defining mapping between charm config and connector config.

    To define a config mapping, following properties could be used:

        json_key (str, required): The counterpart key in connector JSON config. If `mode` is set to "none", this would be ignored and could be set to any arbitrary value.
        default (Any, required): The default value for this config option.
        mode (Literal["both", "source", "sink", "none"], optional): Defaults to "both". The expected behaviour of each mode are as following:
            - both: This config option would be used in both source and sink connector modes.
            - source: This config option would be used ONLY in source connector mode.
            - sink: This config option would be used ONLY in sink connector mode.
            - none: This is not a connector config, but rather a charm config. If set to none, this option would not be used to configure the connector.
        configurable (bool, optional): Whether this option is configurable via charm config. Defaults to True.
        description (str, optional): A brief description of this config option, which will be added to the charm's `config.yaml`.
    """

    json_key: str  # Config key in the Connector configuration JSON
    default: Any  # Default value
    mode: Literal[
        "both", "source", "sink", "none"
    ] = "both"  # Whether this is a generic config or source/sink-only.
    configurable: bool = True  # Whether this option is configurable using charm config or not
    description: str = ""


class BaseConfigFormatter:
    """Object used for mapping charm config keys to connector JSON configuration keys and/or setting default configuration values.

    Mapping of charm config keys to JSON config keys is provided via `ConfigOption` class variables.

    Example: To map `topic` charm config to `connector.config.kafka.topic` in the connector JSON config, one should provide:

        topic = ConfigOption(json_key="connector.config.kafka.topic", default="some-topic", description="...")

    Default static configuration values should be provided by setting `configurable=False` in `ConfigOption` and providing a `default` value

    Note: dynamic configuration based on relation data should be done by calling BaseIntegrator.configure() method either inside hooks or during BaseIntegrator.setup().
    Dynamic config would override static config if provided.
    """

    mode = ConfigOption(
        json_key="na",
        default="source",
        description='Integrator mode, either "source" or "sink"',
        mode="none",
    )

    @classmethod
    def fields(cls) -> list[str]:
        """Returns a list of non-special class variables."""
        return [v for v in dir(cls) if not callable(getattr(cls, v)) and not v.startswith("__")]

    @classmethod
    def to_dict(cls, charm_config: ConfigData, mode: IntegratorMode) -> dict:
        """Serializes a given charm `ConfigData` object to a Python dictionary based on predefined mappings/defaults."""
        ret = {}
        for k in cls.fields():
            option = cast(ConfigOption, getattr(cls, k))

            if option.mode == "none":
                # This is not a connector config option
                continue

            if option.mode != "both" and option.mode != mode:
                # Source/sink-only config, skip
                continue

            if option.configurable and k in charm_config:
                ret[option.json_key] = charm_config[k]
                continue

            ret[option.json_key] = option.default

        return ret

    @classmethod
    def to_config_yaml(cls) -> dict[str, Any]:
        """Returns a dict compatible with charmcraft `config.yaml` format."""
        config = {"options": {}}
        options = config["options"]

        for _attr in dir(cls):

            if _attr.startswith("__"):
                continue

            attr = getattr(cls, _attr)

            if isinstance(attr, ConfigOption):
                option = cast(ConfigOption, attr)

                if not option.configurable:
                    continue

                options[_attr] = {
                    "default": option.default,
                    "type": YAML_TYPE_MAPPER.get(type(option.default), "string"),
                }

                if option.description:
                    options[_attr]["description"] = option.description

        return config


class ConnectClient:
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
            # TODO: FIXME: use tls-ca to verify the cert.
            response = requests.request(method, url, verify=False, auth=auth, **kwargs)
        except Exception as e:
            raise ConnectApiError(f"Connect API call /{api} failed: {e}")

        if verbose:
            logging.debug(f"{method} - {url}: {response.content}")

        return response

    def start_connector(self, connector_config: dict, connector_name: str | None = None) -> None:
        """Starts a connector by posting `connector_config` to the `connectors` endpoint.

        Raises:
            ConnectApiError: If unsuccessful.
        """
        _json = {
            "name": connector_name or self.connector_name,
            "config": connector_config,
        }
        response = self.request(method="POST", api="connectors", json=_json)

        if response.status_code == 201:
            return

        if response.status_code == 409 and "already exists" in response.json().get("message", ""):
            logger.info("Connector has already been submitted, skipping...")
            return

        logger.error(response.content)
        raise ConnectApiError(f"Unable to start the connector, details: {response.content}")

    def resume_connector(self, connector_name: str | None = None) -> None:
        """Resumes a connector task by PUTting to the `connectors/<CONNECTOR-NAME>/resume` endpoint.

        Raises:
            ConnectApiError: If unsuccessful.
        """
        response = self.request(
            method="PUT", api=f"connectors/{connector_name or self.connector_name}/resume"
        )

        if response.status_code == 202:
            return

        logger.error(response.content)
        raise ConnectApiError(f"Unable to resume the connector, details: {response.content}")

    def stop_connector(self, connector_name: str | None = None) -> None:
        """Stops a connector at connectors/[CONNECTOR-NAME|connector-name]/stop endpoint.

        Raises:
            ConnectApiError: If unsuccessful.
        """
        response = self.request(
            method="PUT", api=f"connectors/{connector_name or self.connector_name}/stop"
        )

        if response.status_code != 204:
            raise ConnectApiError(f"Unable to stop the connector, details: {response.content}")

    def delete_connector(self, connector_name: str | None = None) -> None:
        """Deletes a connector at connectors/[CONNECTOR-NAME|connector-name] endpoint.

        Raises:
            ConnectApiError: If unsuccessful.
        """
        response = self.request(
            method="DELETE", api=f"connectors/{connector_name or self.connector_name}"
        )

        if response.status_code != 204:
            raise ConnectApiError(f"Unable to remove the connector, details: {response.content}")

    def patch_connector(self, connector_config: dict, connector_name: str | None = None) -> None:
        """Patches a connector by PATCHting `connector_config` to the `connectors/<CONNECTOR-NAME>` endpoint.

        Raises:
            ConnectApiError: If unsuccessful.
        """
        response = self.request(
            method="PATCH",
            api=f"connectors/{connector_name or self.connector_name}/config",
            json=connector_config,
        )

        if response.status_code == 200:
            logger.debug(
                f"Connector {connector_name or self.connector_name} patched: {connector_config}"
            )
            return

        logger.error(response.content)
        raise ConnectApiError(f"Unable to patch the connector, details: {response.content}")

    def task_status(self, connector_name: str | None = None) -> TaskStatus:
        """Returns the task status of a connector."""
        connector_name = connector_name or self.connector_name

        response = self.request(method="GET", api=f"connectors/{connector_name}/tasks", timeout=10)

        if response.status_code not in (200, 404):
            logger.error(f"Unable to fetch tasks status, details: {response.content}")
            return TaskStatus.UNKNOWN

        if response.status_code == 404:
            return TaskStatus.UNASSIGNED

        tasks = response.json()

        if not tasks:
            return TaskStatus.UNASSIGNED

        task_id = tasks[0].get("id", {}).get("task", 0)
        status_response = self.request(
            method="GET",
            api=f"connectors/{connector_name}/tasks/{task_id}/status",
            timeout=10,
        )

        if status_response.status_code == 404:
            return TaskStatus.UNASSIGNED

        if status_response.status_code != 200:
            logger.error(f"Unable to fetch tasks status, details: {status_response.content}")
            return TaskStatus.UNKNOWN

        state = status_response.json().get("state", "UNASSIGNED")
        return TaskStatus(state)

    def connector_status(self) -> TaskStatus:
        """Returns the connector status."""
        try:
            response = self.request(
                method="GET", api=f"connectors/{self.connector_name}/status", timeout=10
            )
        except ConnectApiError as e:
            logger.error(e)
            return TaskStatus.UNKNOWN

        if response.status_code not in (200, 404):
            logger.error(f"Unable to fetch connector status, details: {response.content}")
            return TaskStatus.UNKNOWN

        if response.status_code == 404:
            return TaskStatus.UNASSIGNED

        status_response = response.json()

        state = status_response.get("connector", {}).get("state", "UNASSIGNED")
        return TaskStatus(state)


class _DataInterfacesHelpers:
    """Helper methods for handling relation data."""

    def __init__(self, charm: CharmBase):
        self.charm = charm

    def remote_app_name(self, relation_name: str) -> str:
        """Returns the remote application name for the given relation name."""
        relation = self.charm.model.get_relation(relation_name=relation_name)
        if not relation:
            return ""

        return relation.app.name

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
    formatter: type[BaseConfigFormatter]
    plugin_server: type[BasePluginServer]
    mode: IntegratorMode = "source"

    CONFIG_SECRET_FIELD = "config"
    CONNECT_REL = "connect-client"
    PEER_REL = "peer"
    # Used on MirrorMaker, the integrator needs to spawn 3 connectors per relation
    MULTICONNECTOR_PREFIX = "mirror"

    def __init__(
        self,
        /,
        charm: CharmBase,
        plugin_server_args: Optional[list] = [],
        plugin_server_kwargs: Mapping[str, Any] = {},
    ):
        super().__init__(charm, f"integrator-{self.name}")

        for field in ("name", "formatter", "plugin_server"):
            if not getattr(self, field, None):
                raise AttributeError(
                    f"{field} not defined on BaseIntegrator interface, did you forget to set the {field} class variable?"
                )

        self.charm = charm
        self.server = self.plugin_server(*plugin_server_args, **plugin_server_kwargs)
        self.plugin_url = self.server.plugin_url
        self.config = charm.config
        self.mode: IntegratorMode = cast(IntegratorMode, self.config.get("mode", self.mode))
        self.helpers: _DataInterfacesHelpers = _DataInterfacesHelpers(self.charm)

        self._connector_names: list[str] = []

        # init handlers
        self.rel_name = self.CONNECT_REL
        self.requirer = KafkaConnectRequirerEventHandlers(self.charm, self._requirer_interface)

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

    @property
    def _connect_client_relation(self) -> Optional[Relation]:
        """connect-client `Relation` object."""
        return self.model.get_relation(self.CONNECT_REL)

    @cached_property
    def _requirer_interface(self) -> KafkaConnectRequirerData:
        """`connect-client` requirer data interface."""
        return KafkaConnectRequirerData(
            self.model, relation_name=self.CONNECT_REL, plugin_url=self.plugin_url
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
        return ClientContext(
            self.charm.model.get_relation(self.CONNECT_REL), self._requirer_interface
        )

    @cached_property
    def _client(self) -> ConnectClient:
        """Kafka Connect client for handling REST API calls."""
        return ConnectClient(self._client_context, self.connector_unique_name)

    # Public properties

    @property
    def connector_unique_name(self) -> str:
        """Returns connectors' unique name used on the REST interface."""
        if not self._connect_client_relation:
            return ""

        relation_id = self._connect_client_relation.id
        return f"{self.name}_r{relation_id}_{self.model.uuid.replace('-', '')}"

    @property
    def started(self) -> bool:
        """Returns True if connector is started, False otherwise."""
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

    @property
    def connector_names(self) -> list[str]:
        """Return a list of connector names that the integrator should create."""
        connectors = []
        for name in self.dynamic_config.keys():
            if name.startswith(self.MULTICONNECTOR_PREFIX):
                connectors.append(name)
        return connectors

    # Public methods

    def configure(self, config: dict[str, Any] | list[dict[str, Any]]) -> None:
        """Dynamically configure the connector with provided `config` dictionary.

        Configuration provided using this method will override default config and config provided
        by juju runtime (i.e. defined using the `BaseConfigFormmatter` interface).
        All configuration provided using this method are persisted using juju secrets.
        Each call would update the previous provided configuration (if any), mimicking the
        `dict.update()` behavior.

        Args:
            config (dict[str, Any] | list[dict[str, Any]]):
                - If a single dict is provided, updates the configuration for a single connector.
                - If a list of dicts is provided, it will create multiple connector. Each dict in
                  the list represents a separate connector configuration. A "name" key should be
                  used to help differentiate connector names.
        """
        if self._peer_relation is None:
            return

        updated_config = self.dynamic_config.copy()

        # Single configuration
        if isinstance(config, dict):
            updated_config.update(config)

        # Multiple configurations
        if isinstance(config, list):
            for connector_config in config:
                try:
                    connector_id = f"{self.MULTICONNECTOR_PREFIX}_{connector_config['name']}_{self.connector_unique_name}"
                except KeyError:
                    logger.error(
                        "List of connectors should provide a 'name' key to differentiate them"
                    )
                    raise

                # Remove "name" key so it's not part of the config passed to the JSON afterwards
                connector_config.pop("name")
                updated_config[connector_id] = connector_config

        self._peer_unit_interface.update_relation_data(
            self._peer_relation.id, data={self.CONFIG_SECRET_FIELD: json.dumps(updated_config)}
        )

    def start_connector(self) -> None:
        """Starts the connectors."""
        if self.started:
            logger.info("Connector has already started")
            return

        self.setup()

        # We have more than one connector to be executed. If list is empty, this is skipped
        for connector_name in self.connector_names:
            try:
                self._client.start_connector(
                    connector_config=self.formatter.to_dict(
                        charm_config=self.config, mode="source"
                    )
                    | self.dynamic_config[connector_name],
                    connector_name=connector_name,
                )
            except ConnectApiError as e:
                logger.error(f"Connector start failed, details: {e}")
                return

        # Single connector case
        if not self.connector_names:
            try:
                self._client.start_connector(
                    self.formatter.to_dict(charm_config=self.config, mode="source")
                    | self.dynamic_config
                )
            except ConnectApiError as e:
                logger.error(f"Connector start failed, details: {e}")
                return

        self.started = True

    def maybe_resume_connector(self) -> None:
        """Restarts/resumes the connector if it's in STOPPED state."""
        # TODO: resume should cover multiple connectors existing
        if self.connector_status != TaskStatus.STOPPED:
            return

        try:
            self._client.resume_connector()
        except ConnectApiError as e:
            logger.error(f"Unable to restart the connector, details: {e}")
            return

    def patch_connector(self) -> None:
        """Updates the connector(s) configuration.

        Will override the existing configuration for the connector.
        """
        if not self.started:
            logger.info("Connector is not started yet, skipping update.")
            return

        self.setup()

        # We have more than one connector to be executed. If list is empty, this is skipped
        for connector_name in self.connector_names:
            try:
                self._client.patch_connector(
                    connector_config=self.formatter.to_dict(
                        charm_config=self.config, mode="source"
                    )
                    | self.dynamic_config[connector_name],
                    connector_name=connector_name,
                )
            except ConnectApiError as e:
                logger.error(f"Connector start failed, details: {e}")
                return

        # Single connector case
        if not self.connector_names:
            try:
                self._client.patch_connector(
                    self.formatter.to_dict(charm_config=self.config, mode="source")
                    | self.dynamic_config
                )
            except ConnectApiError as e:
                logger.error(f"Connector start failed, details: {e}")
                return

        self.started = True

    @property
    def task_status(self) -> TaskStatus:
        """Returns connector task status."""
        if not self.started:
            return TaskStatus.UNASSIGNED

        # TODO: status should be handled in a more complex way than this
        if self.connector_names:
            return self._client.task_status(connector_name=self.connector_names[0])

        return self._client.task_status()

    @property
    def connector_status(self) -> TaskStatus:
        """Returns connector status."""
        return self._client.connector_status()

    # Abstract methods

    @property
    @abstractmethod
    def ready(self) -> bool:
        """Should return True if all conditions for startig the connector is met, including if all client relations are setup successfully."""
        ...

    @abstractmethod
    def setup(self) -> None:
        """Should perform all necessary actions before connector is started."""
        ...

    @abstractmethod
    def teardown(self) -> None:
        """Should perform all necessary cleanups after connector is stopped."""
        ...

    # Event handlers

    def _on_integration_created(self, event: IntegrationCreatedEvent) -> None:
        """Handler for `integration_created` event."""
        if not self.server.health_check() or not self.ready:
            logging.debug("Integrator not ready yet, deferring integration_created event...")
            event.defer()
            return

        logger.info(f"Starting {self.name} connector...")
        self.start_connector()

        if not self.started:
            event.defer()

    def _on_integration_endpoints_changed(self, _: IntegrationEndpointsChangedEvent) -> None:
        """Handler for `integration_endpoints_changed` event."""
        pass

    def _on_relation_broken(self, _: RelationBrokenEvent) -> None:
        """Handler for `relation-broken` event."""
        self.teardown()
        self.started = False
