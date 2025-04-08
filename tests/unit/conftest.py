#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

from collections import defaultdict
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
import yaml
from ops.testing import Container, Context, Resource, State
from src.charm import ConnectCharm
from src.core.workload import Paths
from src.literals import CONTAINER, PLUGIN_RESOURCE_KEY, SNAP_NAME, SUBSTRATE

CONFIG = yaml.safe_load(Path("./config.yaml").read_text())
ACTIONS = yaml.safe_load(Path("./actions.yaml").read_text())
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())


@pytest.fixture()
def base_state():
    if SUBSTRATE == "k8s":
        state = State(leader=True, containers=[Container(name=CONTAINER, can_connect=True)])
    else:
        state = State(leader=True)

    return state


@pytest.fixture()
def ctx() -> Context:
    ctx = Context(ConnectCharm, meta=METADATA, config=CONFIG, actions=ACTIONS, unit_id=0)
    return ctx


@pytest.fixture(autouse=True)
def workload(monkeypatch):
    """Workload with completely mocked functionality."""
    monkeypatch.setattr("workload.Workload.exec", Mock())
    monkeypatch.setattr("workload.Workload.installed", True)
    monkeypatch.setattr("workload.Workload.write", Mock())
    yield


@pytest.fixture()
def workload_with_io(monkeypatch, tmp_path_factory):
    """Workload with simulated read/write functionality using temp paths."""
    # undo previous autouse monkeypatches on workload
    monkeypatch.undo()

    class TmpPaths(Paths):
        logs_dir = tmp_path_factory.mktemp("logs")
        snap_dir = tmp_path_factory.mktemp("snap")
        env = tmp_path_factory.mktemp("etc") / "environment"
        plugins = tmp_path_factory.mktemp("plugins")

    paths = TmpPaths(config_dir=tmp_path_factory.mktemp("config"))

    monkeypatch.setattr("workload.Workload.exec", Mock())
    monkeypatch.setattr("workload.Workload.installed", True)
    monkeypatch.setattr("workload.Workload.paths", paths)
    yield


@pytest.fixture(autouse=True)
def patched_snap(monkeypatch):
    cache = Mock()
    snap_mock = Mock()
    snap_mock.services = defaultdict(default_factory=lambda _: {"active": True})
    cache.return_value = {SNAP_NAME: snap_mock}
    with monkeypatch.context() as m:
        m.setattr("charms.operator_libs_linux.v2.snap.SnapCache", cache)
        yield


@pytest.fixture(autouse=True)
def tenacity_wait():
    with patch("tenacity.nap.time") as patched_nap:
        yield patched_nap


@pytest.fixture(scope="module")
def kafka_client_rel():
    return {
        "username": "username",
        "password": "password",
        "tls": "disabled",
        "tls-ca": "disabled",
        "endpoints": "10.10.10.10:9092,10.10.10.11:9092",
    }


@pytest.fixture(scope="module")
def plugin_resource():
    return Resource(name=PLUGIN_RESOURCE_KEY, path="./tests/unit/resources/FakePlugin.tar")


@pytest.fixture(scope="module")
def active_service():
    mock_response = MagicMock()
    mock_response.json.return_value = {}

    with (
        patch(
            "managers.connect.ConnectManager.health_check", return_value=True
        ) as patched_service,
        patch("managers.connect.ConnectManager._request", return_value=mock_response),
    ):
        yield patched_service
