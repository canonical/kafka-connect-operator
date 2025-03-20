#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import dataclasses
import json
import logging
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import pytest
import yaml
from charms.data_platform_libs.v0.upgrade import ClusterNotReadyError, DependencyModel
from ops.testing import Context, PeerRelation, State

from charm import ConnectCharm
from events.upgrade import ConnectDependencyModel
from literals import DEPENDENCIES, PEER_REL, SUBSTRATE

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.broker


CONFIG = yaml.safe_load(Path("./config.yaml").read_text())
ACTIONS = yaml.safe_load(Path("./actions.yaml").read_text())
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())


@pytest.fixture()
def charm_configuration():
    """Enable direct mutation on configuration dict."""
    return json.loads(json.dumps(CONFIG))


@pytest.fixture()
def upgrade_func() -> str:
    if SUBSTRATE == "k8s":
        return "_on_connect_pebble_ready_upgrade"

    return "_on_upgrade_granted"


def test_pre_upgrade_check_raises_not_healthy(ctx: Context, base_state: State) -> None:
    # Given
    state_in = base_state

    # When
    with ctx(ctx.on.config_changed(), state_in) as manager:
        charm = cast(ConnectCharm, manager.charm)

        with pytest.raises(ClusterNotReadyError):
            charm.upgrade.pre_upgrade_check()


def test_pre_upgrade_check_succeeds(ctx: Context, base_state: State) -> None:
    # Given
    state_in = base_state

    # When
    with (
        patch("managers.connect.ConnectManager.health_check", return_value=True),
        patch("events.upgrade.ConnectUpgrade._set_rolling_update_partition"),
        ctx(ctx.on.config_changed(), state_in) as manager,
    ):
        charm = cast(ConnectCharm, manager.charm)

        # Then
        charm.upgrade.pre_upgrade_check()


@pytest.mark.skipif(SUBSTRATE == "k8s", reason="upgrade stack not used on K8s")
def test_build_upgrade_stack(ctx: Context, base_state: State) -> None:
    # Given
    cluster_peer = PeerRelation(
        PEER_REL,
        PEER_REL,
        local_unit_data={"private-address": "000.000.000"},
        peers_data={1: {"private-address": "111.111.111"}, 2: {"private-address": "222.222.222"}},
    )
    state_in = dataclasses.replace(base_state, relations=[cluster_peer])

    # When
    with ctx(ctx.on.config_changed(), state_in) as manager:
        charm = cast(ConnectCharm, manager.charm)
        stack = charm.upgrade.build_upgrade_stack()

    # Then
    assert len(stack) == 3
    assert len(stack) == len(set(stack))


def test_connect_dependency_model():
    assert sorted(ConnectDependencyModel.__fields__.keys()) == sorted(DEPENDENCIES.keys())

    for value in DEPENDENCIES.values():
        assert DependencyModel(**value)


@pytest.mark.skipif(SUBSTRATE == "k8s", reason="Upgrade granted not used on K8s charms")
def test_upgrade_granted_sets_failed_if_failed_snap(ctx: Context, base_state: State) -> None:
    # Given
    state_in = base_state

    # Then
    with (
        patch("workload.Workload.stop") as patched_stop,
        patch("workload.Workload.install", return_value=False),
        patch(
            "events.upgrade.ConnectUpgrade.set_unit_failed",
        ) as patch_set_failed,
        ctx(ctx.on.config_changed(), state_in) as manager,
    ):
        charm = cast(ConnectCharm, manager.charm)
        mock_event = MagicMock()
        charm.upgrade._on_upgrade_granted(mock_event)

    # Then
    patched_stop.assert_called_once()
    assert patch_set_failed.call_count


def test_upgrade_sets_failed_if_failed_upgrade_check(
    ctx: Context, base_state: State, upgrade_func: str
) -> None:
    # Given
    state_in = base_state

    # When
    with (
        patch("workload.Workload.restart") as patched_restart,
        patch("workload.Workload.start") as patched_start,
        patch("workload.Workload.stop"),
        patch("workload.Workload.install"),
        patch("managers.connect.ConnectManager.health_check", return_value=False),
        patch("core.models.Context.ready", return_value=True),
        patch(
            "events.upgrade.ConnectUpgrade.set_unit_failed",
        ) as patch_set_failed,
        ctx(ctx.on.config_changed(), state_in) as manager,
    ):
        charm = cast(ConnectCharm, manager.charm)
        mock_event = MagicMock()
        getattr(charm.upgrade, upgrade_func)(mock_event)

    # Then
    assert patched_restart.call_count or patched_start.call_count
    assert patch_set_failed.call_count


def test_upgrade_succeeds(
    ctx: Context, base_state: State, upgrade_func: str, active_service
) -> None:
    # Given
    state_in = base_state

    # When
    with (
        patch("workload.Workload.restart") as patched_restart,
        patch("workload.Workload.start") as patched_start,
        patch("workload.Workload.stop"),
        patch("workload.Workload.install"),
        patch("core.models.Context.ready", return_value=True),
        patch(
            "events.upgrade.ConnectUpgrade.set_unit_completed",
        ) as patch_set_completed,
        ctx(ctx.on.config_changed(), state_in) as manager,
    ):
        charm = cast(ConnectCharm, manager.charm)
        mock_event = MagicMock()
        getattr(charm.upgrade, upgrade_func)(mock_event)

    assert patched_restart.call_count or patched_start.call_count
    assert patch_set_completed.call_count


@pytest.mark.skipif(SUBSTRATE == "k8s", reason="Upgrade granted not used on K8s charms")
def test_upgrade_granted_recurses_upgrade_changed_on_leader(
    ctx: Context, base_state: State, active_service
) -> None:
    # Given
    state_in = base_state

    # When
    with (
        patch("workload.Workload.restart"),
        patch("workload.Workload.start"),
        patch("workload.Workload.stop"),
        patch("workload.Workload.install"),
        patch("core.models.Context.ready", return_value=True),
        patch(
            "events.upgrade.ConnectUpgrade.on_upgrade_changed", autospec=True
        ) as patched_upgrade,
        ctx(ctx.on.config_changed(), state_in) as manager,
    ):
        charm = cast(ConnectCharm, manager.charm)
        mock_event = MagicMock()
        charm.upgrade._on_upgrade_granted(mock_event)

    # Then
    patched_upgrade.assert_called_once()
