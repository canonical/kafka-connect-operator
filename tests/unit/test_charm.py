#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import dataclasses
import logging
from unittest.mock import patch

import pytest
from ops.testing import Context, Relation, State
from src.literals import KAFKA_CLIENT_REL, SUBSTRATE, Status

logger = logging.getLogger(__name__)


@pytest.mark.skipif(SUBSTRATE == "k8s", reason="snap not used on K8s")
def test_install_blocks_snap_install_failure(ctx: Context, base_state: State) -> None:
    """Checks unit goes to Blocked status after snap failure on install hook."""
    # Given
    state_in = base_state

    # When
    with patch("workload.Workload.install", return_value=False), patch("workload.Workload.write"):
        state_out = ctx.run(ctx.on.install(), state_in)

    # Then
    assert state_out.unit_status == Status.SNAP_NOT_INSTALLED.value.status


def test_ready_to_start_maintenance_no_kafka_client_relation(
    ctx: Context, base_state: State
) -> None:
    """Checks unit goes to Maintenance status when started without kafka-client relation."""
    # Given
    state_in = base_state

    # When
    state_out = ctx.run(ctx.on.start(), state_in)

    # Then
    assert state_out.unit_status == Status.MISSING_KAFKA.value.status


def test_kafka_client_relation_created_waits_for_credentials(
    ctx: Context, base_state: State
) -> None:
    """Checks unit goes to Waiting status when related with Kafka and waiting for credentials."""
    # Given
    kafka_rel = Relation(KAFKA_CLIENT_REL, KAFKA_CLIENT_REL, remote_app_data={})
    state_in = dataclasses.replace(base_state, relations=[kafka_rel])

    # When
    state_out = ctx.run(ctx.on.relation_created(kafka_rel), state_in)

    # Then
    assert state_out.unit_status == Status.NO_KAFKA_CREDENTIALS.value.status


@pytest.mark.parametrize("broker_available", [False, True])
def test_kafka_client_relation_created_checks_broker_availability(
    ctx: Context, base_state: State, kafka_client_rel: dict, broker_available: bool, active_service
) -> None:
    """Checks unit checks Kafka broker listener availability before transitioning to Active status."""
    # Given
    kafka_rel = Relation(KAFKA_CLIENT_REL, KAFKA_CLIENT_REL, remote_app_data=kafka_client_rel)
    state_in = dataclasses.replace(base_state, relations=[kafka_rel])

    # When
    with (
        patch(
            "managers.kafka.KafkaManager._check_socket",
            return_value=broker_available,
        ) as fake_check_socket,
        patch("workload.Workload.restart") as _restart,
    ):
        state_out = ctx.run(ctx.on.relation_created(kafka_rel), state_in)

    # Then
    assert fake_check_socket.call_count > 0

    if not broker_available:
        assert state_out.unit_status == Status.NO_KAFKA_CREDENTIALS.value.status
    else:
        assert state_out.unit_status == Status.ACTIVE.value.status


def test_kafka_client_relation_change_triggers_restart(
    ctx: Context, base_state: State, kafka_client_rel: dict, active_service
) -> None:
    """Checks change in `kafka-client` relation configuration triggers a restart."""
    # Given
    kafka_rel = Relation(KAFKA_CLIENT_REL, KAFKA_CLIENT_REL, remote_app_data=kafka_client_rel)
    state_in = dataclasses.replace(base_state, relations=[kafka_rel])

    # When
    with (patch("workload.Workload.read"), patch("workload.Workload.restart") as _restart):
        state_out = ctx.run(ctx.on.relation_changed(kafka_rel), state_in)

    # Then
    assert _restart.call_count == 1
    assert state_out.unit_status == Status.ACTIVE.value.status


def test_kafka_client_relation_broken(
    ctx: Context, base_state: State, kafka_client_rel: dict
) -> None:
    """Checks `kafka-client` relation broken puts the unit in Blocked status."""
    # Given
    kafka_rel = Relation(KAFKA_CLIENT_REL, KAFKA_CLIENT_REL, remote_app_data=kafka_client_rel)
    state_in = dataclasses.replace(base_state, relations=[kafka_rel])

    # When
    state_out = ctx.run(ctx.on.relation_broken(kafka_rel), state_in)

    # Then
    assert state_out.unit_status == Status.MISSING_KAFKA.value.status
