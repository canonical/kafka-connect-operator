#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import logging

import pytest
from helpers import (
    APP_NAME,
    KAFKA_APP,
    KAFKA_CHANNEL,
    check_connect_endpoints_status,
    make_connect_api_request,
)
from pytest_operator.plugin import OpsTest

from literals import DEFAULT_API_PORT

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_deploy_charms(ops_test: OpsTest, kafka_connect_charm):
    """Deploys kafka-connect charm along kafka (in KRaft mode)."""
    # deploy kafka & kafka-connect
    await asyncio.gather(
        ops_test.model.deploy(
            kafka_connect_charm,
            application_name=APP_NAME,
            num_units=1,
            series="jammy",
        ),
        ops_test.model.deploy(
            KAFKA_APP,
            channel=KAFKA_CHANNEL,
            application_name=KAFKA_APP,
            num_units=1,
            series="jammy",
            config={"roles": "broker,controller"},
        ),
    )

    await ops_test.model.wait_for_idle(apps=[APP_NAME, KAFKA_APP], timeout=3000)

    assert ops_test.model.applications[KAFKA_APP].status == "active"
    assert ops_test.model.applications[APP_NAME].status == "blocked"

    await ops_test.model.add_relation(APP_NAME, KAFKA_APP)

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, KAFKA_APP], idle_period=60, timeout=1000
        )

    assert ops_test.model.applications[APP_NAME].status == "active"


@pytest.mark.abort_on_fail
async def test_api_endpoint(ops_test: OpsTest):
    """Checks API endpoint connectivity using a socket."""
    status = await check_connect_endpoints_status(
        ops_test, app_name=APP_NAME, port=DEFAULT_API_PORT
    )

    # assert all endpoints are up
    assert all(status.values())


async def test_scale_out(ops_test: OpsTest):
    """Checks connect workers scaling functionality."""
    await ops_test.model.applications[APP_NAME].add_units(count=2)
    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME], idle_period=30, timeout=1200, status="active", wait_for_exact_units=3
        )

    await check_connect_endpoints_status(ops_test, app_name=APP_NAME, port=DEFAULT_API_PORT)


@pytest.mark.abort_on_fail
async def test_auth(ops_test: OpsTest):
    """Checks authentication is enabled on all API endpoints."""
    for unit in ops_test.model.applications[APP_NAME].units:
        response = await make_connect_api_request(ops_test, unit=unit, auth_enabled=False)
        assert response.status_code == 401

        response = await make_connect_api_request(ops_test, unit=unit, auth_enabled=True)
        assert response.status_code == 200


@pytest.mark.abort_on_fail
async def test_broken_kafka_relation(ops_test: OpsTest):

    await ops_test.juju("remove-relation", APP_NAME, KAFKA_APP)
    await ops_test.model.wait_for_idle(apps=[APP_NAME, KAFKA_APP], timeout=1000, idle_period=30)

    status = await check_connect_endpoints_status(
        ops_test, app_name=APP_NAME, port=DEFAULT_API_PORT
    )

    assert ops_test.model.applications[APP_NAME].status == "blocked"
    # assert all endpoints are down
    assert not any(status.values())
