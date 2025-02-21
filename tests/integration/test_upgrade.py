#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import logging
import time

import pytest
from helpers import (
    APP_NAME,
    KAFKA_APP,
    KAFKA_CHANNEL,
    check_connect_endpoints_status,
)
from pytest_operator.plugin import OpsTest

from literals import DEFAULT_API_PORT

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.broker

CHANNEL = "edge"


@pytest.mark.abort_on_fail
async def test_in_place_upgrade(ops_test: OpsTest, kafka_connect_charm):
    # deploy kafka & kafka-connect
    await asyncio.gather(
        ops_test.model.deploy(
            APP_NAME,
            channel=CHANNEL,
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

    logger.info("Calling pre-upgrade-check")
    action = await ops_test.model.applications[APP_NAME].units[0].run_action("pre-upgrade-check")
    await action.wait()

    # ensure action completes
    time.sleep(10)

    logger.info("Upgrading Connect...")
    await ops_test.model.applications[APP_NAME].refresh(path=kafka_connect_charm)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=1000, idle_period=120
    )

    await check_connect_endpoints_status(ops_test, app_name=APP_NAME, port=DEFAULT_API_PORT)
