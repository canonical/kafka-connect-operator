import asyncio
import logging
from itertools import product

import pytest
from helpers import (
    APP_NAME,
    KAFKA_APP,
    KAFKA_CHANNEL,
    make_connect_api_request,
)
from pytest_operator.plugin import OpsTest

from core.models import PeerWorkersContext
from literals import PLUGIN_RESOURCE_KEY

logger = logging.getLogger(__name__)

AUTH_SECRET_CONFIG_KEY = "system-users"
TEST_SECRET_NAME = "test-secret"
INTERNAL_USER = PeerWorkersContext.ADMIN_USERNAME
CUSTOM_AUTH = {INTERNAL_USER: "adminpass", "user1": "user1pass", "user2": "user2pass"}


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_build_and_deploy(ops_test: OpsTest, kafka_connect_charm):
    """Deploys kafka-connect charm along kafka (in KRaft mode)."""
    await asyncio.gather(
        ops_test.model.deploy(
            kafka_connect_charm,
            application_name=APP_NAME,
            resources={PLUGIN_RESOURCE_KEY: "./tests/integration/resources/FakeResource.tar"},
            num_units=1,
            series="jammy",
            config={"profile": "testing"},
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

    await ops_test.model.add_relation(APP_NAME, KAFKA_APP)
    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, KAFKA_APP], idle_period=30, timeout=1800, status="active"
        )


@pytest.mark.abort_on_fail
async def test_add_auth_secret(ops_test: OpsTest):
    """Checks the flow for defining custom username/passwords on Kafka Connect REST interface through user-defined secrets."""
    # add secret
    secret_id = await ops_test.model.add_secret(
        name=TEST_SECRET_NAME, data_args=[f"{u}={p}" for u, p in CUSTOM_AUTH.items()]
    )
    # grant access to our app
    await ops_test.model.grant_secret(secret_name=TEST_SECRET_NAME, application=APP_NAME)
    # configure the app to use the secret_id
    await ops_test.model.applications[APP_NAME].set_config({AUTH_SECRET_CONFIG_KEY: secret_id})

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(apps=[APP_NAME], idle_period=30, timeout=600)
        await asyncio.sleep(60)

    for username, password in CUSTOM_AUTH.items():
        response = await make_connect_api_request(ops_test, custom_auth=(username, password))
        assert response.status_code == 200 if username == INTERNAL_USER else 401


@pytest.mark.abort_on_fail
async def test_after_scale_out(ops_test: OpsTest):
    """Checks custom username/passwords would be available on all units after scaling."""
    await ops_test.model.applications[APP_NAME].add_units(count=2)
    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            idle_period=30,
            timeout=1200,
            status="active",
            wait_for_exact_units=3,
            raise_on_error=False,
        )

    # now let's test all credentials on all units
    for unit, creds in product(ops_test.model.applications[APP_NAME].units, CUSTOM_AUTH.items()):
        username, password = creds
        response = await make_connect_api_request(
            ops_test, unit=unit, custom_auth=(username, password)
        )
        logger.info(f"Testing {username} on {unit.name}: {response.status_code}")
        assert response.status_code == 200 if username == INTERNAL_USER else 401


@pytest.mark.abort_on_fail
async def test_update_secret(ops_test: OpsTest):
    """Checks `update-secret` functionality."""
    # let's update admin password, remove user1 & user2 and add user3
    new_credentials = {"admin": "newadminpass", "user3": "user3pass"}

    await ops_test.model.update_secret(
        name=TEST_SECRET_NAME, data_args=[f"{u}={p}" for u, p in new_credentials.items()]
    )
    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME], idle_period=30, timeout=1200, status="active"
        )

    # now let's test old credentials on all units, they shouldn't work.
    for unit, creds in product(ops_test.model.applications[APP_NAME].units, CUSTOM_AUTH.items()):
        username, password = creds
        response = await make_connect_api_request(
            ops_test, unit=unit, custom_auth=(username, password)
        )
        logger.info(f"Testing {username} on {unit.name}: {response.status_code}")
        assert response.status_code == 401

    # and new credentials should work!
    for unit, creds in product(
        ops_test.model.applications[APP_NAME].units, new_credentials.items()
    ):
        username, password = creds
        response = await make_connect_api_request(
            ops_test, unit=unit, custom_auth=(username, password)
        )
        logger.info(f"Testing {username} on {unit.name}: {response.status_code}")
        assert response.status_code == 200 if username == INTERNAL_USER else 401


@pytest.mark.abort_on_fail
async def test_remove_admin_user_is_safe(ops_test: OpsTest):
    """Checks removing admin user from the user-defined secret wouldn't affect cluster functionality."""
    secret_id = await ops_test.model.add_secret(name="new-secret", data_args=["user4=user4pass"])
    await ops_test.model.grant_secret(secret_name="new-secret", application=APP_NAME)

    await ops_test.model.applications[APP_NAME].set_config({AUTH_SECRET_CONFIG_KEY: secret_id})

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME], idle_period=30, timeout=1200, status="active"
        )

    for unit in ops_test.model.applications[APP_NAME].units:
        # If we don't provide `custom_auth` argument, `make_connect_api_request` will read
        # admin credentials from passwords file on the unit.
        response = await make_connect_api_request(ops_test, unit=unit)
        assert response.status_code == 200
