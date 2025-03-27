import asyncio
import logging
import tempfile

import pytest
from helpers import (
    APP_NAME,
    JDBC_CONNECTOR_DOWNLOAD_LINK,
    JDBC_SOURCE_CONNECTOR_CLASS,
    KAFKA_APP,
    KAFKA_CHANNEL,
    download_file,
    make_api_request,
    make_connect_api_request,
    search_secrets,
)
from pytest_operator.plugin import OpsTest

from literals import PLUGIN_RESOURCE_KEY

logger = logging.getLogger(__name__)

INTEGRATOR_APP = "test-integrator"
INTEGRATOR_PORT = 8080
USERNAME_CACHE_KEY = "integrator-username"
PASSWORD_CACHE_KEY = "integrator-password"


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_deploy_app_and_integrator(
    ops_test: OpsTest, kafka_connect_charm, source_integrator_charm
):

    # download JDBC connector plugin and deploy the integrator charm with it.
    with tempfile.TemporaryDirectory() as temp_dir:
        plugin_path = f"{temp_dir}/jdbc-plugin.tar"
        logging.info(f"Downloading JDBC connectors from {JDBC_CONNECTOR_DOWNLOAD_LINK}...")
        download_file(JDBC_CONNECTOR_DOWNLOAD_LINK, plugin_path)
        logging.info("Download finished successfully.")

        await ops_test.model.deploy(
            source_integrator_charm,
            application_name=INTEGRATOR_APP,
            resources={PLUGIN_RESOURCE_KEY: plugin_path},
            config={"mode": "source"},
        )

    # deploy kafka & kafka connect
    await asyncio.gather(
        ops_test.model.deploy(
            kafka_connect_charm,
            application_name=APP_NAME,
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
            apps=[APP_NAME, KAFKA_APP, INTEGRATOR_APP], idle_period=60, timeout=1800
        )

    assert ops_test.model.applications[APP_NAME].status == "active"
    # dummy integrator boots up with blocked status, because BaseIntegrator.ready returns False.
    assert ops_test.model.applications[INTEGRATOR_APP].status == "blocked"


@pytest.mark.abort_on_fail
async def test_rest_endpoints_before_integration(ops_test: OpsTest):
    # assert connect is up
    response = await make_connect_api_request(ops_test)

    assert response.status_code == 200

    # assert connect doesn't have JDBC plugins loaded
    response = await make_connect_api_request(ops_test, method="GET", endpoint="connector-plugins")
    assert response.status_code == 200

    connector_classes = [c.get("class") for c in response.json()]

    assert JDBC_SOURCE_CONNECTOR_CLASS not in connector_classes


@pytest.mark.abort_on_fail
async def test_integrate(ops_test: OpsTest, request: pytest.FixtureRequest):
    """Tests the integration functionality between Kafka Connect and integrator charm."""
    await ops_test.model.add_relation(APP_NAME, INTEGRATOR_APP)

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, INTEGRATOR_APP], idle_period=30, timeout=600
        )

    # assert connect have JDBC plugins loaded after integration
    response = await make_connect_api_request(ops_test, method="GET", endpoint="connector-plugins")
    connector_classes = [c.get("class") for c in response.json()]
    assert JDBC_SOURCE_CONNECTOR_CLASS in connector_classes

    username = search_secrets(ops_test, owner=APP_NAME, search_key="username")
    password = search_secrets(ops_test, owner=APP_NAME, search_key="password")

    # cache the values for next tests
    request.config.cache.set(USERNAME_CACHE_KEY, username)
    request.config.cache.set(PASSWORD_CACHE_KEY, password)

    assert username.startswith("relation-")

    # make sure the credentials work.
    response = await make_api_request(ops_test, custom_auth=(username, password))
    assert response.status_code == 200

    # and the REST API is indeed protected.
    response = await make_api_request(ops_test, custom_auth=(username, "wrong-password"))
    assert response.status_code == 401


@pytest.mark.abort_on_fail
async def test_remove_integration(ops_test: OpsTest, request: pytest.FixtureRequest):
    """Tests a broken integration leads to plugins being removed and credentials being revoked."""
    username = request.config.cache.get(USERNAME_CACHE_KEY, "")
    password = request.config.cache.get(PASSWORD_CACHE_KEY, "")

    await ops_test.juju("remove-relation", APP_NAME, INTEGRATOR_APP)

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, INTEGRATOR_APP], idle_period=30, timeout=600
        )

    # assert connect has removed JDBC plugins after integration is removed
    response = await make_connect_api_request(ops_test, method="GET", endpoint="connector-plugins")
    connector_classes = [c.get("class") for c in response.json()]
    assert JDBC_SOURCE_CONNECTOR_CLASS not in connector_classes

    assert username.startswith("relation-")

    # make sure the credentials don't work anymore.
    response = await make_api_request(ops_test, custom_auth=(username, password))
    assert response.status_code == 401
