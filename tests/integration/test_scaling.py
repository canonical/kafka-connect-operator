import asyncio
import logging
import tempfile

import pytest
from helpers import (
    APP_NAME,
    JDBC_CONNECTOR_DOWNLOAD_LINK,
    KAFKA_APP,
    MYSQL_APP,
    MYSQL_CHANNEL,
    DatabaseFixtureParams,
    deploy_kafka,
    destroy_active_workers,
    download_file,
    get_unit_ipv4_address,
    make_connect_api_request,
)
from pytest_operator.plugin import OpsTest

from literals import PLUGIN_RESOURCE_KEY

logger = logging.getLogger(__name__)


MYSQL_DB = "test_db"
INTEGRATOR = "integrator"


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_build_and_deploy(ops_test: OpsTest, kafka_version: int, kafka_connect_charm):
    """Deploys kafka-connect charm along kafka (in KRaft mode) & MySQL."""
    await asyncio.gather(
        ops_test.model.deploy(
            kafka_connect_charm,
            application_name=APP_NAME,
            resources={PLUGIN_RESOURCE_KEY: "./tests/integration/resources/FakeResource.tar"},
            num_units=1,
            series="noble",
            # config={"profile": "testing"},
        ),
        deploy_kafka(ops_test, kafka_version),
        ops_test.model.deploy(
            MYSQL_APP,
            channel=MYSQL_CHANNEL,
            application_name=MYSQL_APP,
            num_units=1,
            series="jammy",
        ),
    )

    await ops_test.model.add_relation(APP_NAME, KAFKA_APP)
    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, KAFKA_APP, MYSQL_APP], idle_period=30, timeout=1800, status="active"
        )


@pytest.mark.abort_on_fail
async def test_deploy_integrator(ops_test: OpsTest, source_integrator_charm):
    """Deploys MySQL source integrator."""
    with tempfile.TemporaryDirectory() as temp_dir:
        plugin_path = f"{temp_dir}/jdbc-plugin.tar"
        logging.info(f"Downloading JDBC connectors from {JDBC_CONNECTOR_DOWNLOAD_LINK}...")
        download_file(JDBC_CONNECTOR_DOWNLOAD_LINK, plugin_path)
        logging.info("Download finished successfully.")

        await ops_test.model.deploy(
            source_integrator_charm,
            application_name=INTEGRATOR,
            resources={PLUGIN_RESOURCE_KEY: plugin_path},
            config={"mode": "source"},
        )

    await ops_test.model.add_relation(INTEGRATOR, MYSQL_APP)
    await ops_test.model.add_relation(INTEGRATOR, APP_NAME)

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[INTEGRATOR], idle_period=30, timeout=1800, status="active"
        )


@pytest.mark.abort_on_fail
@pytest.mark.parametrize(
    "mysql_test_data",
    [DatabaseFixtureParams(app_name="mysql", db_name=MYSQL_DB, no_tables=1, no_records=93)],
    indirect=True,
)
async def test_load_data(ops_test: OpsTest, mysql_test_data):
    """Loads test data into MySQL DB and ensures connector transitions into RUNNING state."""
    # Hopefully, mysql_test_data fixture has filled our db with some test data.
    # Now it's time relate to Kafka Connect to start the task.
    logger.info("Loaded 93 records into source MySQL DB.")

    async with ops_test.fast_forward(fast_interval="30s"):
        await asyncio.sleep(120)

    assert "RUNNING" in ops_test.model.applications[INTEGRATOR].status_message


@pytest.mark.abort_on_fail
async def test_scale_out(ops_test: OpsTest):
    await ops_test.model.applications[APP_NAME].add_units(count=2)
    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME], idle_period=30, timeout=1200, status="active", wait_for_exact_units=3
        )

    async with ops_test.fast_forward(fast_interval="30s"):
        await ops_test.model.block_until(
            lambda: "RUNNING" in ops_test.model.applications[INTEGRATOR].status_message,
            timeout=600,
            wait_period=15,
        )

    for unit in ops_test.model.applications[APP_NAME].units:
        status_resp = await make_connect_api_request(
            ops_test, unit=unit, endpoint="connectors?expand=status"
        )
        assert status_resp.status_code == 200
        status_json = status_resp.json()
        for connector in status_json:
            assert status_json[connector]["status"]["connector"]["state"] == "RUNNING"


@pytest.mark.abort_on_fail
async def test_destroy_active_workers(ops_test: OpsTest):
    """Checks scaling in functionality by destroying workers with active connectors.

    This test ensures that connector tasks are resumed on remaining worker(s).
    """
    while len(ops_test.model.applications[APP_NAME].units) > 1:
        await destroy_active_workers(ops_test)

        async with ops_test.fast_forward(fast_interval="60s"):
            await ops_test.model.wait_for_idle(
                apps=[INTEGRATOR, APP_NAME],
                idle_period=30,
                timeout=600,
                status="active",
                raise_on_error=False,
            )

    async with ops_test.fast_forward(fast_interval="30s"):
        await asyncio.sleep(150)

    status_resp = await make_connect_api_request(ops_test, endpoint="connectors?expand=status")
    remaining_unit = ops_test.model.applications[APP_NAME].units[0]
    assert {
        item["status"]["connector"]["worker_id"].split(":")[0]
        for item in status_resp.json().values()
    } == {await get_unit_ipv4_address(ops_test, remaining_unit)}
