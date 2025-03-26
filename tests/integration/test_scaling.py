import asyncio
import logging
import tempfile

import pytest
from helpers import (
    APP_NAME,
    JDBC_CONNECTOR_DOWNLOAD_LINK,
    KAFKA_APP,
    KAFKA_CHANNEL,
    MYSQL_APP,
    MYSQL_CHANNEL,
    DatabaseFixtureParams,
    download_file,
    get_unit_ipv4_address,
    make_connect_api_request,
)
from pytest_operator.plugin import OpsTest

from literals import PLUGIN_RESOURCE_KEY

logger = logging.getLogger(__name__)


MYSQL_DB = "test_db"
INTEGRATOR = "integrator"


async def destroy_active_workers(ops_test: OpsTest):
    """Finds the unit with active connector task(s) and destroys it."""
    status_resp = await make_connect_api_request(ops_test, endpoint="connectors?expand=status")

    workers = {
        item["status"]["connector"]["worker_id"].split(":")[0]
        for item in status_resp.json().values()
    }

    for unit in ops_test.model.applications[APP_NAME].units:
        if await get_unit_ipv4_address(ops_test, unit) in workers:
            logger.info(f"Destroying {unit}")
            await ops_test.model.applications[APP_NAME].destroy_units(unit.name)


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_build_and_deploy(ops_test: OpsTest, kafka_connect_charm):
    """Deploys kafka-connect charm along kafka (in KRaft mode) & MySQL."""
    await asyncio.gather(
        ops_test.model.deploy(
            kafka_connect_charm,
            application_name=APP_NAME,
            resources={PLUGIN_RESOURCE_KEY: "./tests/integration/resources/FakeResource.tar"},
            num_units=3,
            series="jammy",
            # config={"profile": "testing"},
        ),
        ops_test.model.deploy(
            KAFKA_APP,
            channel=KAFKA_CHANNEL,
            application_name=KAFKA_APP,
            num_units=1,
            series="jammy",
            config={"roles": "broker,controller"},
        ),
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


async def test_destroy_active_workers(ops_test: OpsTest):
    """Checks scaling in functionality by destroying workers with active connectors.

    This test ensures that connector tasks are resumed on remaining worker(s).
    """
    for _ in range(2):
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
        await asyncio.sleep(120)

    assert "RUNNING" in ops_test.model.applications[INTEGRATOR].status_message
