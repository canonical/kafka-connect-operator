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
    POSTGRES_APP,
    POSTGRES_CHANNEL,
    DatabaseFixtureParams,
    assert_connector_statuses,
    download_file,
    get_unit_ipv4_address,
    run_command_on_unit,
)
from pytest_operator.plugin import OpsTest

from literals import PLUGIN_RESOURCE_KEY

MYSQL_INTEGRATOR = "mysql-source-integrator"
MYSQL_DB = "test_db"
POSTGRES_INTEGRATOR = "postgres-sink-integrator"
POSTGRES_DB = "sink_db"


logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_build_and_deploy(ops_test: OpsTest, kafka_connect_charm):
    """Deploys a basic test setup with Kafka, Kafka Connect, MySQL, and PostgreSQL."""
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
        ops_test.model.deploy(
            MYSQL_APP,
            channel=MYSQL_CHANNEL,
            application_name=MYSQL_APP,
            num_units=1,
            series="jammy",
        ),
        ops_test.model.deploy(
            POSTGRES_APP,
            channel=POSTGRES_CHANNEL,
            application_name=POSTGRES_APP,
            num_units=1,
            series="jammy",
        ),
    )

    await ops_test.model.add_relation(APP_NAME, KAFKA_APP)
    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, KAFKA_APP, MYSQL_APP, POSTGRES_APP],
            idle_period=30,
            timeout=1800,
            status="active",
        )


@pytest.mark.abort_on_fail
async def test_deploy_source_integrator(ops_test: OpsTest, source_integrator_charm):
    """Deploys MySQL source integrator."""
    with tempfile.TemporaryDirectory() as temp_dir:
        plugin_path = f"{temp_dir}/jdbc-plugin.tar"
        logging.info(f"Downloading JDBC connectors from {JDBC_CONNECTOR_DOWNLOAD_LINK}...")
        download_file(JDBC_CONNECTOR_DOWNLOAD_LINK, plugin_path)
        logging.info("Download finished successfully.")

        await ops_test.model.deploy(
            source_integrator_charm,
            application_name=MYSQL_INTEGRATOR,
            resources={PLUGIN_RESOURCE_KEY: plugin_path},
            config={"mode": "source"},
        )

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[MYSQL_INTEGRATOR], idle_period=30, timeout=1800, status="blocked"
        )


@pytest.mark.abort_on_fail
async def test_activate_source_integrator(ops_test: OpsTest):
    """Checks source integrator becomes active after related with MySQL."""
    # our source mysql integrator need a mysql_client relation to unblock:
    await ops_test.model.add_relation(f"{MYSQL_INTEGRATOR}:data", MYSQL_APP)
    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[MYSQL_INTEGRATOR, MYSQL_APP], idle_period=30, timeout=600
        )

    assert ops_test.model.applications[MYSQL_INTEGRATOR].status == "active"
    assert "UNKNOWN" in ops_test.model.applications[MYSQL_INTEGRATOR].status_message


@pytest.mark.abort_on_fail
@pytest.mark.parametrize(
    "mysql_test_data",
    [DatabaseFixtureParams(app_name="mysql", db_name=MYSQL_DB, no_tables=1, no_records=20)],
    indirect=True,
)
async def test_relate_with_connect_starts_source_integrator(ops_test: OpsTest, mysql_test_data):
    """Checks source integrator task starts after relation with Kafka Connect."""
    # Hopefully, mysql_test_data fixture has filled our db with some test data.
    # Now it's time relate to Kafka Connect to start the task.
    logger.info("Loaded 20 records into source MySQL DB.")
    await ops_test.model.add_relation(MYSQL_INTEGRATOR, APP_NAME)

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[MYSQL_INTEGRATOR, APP_NAME], idle_period=30, timeout=600
        )

    assert ops_test.model.applications[MYSQL_INTEGRATOR].status == "active"

    logging.info("Sleeping for a minute...")
    async with ops_test.fast_forward(fast_interval="20s"):
        await asyncio.sleep(60)

    # test task is running
    assert "RUNNING" in ops_test.model.applications[MYSQL_INTEGRATOR].status_message


@pytest.mark.abort_on_fail
async def test_deploy_postgres_sink_integrator(ops_test: OpsTest, sink_integrator_charm):
    """Deploys PostgreSQL sink integrator."""
    with tempfile.TemporaryDirectory() as temp_dir:
        plugin_path = f"{temp_dir}/jdbc-plugin.tar"
        logging.info(f"Downloading JDBC connectors from {JDBC_CONNECTOR_DOWNLOAD_LINK}...")
        download_file(JDBC_CONNECTOR_DOWNLOAD_LINK, plugin_path)
        logging.info("Download finished successfully.")

        await ops_test.model.deploy(
            sink_integrator_charm,
            application_name=POSTGRES_INTEGRATOR,
            resources={PLUGIN_RESOURCE_KEY: plugin_path},
            config={"mode": "sink", "topics_regex": "test_.+"},
        )

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[POSTGRES_INTEGRATOR], idle_period=30, timeout=1800, status="blocked"
        )


@pytest.mark.abort_on_fail
async def test_activate_sink_integrator(ops_test: OpsTest):
    """Checks sink integrator becomes active after related with PostgreSQL."""
    # our sink postgres integrator need a postgresql relation to unblock:
    await ops_test.model.add_relation(f"{POSTGRES_INTEGRATOR}:data", POSTGRES_APP)
    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[MYSQL_INTEGRATOR, MYSQL_APP], idle_period=30, timeout=600
        )

    assert ops_test.model.applications[POSTGRES_INTEGRATOR].status == "active"
    assert "UNKNOWN" in ops_test.model.applications[POSTGRES_INTEGRATOR].status_message


@pytest.mark.abort_on_fail
async def test_relate_with_connect_starts_sink_integrator(ops_test: OpsTest):
    """Checks sink task starts after related with Kafka Connect and ensures records are being loaded into PostgreSQL sink db."""
    await ops_test.model.add_relation(POSTGRES_INTEGRATOR, APP_NAME)

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[POSTGRES_INTEGRATOR, APP_NAME], idle_period=30, timeout=600
        )

    assert ops_test.model.applications[POSTGRES_INTEGRATOR].status == "active"

    logging.info("Sleeping for a minute...")
    async with ops_test.fast_forward(fast_interval="20s"):
        await asyncio.sleep(60)

    assert "RUNNING" in ops_test.model.applications[POSTGRES_INTEGRATOR].status_message

    # Besides just checking the task status, we assert the end-to-end functionality
    # End-to-end test: we should have 20 records loaded into postgres:
    postgres_leader = ops_test.model.applications[POSTGRES_APP].units[0]
    postgres_host = await get_unit_ipv4_address(ops_test, postgres_leader)

    get_pass_action = await postgres_leader.run_action("get-password", mode="full", dryrun=False)
    response = await get_pass_action.wait()
    root_pass = response.results.get("password")

    res = await run_command_on_unit(
        ops_test,
        postgres_leader,
        f"psql postgresql://operator:{root_pass}@{postgres_host}:5432/{POSTGRES_DB} -c 'SELECT COUNT(*) FROM \"test_table_1\"'",
    )

    logger.info("Checking number of records in sink Postgres DB (should be 20):")
    print(res.stdout)
    assert "20" in res.stdout


@pytest.mark.abort_on_fail
async def test_relation_broken(ops_test: OpsTest):
    """Checks `relation-broken` stops the connectors."""
    await assert_connector_statuses(ops_test, running=2)

    await ops_test.juju("remove-relation", APP_NAME, POSTGRES_INTEGRATOR)
    async with ops_test.fast_forward(fast_interval="30s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, POSTGRES_INTEGRATOR], idle_period=60, timeout=600
        )

    await assert_connector_statuses(ops_test, running=1)

    # relate again with connect
    await ops_test.model.add_relation(APP_NAME, POSTGRES_INTEGRATOR)
    async with ops_test.fast_forward(fast_interval="30s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, POSTGRES_INTEGRATOR], idle_period=30, timeout=600
        )
        await asyncio.sleep(120)

    # new connector should show up in RUNNING state,
    # while previous connector should be in STOPPED state.
    await assert_connector_statuses(ops_test, running=2)
