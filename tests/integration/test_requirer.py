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
    download_file,
    get_unit_ipv4_address,
    run_command_on_unit,
)
from ops import Unit
from pytest_operator.plugin import OpsTest

from literals import PLUGIN_RESOURCE_KEY

MYSQL_INTEGRATOR = "mysql-source-integrator"
MYSQL_DB = "test_db"
POSTGRES_INTEGRATOR = "postgres-sink-integrator"
POSTGRES_DB = "sink_db"


logger = logging.getLogger(__name__)


async def load_implementation(ops_test: OpsTest, unit: Unit, implementation_name: str):
    """Dynamically load an integrator implementation on specified unit."""
    logger.info(f"Using {implementation_name} for {unit.name}")

    unit_name_with_dash = unit.name.replace("/", "-")
    base_path = f"/var/lib/juju/agents/unit-{unit_name_with_dash}/charm/src"

    ret_code, _, _ = await ops_test.juju(
        "ssh",
        unit.name,
        f"sudo cp {base_path}/implementations/{implementation_name}.py {base_path}/integrator.py",
    )

    assert not ret_code


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_build_and_deploy(ops_test: OpsTest, kafka_connect_charm):
    """Deploys a basic test setup with Kafka, Kafka Connect, MySQL, and PostgreSQL."""
    await asyncio.gather(
        ops_test.model.deploy(kafka_connect_charm, application_name=APP_NAME, series="jammy"),
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


@pytest.mark.skip
@pytest.mark.abort_on_fail
async def test_deploy_source_integrator(ops_test: OpsTest, integrator_charm):
    """Deploys MySQL source integrator."""
    with tempfile.TemporaryDirectory() as temp_dir:
        plugin_path = f"{temp_dir}/jdbc-plugin.tar"
        logging.info(f"Downloading JDBC connectors from {JDBC_CONNECTOR_DOWNLOAD_LINK}...")
        download_file(JDBC_CONNECTOR_DOWNLOAD_LINK, plugin_path)
        logging.info("Download finished successfully.")

        await ops_test.model.deploy(
            integrator_charm,
            application_name=MYSQL_INTEGRATOR,
            resources={PLUGIN_RESOURCE_KEY: plugin_path},
        )

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[MYSQL_INTEGRATOR], idle_period=30, timeout=1800, status="blocked"
        )

    # Dynamically load `mysql` implementation for this unit
    unit = ops_test.model.applications[MYSQL_INTEGRATOR].units[0]
    await load_implementation(ops_test, unit, "mysql")


@pytest.mark.skip
@pytest.mark.abort_on_fail
async def test_activate_source_integrator(ops_test: OpsTest):
    """Checks source integrator becomes active after related with MySQL."""
    # our source mysql integrator need a mysql_client relation to unblock:
    await ops_test.model.add_relation(f"{MYSQL_INTEGRATOR}:source", MYSQL_APP)
    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[MYSQL_INTEGRATOR, MYSQL_APP], idle_period=30, timeout=600
        )

    assert ops_test.model.applications[MYSQL_INTEGRATOR].status == "active"
    assert "UNASSIGNED" in ops_test.model.applications[MYSQL_INTEGRATOR].status_message


@pytest.mark.skip
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
async def test_deploy_postgres_sink_integrator(ops_test: OpsTest, integrator_charm):
    """Deploys PostgreSQL sink integrator."""
    with tempfile.TemporaryDirectory() as temp_dir:
        plugin_path = f"{temp_dir}/jdbc-plugin.tar"
        logging.info(f"Downloading JDBC connectors from {JDBC_CONNECTOR_DOWNLOAD_LINK}...")
        download_file(JDBC_CONNECTOR_DOWNLOAD_LINK, plugin_path)
        logging.info("Download finished successfully.")

        await ops_test.model.deploy(
            integrator_charm,
            application_name=POSTGRES_INTEGRATOR,
            resources={PLUGIN_RESOURCE_KEY: plugin_path},
            config={"psql_topic_regex": "test_.+"},
        )

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[POSTGRES_INTEGRATOR], idle_period=30, timeout=1800, status="blocked"
        )

    # Dynamically load `postgres` implementation for this unit
    unit = ops_test.model.applications[POSTGRES_INTEGRATOR].units[0]
    await load_implementation(ops_test, unit, "postgres")


@pytest.mark.abort_on_fail
async def test_activate_sink_integrator(ops_test: OpsTest):
    """Checks sink integrator becomes active after related with PostgreSQL."""
    # our sink postgres integrator need a postgresql relation to unblock:
    await ops_test.model.add_relation(f"{POSTGRES_INTEGRATOR}:sink", POSTGRES_APP)
    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[MYSQL_INTEGRATOR, MYSQL_APP], idle_period=30, timeout=600
        )

    assert ops_test.model.applications[POSTGRES_INTEGRATOR].status == "active"
    assert "UNASSIGNED" in ops_test.model.applications[POSTGRES_INTEGRATOR].status_message


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
