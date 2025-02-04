import asyncio
import logging
import os
import tempfile

import pytest
from helpers import (
    APP_NAME,
    JDBC_CONNECTOR_DOWNLOAD_LINK,
    JDBC_SINK_CONNECTOR_CLASS,
    JDBC_SOURCE_CONNECTOR_CLASS,
    KAFKA_APP,
    KAFKA_CHANNEL,
    MYSQL_APP,
    MYSQL_CHANNEL,
    S3_CONNECTOR_CLASS,
    S3_CONNECTOR_LINK,
    build_mysql_db_init_queries,
    download_file,
    get_unit_ipv4_address,
    make_connect_api_request,
)
from pytest_operator.plugin import OpsTest

from literals import PLUGIN_RESOURCE_KEY

logger = logging.getLogger(__name__)


TEST_DB_USER = "testuser"
TEST_DB_PASS = "testpass"
TEST_DB_NAME = "testdb"
TEST_TASK_NAME = "test_task"


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_build_and_deploy(ops_test: OpsTest, kafka_connect_charm):
    """Deploys kafka-connect charm along kafka (in KRaft mode) & MySQL."""
    await asyncio.gather(
        ops_test.model.deploy(
            kafka_connect_charm,
            application_name=APP_NAME,
            resources={PLUGIN_RESOURCE_KEY: "./tests/integration/resources/FakeResource.tar"},
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
            apps=[APP_NAME, KAFKA_APP, MYSQL_APP], idle_period=30, timeout=1800
        )

    assert ops_test.model.applications[APP_NAME].status == "active"
    assert ops_test.model.applications[MYSQL_APP].status == "active"


@pytest.mark.abort_on_fail
async def test_add_plugin(ops_test: OpsTest):
    """Checks attach-resource functionality using Aiven JDBC connector and ensures JDBC source/sink connector plugins are added."""
    with tempfile.TemporaryDirectory() as temp_dir:
        plugin_path = f"{temp_dir}/jdbc-plugin.tar"
        logging.info(f"Downloading JDBC connectors from {JDBC_CONNECTOR_DOWNLOAD_LINK}...")
        download_file(JDBC_CONNECTOR_DOWNLOAD_LINK, plugin_path)
        logging.info("Download finished successfully.")
        # attach resource
        ops_test.model.applications[APP_NAME].attach_resource(
            PLUGIN_RESOURCE_KEY,
            file_name=os.path.basename(plugin_path),
            file_obj=open(plugin_path, "rb"),
        )

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(apps=[APP_NAME], idle_period=30, timeout=600)

    response = await make_connect_api_request(ops_test, method="GET", endpoint="connector-plugins")
    assert response.status_code == 200

    connector_classes = [c.get("class") for c in response.json()]

    assert JDBC_SOURCE_CONNECTOR_CLASS in connector_classes
    assert JDBC_SINK_CONNECTOR_CLASS in connector_classes


@pytest.mark.abort_on_fail
async def test_mysql_setup(ops_test: OpsTest):
    """Bootstraps MySQL database with test data and ensures testbed integrity."""
    mysql_leader = ops_test.model.applications[MYSQL_APP].units[0]
    get_pass_action = await mysql_leader.run_action("get-password", mode="full", dryrun=False)
    response = await get_pass_action.wait()

    mysql_root_pass = response.results.get("password")
    mysql_host = await get_unit_ipv4_address(ops_test, mysql_leader)

    for query in build_mysql_db_init_queries(
        test_db_host=str(mysql_host),
        test_db_user=TEST_DB_USER,
        test_db_pass=TEST_DB_PASS,
        test_db_name=TEST_DB_NAME,
    ):
        cmd = f'mysql -h 127.0.0.1 -u root -p{mysql_root_pass} -e "{query}"'
        print(cmd.replace(mysql_root_pass, "******").replace(TEST_DB_PASS, "******"))
        return_code, _, _ = await ops_test.juju("ssh", f"{mysql_leader.name}", cmd)
        assert return_code == 0


@pytest.mark.abort_on_fail
async def test_add_task(ops_test: OpsTest):
    """Checks whether connector plugin is usable or not by adding a sample test task using MySQL source."""
    mysql_leader = ops_test.model.applications[MYSQL_APP].units[0]
    mysql_host = await get_unit_ipv4_address(ops_test, mysql_leader)

    task_json = {
        "name": TEST_TASK_NAME,
        "config": {
            "mode": "bulk",
            "connector.class": JDBC_SOURCE_CONNECTOR_CLASS,
            "topics": "test",
            "connection.url": f"jdbc:mysql://{mysql_host}:3306/{TEST_DB_NAME}",
            "connection.user": TEST_DB_USER,
            "connection.password": TEST_DB_PASS,
            "topic.prefix": "test_etl_",
            "tasks.max": "1",
            "auto.create": True,
            "auto.evolve": True,
            "insert.mode": "upsert",
            "pk.mode": "record_key",
            "pk.fields": "id",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        },
    }

    response = await make_connect_api_request(
        ops_test, method="POST", endpoint="connectors", json=task_json
    )

    assert response.status_code == 201


@pytest.mark.abort_on_fail
async def test_task_is_running(ops_test: OpsTest):
    """Checks whether the added task is in RUNNING state."""
    # wait for 60s
    await asyncio.sleep(60)

    tasks_response = await make_connect_api_request(
        ops_test, method="GET", endpoint=f"connectors/{TEST_TASK_NAME}/tasks", timeout=10
    )

    assert tasks_response.status_code == 200
    tasks = tasks_response.json()
    assert len(tasks) == 1  # 1 task should be submitted here

    task_id = tasks[0].get("id", {}).get("task", 0)
    status_response = await make_connect_api_request(
        ops_test,
        method="GET",
        endpoint=f"connectors/{TEST_TASK_NAME}/tasks/{task_id}/status",
        timeout=10,
    )

    assert status_response.status_code == 200
    assert status_response.json().get("state") == "RUNNING"


@pytest.mark.abort_on_fail
async def test_add_another_plugin(ops_test: OpsTest):
    """Checks attaching new plugins work as expected, preserving the old ones."""
    with tempfile.TemporaryDirectory() as temp_dir:
        plugin_path = f"{temp_dir}/jdbc-plugin.tar"
        logging.info(f"Downloading S3 connectors from {S3_CONNECTOR_LINK}...")
        download_file(S3_CONNECTOR_LINK, plugin_path)
        logging.info("Download finished successfully.")
        # attach resource
        ops_test.model.applications[APP_NAME].attach_resource(
            PLUGIN_RESOURCE_KEY,
            file_name=os.path.basename(plugin_path),
            file_obj=open(plugin_path, "rb"),
        )

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(apps=[APP_NAME], idle_period=30, timeout=600)

    response = await make_connect_api_request(ops_test, method="GET", endpoint="connector-plugins")
    assert response.status_code == 200

    connector_classes = [c.get("class") for c in response.json()]

    assert S3_CONNECTOR_CLASS in connector_classes
    assert JDBC_SOURCE_CONNECTOR_CLASS in connector_classes
    assert JDBC_SINK_CONNECTOR_CLASS in connector_classes
