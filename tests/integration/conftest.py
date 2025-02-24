#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.


import asyncio
import logging

import pytest
from helpers import Testbed
from juju.errors import JujuConnectionError
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


def pytest_addoption(parser):
    """Defines pytest parsers."""
    parser.addoption("--cos-model", action="store", help="COS model name", default="cos")

    parser.addoption(
        "--cos-channel", action="store", help="COS-lite bundle channel", default="edge"
    )


@pytest.fixture(scope="module")
async def kafka_connect_charm(ops_test: OpsTest):
    """Build the application charm."""
    charm_path = "."
    charm = await ops_test.build_charm(charm_path)
    return charm


@pytest.fixture(scope="module")
async def integrator_charm(ops_test: OpsTest):
    """Build the integrator charm."""
    charm_path = "./tests/integration/app-charm/"
    charm = await ops_test.build_charm(charm_path)
    return charm


@pytest.fixture(scope="module")
def testbed(ops_test: OpsTest):
    return Testbed(ops_test=ops_test)


@pytest.fixture(scope="module")
def microk8s_controller(testbed: Testbed):
    """Returns the microk8s controller name, boots up a new one if not existent."""
    if testbed.microk8s_controller:
        logger.info(f"Microk8s controller {testbed.microk8s_controller} exists, skipping setup...")
        return testbed.microk8s_controller

    testbed.bootstrap_microk8s()
    return "microk8s-localhost"


@pytest.fixture(scope="module")
async def cos_lite(microk8s_controller: str, testbed: Testbed, request: pytest.FixtureRequest):
    """Returns the COS-lite model, deploys a new one if not existent."""
    cos_model_name = f'{request.config.getoption("--cos-model")}'
    cos_channel = f'{request.config.getoption("--cos-channel")}'

    try:
        model = await testbed.get_model(microk8s_controller, cos_model_name)
        yield model

        await model.disconnect()
        return
    except JujuConnectionError:
        logger.info(f"Model {cos_model_name} doesn't exist, trying to deploy...")

    testbed.run_script(
        f"""
        # deploy COS-Lite
        juju switch {microk8s_controller}
        juju add-model {cos_model_name}
        juju switch {cos_model_name}

        curl -L https://raw.githubusercontent.com/canonical/cos-lite-bundle/main/overlays/offers-overlay.yaml -O
        curl -L https://raw.githubusercontent.com/canonical/cos-lite-bundle/main/overlays/storage-small-overlay.yaml -O

        juju deploy cos-lite --channel {cos_channel} --trust --overlay ./offers-overlay.yaml --overlay ./storage-small-overlay.yaml

        rm ./offers-overlay.yaml ./storage-small-overlay.yaml
    """
    )

    await asyncio.sleep(60)
    model = await testbed.get_model(microk8s_controller, cos_model_name)

    await model.wait_for_idle(status="active", idle_period=60, timeout=3000, raise_on_error=False)
    yield model

    await model.disconnect()


@pytest.fixture(scope="module")
async def test_model(ops_test: OpsTest, testbed: Testbed):
    """Returns the ops_test model on lxd cloud."""
    if not testbed.lxd_controller or not ops_test.model:
        raise Exception("Can't communicate with the controller.")

    model = await testbed.get_or_create_model(testbed.lxd_controller, ops_test.model.name)
    yield model

    await model.disconnect()
