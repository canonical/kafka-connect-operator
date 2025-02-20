#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.


import asyncio
import logging
import os
import subprocess

import pytest
from deployment import Deployment
from juju.errors import JujuConnectionError
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


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


@pytest.fixture(scope="session")
def deployment():
    return Deployment()


@pytest.fixture(scope="module")
def microk8s_controller(deployment: Deployment):
    """Returns the microk8s controller name, boots up a new one if not existent."""
    if deployment.microk8s_controller:
        logger.info(
            f"Microk8s controller {deployment.microk8s_controller} exists, skipping setup..."
        )
        return deployment.microk8s_controller

    user_env_var = os.environ.get("USER", "root")
    os.system("sudo apt install -y jq")
    ip_addr = subprocess.check_output(
        "ip -4 -j route get 2.2.2.2 | jq -r '.[] | .prefsrc'",
        universal_newlines=True,
        stderr=subprocess.PIPE,
        shell=True,
    ).strip()

    deployment.run_script(
        f"""
        # install microk8s
        sudo snap install microk8s --classic --channel=1.32

        # configure microk8s
        sudo usermod -a -G microk8s {user_env_var}
        mkdir -p ~/.kube
        chmod 0700 ~/.kube

        # ensure microk8s is up
        sudo microk8s status --wait-ready

        # enable required addons
        sudo microk8s enable dns
        sudo microk8s enable hostpath-storage
        sudo microk8s enable metallb:{ip_addr}-{ip_addr}

        # configure & bootstrap microk8s controller
        sudo mkdir -p /var/snap/juju/current/microk8s/credentials
        sudo microk8s config | sudo tee /var/snap/juju/current/microk8s/credentials/client.config
        sudo chown -R {user_env_var}:{user_env_var} /var/snap/juju/current/microk8s/credentials

        juju bootstrap microk8s
        sleep 90
    """
    )

    return "microk8s-localhost"


@pytest.fixture(scope="module")
async def cos_lite(microk8s_controller: str, deployment: Deployment):
    """Returns the COS-lite model, deploys a new one if not existent."""
    cos_model_name = "cos"

    try:
        model = await deployment.get_model(microk8s_controller, cos_model_name)
        yield model

        await model.disconnect()
        return
    except JujuConnectionError:
        logger.info(f"Model {cos_model_name} doesn't exist, trying to deploy...")

    deployment.run_script(
        f"""
        # deploy COS-Lite
        juju switch {microk8s_controller}
        juju add-model {cos_model_name}
        juju switch {cos_model_name}

        curl -L https://raw.githubusercontent.com/canonical/cos-lite-bundle/main/overlays/offers-overlay.yaml -O
        curl -L https://raw.githubusercontent.com/canonical/cos-lite-bundle/main/overlays/storage-small-overlay.yaml -O

        juju deploy cos-lite --trust --overlay ./offers-overlay.yaml --overlay ./storage-small-overlay.yaml

        rm ./offers-overlay.yaml ./storage-small-overlay.yaml
    """
    )

    await asyncio.sleep(60)
    model = await deployment.get_model(microk8s_controller, cos_model_name)

    await model.wait_for_idle(status="active", idle_period=60, timeout=3000, raise_on_error=False)
    yield model

    await model.disconnect()


@pytest.fixture(scope="module")
async def test_model(ops_test: OpsTest, deployment: Deployment):
    """Returns the ops_test model on lxd cloud."""
    if not deployment.lxd_controller or not ops_test.model:
        raise Exception("Can't communicate with the controller.")

    model = await deployment.get_or_create_model(deployment.lxd_controller, ops_test.model.name)
    yield model

    await model.disconnect()
