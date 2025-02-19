#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.


import logging
import os
import subprocess

import pytest
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
def juju_microk8s():
    user_env_var = os.environ.get("USER", "root")
    os.system("sudo apt install -y jq")
    ip_addr = subprocess.check_output(
        "ip -4 -j route get 2.2.2.2 | jq -r '.[] | .prefsrc'",
        universal_newlines=True,
        stderr=subprocess.PIPE,
        shell=True,
    ).strip()

    setup_commands = f"""
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

        # deploy COS
        juju switch microk8s-localhost
        juju add-model cos
        juju switch cos

        curl -L https://raw.githubusercontent.com/canonical/cos-lite-bundle/main/overlays/offers-overlay.yaml -O
        curl -L https://raw.githubusercontent.com/canonical/cos-lite-bundle/main/overlays/storage-small-overlay.yaml -O

        juju deploy cos-lite --trust --overlay ./offers-overlay.yaml --overlay ./storage-small-overlay.yaml
        sleep 300
        juju status --relations
    """

    for line in setup_commands.split("\n"):
        command = line.strip()

        if not command or command.startswith("#"):
            continue

        logger.info(command)
        ret_code = os.system(command)

        if ret_code:
            raise OSError(f'command "{command}" failed with error code {ret_code}')
