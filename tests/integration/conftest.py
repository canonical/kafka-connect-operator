#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.


import logging
import os

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
    setup_commands = """
        # install microk8s
        sudo snap install microk8s --classic --channel=1.32

        # configure microk8s
        sudo usermod -a -G microk8s $USER
        mkdir -p ~/.kube
        chmod 0700 ~/.kube

        # ensure microk8s is up
        microk8s status --wait-ready

        # enable required addons
        microk8s enable dns
        microk8s enable hostpath-storage
        sudo apt install jq
        IPADDR=$(ip -4 -j route get 2.2.2.2 | jq -r '.[] | .prefsrc')
        microk8s enable metallb:$IPADDR-$IPADDR

        # configure & bootstrap microk8s controller
        sudo mkdir -p /var/snap/juju/current/microk8s/credentials
        microk8s config | sudo tee /var/snap/juju/current/microk8s/credentials/client.config
        sudo chown -R $USER:$USER /var/snap/juju/current/microk8s/credentials

        juju bootstrap microk8s
    """

    for line in setup_commands.split("\n"):
        command = line.strip()

        if not command or command.startswith("#"):
            continue

        logger.info(command)
        ret_code = os.system(command)

        if not ret_code:
            raise OSError(f'command "{command}" failed with error code {ret_code}')
