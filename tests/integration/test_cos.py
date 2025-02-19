#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import os
import subprocess

import pytest
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_deploy_charms(ops_test: OpsTest, juju_microk8s):
    logger.info("Microk8s controller set up successfully!")

    raw = subprocess.check_output(
        "juju controllers --format json | jq -r '.controllers | keys'",
        shell=True,
        universal_newlines=True,
    )
    controllers = json.loads(raw)

    lxd_controller = None
    if match := [k for k in controllers if k != "microk8s-localhost"]:
        lxd_controller = match[0]

    os.system(f"juju switch {lxd_controller}")
    os.system("juju models")
