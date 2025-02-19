#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
import os

import pytest
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_deploy_charms(ops_test: OpsTest, juju_microk8s):
    logger.info("Microk8s controller set up successfully!")

    os.system("juju switch localhost-localhost")
    os.system("juju models")
