#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.


import pytest
from pytest_operator.plugin import OpsTest


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
