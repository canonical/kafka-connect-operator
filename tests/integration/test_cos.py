#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import logging

import pytest
from deployment import Deployment
from juju.model import Model

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_deploy_charms(deployment: Deployment, cos_lite: Model, test_model: Model):

    await test_model.deploy(
        "kafka", "kafka", channel="3/edge", config={"roles": "broker,controller"}
    )

    await test_model.wait_for_idle(status="active", idle_period=60, timeout=1200)

    print(f"juju status --relations -m {deployment.lxd_controller}:{test_model.name}")
    print(f"juju status --relations -m {deployment.microk8s_controller}:{cos_lite.name}")
