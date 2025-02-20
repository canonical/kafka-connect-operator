#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import logging
import os

import pytest
from deployment import Deployment
from juju.model import Model

logger = logging.getLogger(__name__)


GRAFANA_AGENT = "grafana-agent"
KAFKA = "kafka"

PROMETHEUS_OFFER = "prometheus-receive-remote-write"
LOKI_OFFER = "loki-logging"
GRAFANA_OFFER = "grafana-dashboards"


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_deploy_charms(deployment: Deployment, cos_lite: Model, test_model: Model):

    await asyncio.gather(
        test_model.deploy(KAFKA, KAFKA, channel="3/edge", config={"roles": "broker,controller"}),
        test_model.deploy(GRAFANA_AGENT, GRAFANA_AGENT, channel="edge"),
    )

    await test_model.wait_for_idle(apps=[KAFKA], status="active", idle_period=60, timeout=1200)

    await test_model.consume(
        f"{deployment.microk8s_controller}:admin/{cos_lite.name}.{PROMETHEUS_OFFER}"
    )
    await test_model.consume(
        f"{deployment.microk8s_controller}:admin/{cos_lite.name}.{LOKI_OFFER}"
    )
    await test_model.consume(
        f"{deployment.microk8s_controller}:admin/{cos_lite.name}.{GRAFANA_OFFER}"
    )

    await test_model.relate(GRAFANA_AGENT, KAFKA)
    await test_model.relate(GRAFANA_AGENT, PROMETHEUS_OFFER)
    await test_model.relate(GRAFANA_AGENT, LOKI_OFFER)
    await test_model.relate(GRAFANA_AGENT, GRAFANA_OFFER)

    await test_model.wait_for_idle(idle_period=30, timeout=600, status="active")

    os.system(f"juju status --relations -m {deployment.lxd_controller}:{test_model.name}")
    os.system(f"juju status --relations -m {deployment.microk8s_controller}:{cos_lite.name}")
