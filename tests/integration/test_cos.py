#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import logging
import os

import pytest
import requests
from helpers import Testbed
from juju.model import Model
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


GRAFANA_AGENT = "grafana-agent"
KAFKA = "kafka"

PROMETHEUS_OFFER = "prometheus-receive-remote-write"
LOKI_OFFER = "loki-logging"
GRAFANA_OFFER = "grafana-dashboards"

# Grafana assertions
DASHBOARD_TITLE = "Kafka Metrics"
PANELS_COUNT = 44
PANELS_TO_CHECK = (
    "JVM",
    "Brokers Online",
    "Active Controllers",
    "Total of Topics",
)


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_deploy_charms(testbed: Testbed, cos_lite: Model, test_model: Model):

    await asyncio.gather(
        test_model.deploy(KAFKA, KAFKA, channel="3/edge", config={"roles": "broker,controller"}),
        test_model.deploy(GRAFANA_AGENT, GRAFANA_AGENT, channel="edge"),
    )

    await test_model.wait_for_idle(apps=[KAFKA], status="active", idle_period=60, timeout=1200)

    await test_model.consume(
        f"{testbed.microk8s_controller}:admin/{cos_lite.name}.{PROMETHEUS_OFFER}"
    )
    await test_model.consume(f"{testbed.microk8s_controller}:admin/{cos_lite.name}.{LOKI_OFFER}")
    await test_model.consume(
        f"{testbed.microk8s_controller}:admin/{cos_lite.name}.{GRAFANA_OFFER}"
    )

    await test_model.relate(GRAFANA_AGENT, KAFKA)
    await test_model.relate(GRAFANA_AGENT, PROMETHEUS_OFFER)
    await test_model.relate(GRAFANA_AGENT, LOKI_OFFER)
    await test_model.relate(GRAFANA_AGENT, GRAFANA_OFFER)

    await test_model.wait_for_idle(idle_period=30, timeout=600, status="active")

    os.system(f"juju status --relations -m {testbed.lxd_controller}:{test_model.name}")
    os.system(f"juju status --relations -m {testbed.microk8s_controller}:{cos_lite.name}")


async def test_grafana(cos_lite: Model):
    grafana_unit = cos_lite.applications["grafana"].units[0]
    action = await grafana_unit.run_action("get-admin-password")
    response = await action.wait()

    grafana_url = response.results.get("url")
    admin_password = response.results.get("admin-password")

    auth = HTTPBasicAuth("admin", admin_password)
    dashboards = requests.get(f"{grafana_url}/api/search?query=kafka", auth=auth).json()

    assert dashboards

    match = [dash for dash in dashboards if dash["title"] == DASHBOARD_TITLE]

    assert match

    app_dashboard = match[0]
    dashboard_uid = app_dashboard["uid"]
    # dashboard_id = app_dashboard["id"]
    # dashboard_url = app_dashboard["url"]

    details = requests.get(f"{grafana_url}/api/dashboards/uid/{dashboard_uid}", auth=auth).json()

    panels = details["dashboard"]["panels"]

    assert len(panels) == PANELS_COUNT

    panel_titles = [_panel.get("title") for _panel in panels]

    logger.warning(
        f"{len([i for i in panel_titles if not i])} panels don't have title which might be an issue."
    )
    logger.warning(
        f'{len([i for i in panel_titles if i and i.title() != i])} panels don\'t observe "Panel Title" format.'
    )

    for item in PANELS_TO_CHECK:
        assert item in panel_titles
