#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import requests
from helpers import APP_NAME, KAFKA_APP, KAFKA_CHANNEL, Testbed
from juju.model import Model
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


GRAFANA_AGENT = "grafana-agent"
COS_CHANNEL = "edge"

PROMETHEUS_OFFER = "prometheus-receive-remote-write"
LOKI_OFFER = "loki-logging"
GRAFANA_OFFER = "grafana-dashboards"

# assertions
DASHBOARD_TITLE = "Kafka Connect Cluster"
PANELS_COUNT = 0
PANELS_TO_CHECK = (
    "Tasks Total",
    "Tasks Running",
    "Status of connectors",
    "Status of tasks",
    "CPU Usage",
)
ALERTS_COUNT = 3
LOG_STREAMS = ("/var/snap/charmed-kafka/common/var/log/connect/server.log",)


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_deploy_charms(
    testbed: Testbed, cos_lite: Model, test_model: Model, kafka_connect_charm: Path
):

    await asyncio.gather(
        test_model.deploy(kafka_connect_charm, APP_NAME),
        test_model.deploy(
            KAFKA_APP, KAFKA_APP, channel=KAFKA_CHANNEL, config={"roles": "broker,controller"}
        ),
        test_model.deploy(GRAFANA_AGENT, GRAFANA_AGENT, channel=COS_CHANNEL),
    )

    await test_model.add_relation(APP_NAME, KAFKA_APP)

    await test_model.consume(
        f"{testbed.microk8s_controller}:admin/{cos_lite.name}.{PROMETHEUS_OFFER}"
    )
    await test_model.consume(f"{testbed.microk8s_controller}:admin/{cos_lite.name}.{LOKI_OFFER}")
    await test_model.consume(
        f"{testbed.microk8s_controller}:admin/{cos_lite.name}.{GRAFANA_OFFER}"
    )

    await test_model.relate(GRAFANA_AGENT, APP_NAME)
    await test_model.relate(GRAFANA_AGENT, PROMETHEUS_OFFER)
    await test_model.relate(GRAFANA_AGENT, LOKI_OFFER)
    await test_model.relate(GRAFANA_AGENT, GRAFANA_OFFER)

    await test_model.wait_for_idle(idle_period=30, timeout=1800, status="active")

    os.system(f"juju status --relations -m {testbed.lxd_controller}:{test_model.name}")
    os.system(f"juju status --relations -m {testbed.microk8s_controller}:{cos_lite.name}")


@pytest.mark.abort_on_fail
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

    # assert len(panels) == PANELS_COUNT

    panel_titles = [_panel.get("title") for _panel in panels]

    logger.warning(
        f"{len([i for i in panel_titles if not i])} panels don't have title which might be an issue."
    )
    logger.warning(
        f'{len([i for i in panel_titles if i and i.title() != i])} panels don\'t observe "Panel Title" format.'
    )

    for item in PANELS_TO_CHECK:
        assert item in panel_titles

    logger.info(f"{DASHBOARD_TITLE} dashboard has following panels:")
    for panel in panel_titles:
        logger.info(f"|__ {panel}")


@pytest.mark.abort_on_fail
async def test_metrics_and_alerts(cos_lite: Model):
    # wait a couple of minutes for metrics to show up
    logging.info("Sleeping for 5 min.")
    await asyncio.sleep(300)

    traefik_unit = cos_lite.applications["traefik"].units[0]
    action = await traefik_unit.run_action("show-proxied-endpoints")
    response = await action.wait()

    prometheus_url = json.loads(response.results["proxied-endpoints"])["prometheus/0"]["url"]

    # metrics

    response = requests.get(f"{prometheus_url}/api/v1/label/__name__/values").json()
    metrics = [i for i in response["data"] if APP_NAME in i]

    assert metrics, f"No {APP_NAME} metrics found!"
    logger.info(f'{len(metrics)} metrics found for "{APP_NAME}" in prometheus.')

    # alerts

    response = requests.get(f"{prometheus_url}/api/v1/rules?type=alert").json()

    match = [group for group in response["data"]["groups"] if APP_NAME in group["name"].lower()]

    assert match

    alerts = match[0]

    assert len(alerts["rules"]) == ALERTS_COUNT

    logger.info("Following alert rules are registered:")
    for rule in alerts["rules"]:
        logger.info(f'|__ {rule["name"]}')


@pytest.mark.abort_on_fail
async def test_loki(cos_lite: Model):
    traefik_unit = cos_lite.applications["traefik"].units[0]
    action = await traefik_unit.run_action("show-proxied-endpoints")
    response = await action.wait()

    loki_url = json.loads(response.results["proxied-endpoints"])["loki/0"]["url"]

    endpoint = f"{loki_url}/loki/api/v1/query_range"
    headers = {"Accept": "application/json"}

    start_time = (datetime.now(timezone.utc) - timedelta(hours=4)).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = {"query": f'{{juju_application="{APP_NAME}"}} |= ``', "start": start_time}

    response = requests.get(endpoint, params=payload, headers=headers, verify=False)
    results = response.json()["data"]["result"]
    streams = [i["stream"]["filename"] for i in results]

    assert len(streams) >= len(LOG_STREAMS)
    for _stream in LOG_STREAMS:
        assert _stream in streams

    logger.info("Displaying some of the logs pushed to loki:")
    for item in results:
        # we should have some logs
        assert len(item["values"]) > 0, f'No log pushed for {item["stream"]["filename"]}'

        logger.info(f'Stream: {item["stream"]["filename"]}')
        for _, log in item["values"][:10]:
            logger.info(f"|__ {log}")
        logger.info("\n")
