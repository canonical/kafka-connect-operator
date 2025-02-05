#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of globals common to the Integrator Charm."""

import os
from typing import Literal

Substrates = Literal["vm", "k8s"]

CHARM_KEY = "kafka-connect-integrator"
CHARM_DIR = os.environ.get("CHARM_DIR", "")
SUBSTRATE = "vm"

PEER_REL = "peer"
SOURCE_REL = "source"
SINK_REL = "sink"

SERVICE_NAME = "integrator-rest"
SERVICE_PATH = f"/etc/systemd/system/{SERVICE_NAME}.service"
USER = "root"
GROUP = "root"

REST_PORT = 8080
PLUGIN_RESOURCE_KEY = "connect-plugin"
PLUGIN_FILE_PATH = f"{CHARM_DIR}/src/rest/resources/connector-plugin.tar"
