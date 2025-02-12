#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Basic Integrator implementation which does nothing."""

from charms.kafka_connect.v0.integrator import BaseConfigFormatter, BaseIntegrator
from ops.charm import CharmBase
from typing_extensions import override


class ConfigFormatter(BaseConfigFormatter):
    pass


class Integrator(BaseIntegrator):

    name = "test-integrator"
    mode = "source"
    formatter = ConfigFormatter

    def __init__(self, charm: CharmBase, plugin_url: str):

        super().__init__(charm=charm, plugin_url=plugin_url)

    @override
    def setup(self) -> None:
        return

    @override
    def teardown(self) -> None:
        return

    @property
    @override
    def ready(self) -> bool:
        return False
