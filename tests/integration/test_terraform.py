#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Simple Terraform smoke tests."""

import json
import logging
import os
import random

import pytest
from helpers import APP_NAME
from pytest_operator.plugin import OpsTest

from literals import Status

logger = logging.getLogger(__name__)


TFVARS_DEFAULTS = {
    "app_name": APP_NAME,
    "units": 3,
}
TFVARS_FILENAME = "test.tfvars.json"


def _deploy_terraform(tmp_path, tfvars: dict = {}) -> str:
    """Deploy the charm Terraform module, using a provided set of variables."""
    tf_path = tmp_path / "terraform"
    tf_path.mkdir()
    logger.info(f"Using {tf_path}")
    os.system(f"cp -R terraform/* {tf_path}/")

    _tfvars = TFVARS_DEFAULTS | tfvars
    with open(f"{tf_path}/{TFVARS_FILENAME}", "w") as f:
        json.dump(_tfvars, f, indent=4)

    ret_code = os.system(f"terraform -chdir={tf_path} init")
    assert not ret_code
    ret_code = os.system(
        f"terraform -chdir={tf_path} apply -var-file={tf_path}/{TFVARS_FILENAME} -auto-approve"
    )
    assert not ret_code

    return tf_path


def _destroy_terraform(working_dir: str) -> None:
    """Destroy the Terraform module and related tmp resources."""
    ret_code = os.system(
        f"terraform -chdir={working_dir} destroy -var-file={working_dir}/{TFVARS_FILENAME} -auto-approve"
    )
    assert not ret_code

    os.system(f"rm -rf {working_dir}")


@pytest.mark.abort_on_fail
async def test_deployment_active(ops_test: OpsTest, model_uuid: str, tmp_path):
    """Test that application is deployed and active."""
    working_dir = _deploy_terraform(tmp_path, tfvars={"model_uuid": model_uuid})

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], idle_period=30, timeout=900, status="blocked"
    )
    assert (
        ops_test.model.applications[APP_NAME].status_message
        == Status.MISSING_KAFKA.value.status.message
    )

    _destroy_terraform(working_dir)

    await ops_test.model.block_until(
        lambda: len(ops_test.model.units) == 0
        and len(ops_test.model.machines) == 0
        and len(ops_test.model.applications) == 0,
        timeout=900,
    )


@pytest.mark.abort_on_fail
async def test_deployment_on_machines(ops_test: OpsTest, model_uuid: str, tmp_path):
    """Test that `machines` TF variable work as expected."""
    # Add machines and wait for them to start
    await ops_test.juju("add-machine", "--base", "ubuntu@22.04", "-n", "3")

    await ops_test.model.block_until(
        lambda: len(ops_test.model.machines) == 3
        and {machine.agent_status for machine in ops_test.model.machines.values()} == {"started"},
        timeout=900,
    )

    machines = list(ops_test.model.machines)
    target_machine = random.choice(machines)

    # Deploy 1 unit on a target machine
    working_dir = _deploy_terraform(
        tmp_path, tfvars={"model_uuid": model_uuid, "machines": [target_machine]}
    )

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], idle_period=30, timeout=900, status="blocked"
    )
    assert (
        ops_test.model.applications[APP_NAME].status_message
        == Status.MISSING_KAFKA.value.status.message
    )

    status = ops_test.model.applications
    assert len(status[APP_NAME].units) == 1
    deployed_unit = next(iter(status[APP_NAME].units))
    assert deployed_unit.machine.id == target_machine

    _destroy_terraform(working_dir)

    await ops_test.model.block_until(
        lambda: len(ops_test.model.units) == 0
        and len(ops_test.model.machines) == 0
        and len(ops_test.model.applications) == 0,
        timeout=900,
    )
