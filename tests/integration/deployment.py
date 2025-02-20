import asyncio
import json
import logging
import os
import subprocess
from typing import Optional

from juju.errors import JujuConnectionError
from juju.model import Model
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


class Deployment:
    def __init__(self):
        pass

    def exec(self, cmd: str) -> str:
        """Executes a command on shell and returns the result."""
        return subprocess.check_output(
            cmd,
            stderr=subprocess.PIPE,
            shell=True,
            universal_newlines=True,
        )

    def run_script(self, script: str) -> None:
        """Runs a script on Linux OS.

        Args:
            script (str): Bash script

        Raises:
            OSError: If the script run fails.
        """
        for line in script.split("\n"):
            command = line.strip()

            if not command or command.startswith("#"):
                continue

            logger.info(command)
            ret_code = os.system(command)

            if ret_code:
                raise OSError(f'command "{command}" failed with error code {ret_code}')

    def juju(self, *args) -> dict:
        """Runs a juju command and returns the result in JSON format."""
        res = self.exec(f"juju {' '.join(args)} --format json")
        return json.loads(res)

    def get_controller_name(self, cloud: str) -> Optional[str]:
        """Gets controller name for specified cloud, e.g. localhost, microk8s, lxd, etc."""
        res = self.juju("controllers")
        for controller in res.get("controllers", {}):
            if res["controllers"][controller].get("cloud") == cloud:
                return controller

        return None

    async def get_model(self, controller_name: str, model_name: str) -> Model:
        """Gets a juju model on specified controller, raises JujuConnectionError if not found."""
        state = await OpsTest._connect_to_model(controller_name, model_name)
        return state.model

    async def get_or_create_model(self, controller_name: str, model_name: str) -> Model:
        """Returns an existing model on a controller or creates new one if not existing."""
        try:
            return await self.get_model(controller_name, model_name)
        except JujuConnectionError:
            self.juju("add-model", "-c", controller_name, model_name)
            await asyncio.sleep(10)
            return await self.get_model(controller_name, model_name)

    @property
    def lxd_controller(self) -> Optional[str]:
        """Returns the lxd controller name or None if not available."""
        return self.get_controller_name("localhost")

    @property
    def microk8s_controller(self) -> Optional[str]:
        """Returns the microk8s controller name or None if not available."""
        return self.get_controller_name("microk8s")
