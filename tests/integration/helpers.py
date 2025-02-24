#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import asyncio
import json
import logging
import os
import re
import socket
import ssl
import tempfile
from contextlib import asynccontextmanager, closing
from dataclasses import dataclass
from pathlib import Path
from subprocess import PIPE, check_output
from typing import Optional, cast

import requests
import yaml
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.x509 import Certificate, load_pem_x509_certificate
from cryptography.x509.extensions import SubjectAlternativeName
from cryptography.x509.oid import ExtensionOID
from juju.errors import JujuConnectionError
from juju.model import Model
from ops.model import Unit
from pytest_operator.plugin import OpsTest
from requests.auth import HTTPBasicAuth

from core.models import PeerWorkersContext
from literals import CONFIG_DIR, DEFAULT_API_PORT

logger = logging.getLogger(__name__)


METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
PASSWORDS_PATH = f"{CONFIG_DIR}/connect.password"
KAFKA_APP = "kafka"
KAFKA_CHANNEL = "3/edge"
MYSQL_APP = "mysql"
MYSQL_CHANNEL = "8.0/stable"

JDBC_CONNECTOR_DOWNLOAD_LINK = "https://github.com/Aiven-Open/jdbc-connector-for-apache-kafka/releases/download/v6.10.0/jdbc-connector-for-apache-kafka-6.10.0.tar"
JDBC_SOURCE_CONNECTOR_CLASS = "io.aiven.connect.jdbc.JdbcSourceConnector"
JDBC_SINK_CONNECTOR_CLASS = "io.aiven.connect.jdbc.JdbcSinkConnector"

S3_CONNECTOR_LINK = "https://github.com/Aiven-Open/cloud-storage-connectors-for-apache-kafka/releases/download/v3.1.0/s3-sink-connector-for-apache-kafka-3.1.0.tar"
S3_CONNECTOR_CLASS = "io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector"


@dataclass
class CommandResult:
    return_code: int | None
    stdout: str
    stderr: str


def check_socket(host: str | None, port: int) -> bool:
    """Checks whether IPv4 socket is up or not."""
    if host is None:
        return False

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        return sock.connect_ex((host, port)) == 0


async def run_command_on_unit(
    ops_test: OpsTest, unit: Unit, command: str | list[str]
) -> CommandResult:
    """Runs a command on a given unit and returns the result."""
    command_args = command.split() if isinstance(command, str) else command
    return_code, stdout, stderr = await ops_test.juju("ssh", f"{unit.name}", *command_args)

    return CommandResult(return_code=return_code, stdout=stdout, stderr=stderr)


async def get_unit_ipv4_address(ops_test: OpsTest, unit: Unit) -> str | None:
    """A safer alternative for `juju.unit.get_public_address()` which is robust to network changes."""
    _, stdout, _ = await ops_test.juju("ssh", f"{unit.name}", "hostname -i")
    ipv4_matches = re.findall(r"[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}", stdout)

    if ipv4_matches:
        return ipv4_matches[0]

    return None


async def check_connect_endpoints_status(
    ops_test: OpsTest, app_name: str = APP_NAME, port: int = DEFAULT_API_PORT, verbose: bool = True
) -> dict[Unit, bool]:
    """Returns a dict of unit: status mapping where status is True if endpoint is up and False otherwise."""
    status = {}
    units = ops_test.model.applications[app_name].units

    for unit in units:
        ipv4_address = await get_unit_ipv4_address(ops_test, unit)
        status[unit] = check_socket(ipv4_address, port)

    if verbose:
        print(status)

    return status


async def get_admin_password(ops_test: OpsTest, unit: Unit) -> str:
    """Get admin user's password of a unit by reading credentials file."""
    res = await run_command_on_unit(ops_test, unit, f"sudo cat {PASSWORDS_PATH}")
    raw = res.stdout.strip().split("\n")

    if not raw:
        raise Exception(f"Unable to read the credentials file on unit {unit.name}.")

    for line in raw:
        if line.startswith(PeerWorkersContext.ADMIN_USERNAME):
            return line.split(":")[-1].strip()

    raise Exception(f"Admin user not defined in the credentials file on unit {unit.name}.")


async def make_api_request(
    ops_test: OpsTest,
    unit: Unit | None = None,
    method: str = "GET",
    endpoint: str = "",
    proto: str = "http",
    port: int = DEFAULT_API_PORT,
    auth_enabled: bool = True,
    custom_auth: tuple[str, str] | None = None,
    verbose: bool = True,
    **kwargs,
) -> requests.Response:
    """Makes a request to a REST Endpoint and returns the response.

    Args:
        ops_test (OpsTest): OpsTest object
        unit (Unit, optional): Unit used to make the request; if not supplied, uses the first unit in the Kafka Connect application `APP_NAME`.
        method (str, optional): Request method. Defaults to "GET".
        endpoint (str, optional): API endpoint. Defaults to "".
        proto (str, optional): HTTP Protocol: "http" or "https". Defaults to "http".
        port (int, optional): TCP Port. defaults to `DEFAULT_API_PORT`.
        auth_enabled (bool, optional): Whether should use authentication on the endpoint, defaults to True.
        custom_auth (tuple[str, str], optional): A (username, password) tuple to be used for HTTP Basic authentication.
        verbose (bool, optional): Enable verbose logging. Defaults to True.
        kwargs: Keyword arguments which will be passed to `requests.request` method.

    Returns:
        requests.Response: Response object.
    """
    target_unit = ops_test.model.applications[APP_NAME].units[0] if unit is None else unit

    unit_ip = await get_unit_ipv4_address(ops_test, target_unit)
    url = f"{proto}://{unit_ip}:{port}/{endpoint}"

    auth = None
    if auth_enabled:
        admin_password = await get_admin_password(ops_test, unit=target_unit)
        auth = (
            HTTPBasicAuth(PeerWorkersContext.ADMIN_USERNAME, admin_password)
            if not custom_auth
            else HTTPBasicAuth(*custom_auth)
        )

    response = requests.request(method, url, auth=auth, **kwargs)

    if verbose:
        print(f"{method} - {url}: {response.content}")

    return response


make_connect_api_request = make_api_request


def download_file(url: str, dst_path: str):
    """Downloads a file from given `url` to `dst_path`."""
    response = requests.get(url, stream=True)
    with open(dst_path, mode="wb") as file:
        for chunk in response.iter_content(chunk_size=10 * 1024):
            file.write(chunk)


def build_mysql_db_init_queries(
    test_db_host: str, test_db_user: str, test_db_pass: str, test_db_name: str
) -> list[str]:
    """Returns a list of queries to initiate a test MySQL database with 1 table and 3 sample entries."""
    raw = f"""CREATE DATABASE {test_db_name};
    CREATE USER '{test_db_user}'@'{test_db_host}' IDENTIFIED BY '{test_db_pass}';
    GRANT ALL PRIVILEGES ON {test_db_name}.* TO '{test_db_user}'@'{test_db_host}' WITH GRANT OPTION;
    CREATE USER '{test_db_user}'@'%' IDENTIFIED BY '{test_db_pass}';
    GRANT ALL PRIVILEGES ON {test_db_name}.* TO '{test_db_user}'@'%' WITH GRANT OPTION;
    CREATE TABLE {test_db_name}.products (
        id int NOT NULL AUTO_INCREMENT,
        name varchar(50) DEFAULT NULL,
        price float DEFAULT NULL,
        created_at timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY product_id_uindex (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    INSERT INTO {test_db_name}.products (name, price) Values ('p1', 10.0), ('p2', 20.0), ('p3', 30.0);
    """

    queries = [query.strip().replace("\n", " ") for query in raw.split(";")]
    return [query for query in queries if query]


def search_secrets(ops_test: OpsTest, owner: str, search_key: str) -> str:
    """Searches secrets for a provided `search_key` and returns it if found, otherwise return empty string."""
    secrets_meta_raw = check_output(
        f"JUJU_MODEL={ops_test.model_full_name} juju list-secrets --format json",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    ).strip()
    secrets_meta = json.loads(secrets_meta_raw)

    for secret_id in secrets_meta:
        if owner and not secrets_meta[secret_id]["owner"] == owner:
            continue

        secrets_data_raw = check_output(
            f"JUJU_MODEL={ops_test.model_full_name} juju show-secret --format json --reveal {secret_id}",
            stderr=PIPE,
            shell=True,
            universal_newlines=True,
        )

        secret_data = json.loads(secrets_data_raw)
        if search_key in secret_data[secret_id]["content"]["Data"]:
            return secret_data[secret_id]["content"]["Data"][search_key]

    return ""


async def get_certificate(
    ops_test: OpsTest, unit: Unit | None = None, port: int = DEFAULT_API_PORT
) -> Certificate:
    """Gets TLS certificate of a particular unit using a socket.

    Args:
        ops_test (OpsTest): OpsTest object
        unit (Unit | None, optional): Unit used to establish the socket; if not supplied, uses the first unit in the Kafka Connect application `APP_NAME`.
        port (int, optional): Socket port. Defaults to DEFAULT_API_PORT.

    Returns:
        Certificate: Unit's certificate used on the socket.
    """
    target_unit = ops_test.model.applications[APP_NAME].units[0] if unit is None else unit
    unit_ip = await get_unit_ipv4_address(ops_test, target_unit)

    pem = ssl.get_server_certificate((f"{unit_ip}", port))
    return load_pem_x509_certificate(str.encode(pem), default_backend())


def extract_sans(cert: Certificate) -> list[str]:
    """Returns the list of Subject Alternative Names (SANs) of a given certificate."""
    ext = cert.extensions.get_extension_for_oid(ExtensionOID.SUBJECT_ALTERNATIVE_NAME)
    val = cast(SubjectAlternativeName, ext.value)
    return val.get_values_for_type(x509.DNSName)


@asynccontextmanager
async def self_signed_ca(ops_test: OpsTest, app_name: str):
    """Returns a context manager with self-signed-certificates operator CA file."""
    action_name: str = "get-ca-certificate"
    unit = ops_test.model.applications[app_name].units[0]
    action = await unit.run_action(action_name=action_name)
    result = await action.wait()
    ca = result.results.get("ca-certificate")

    with tempfile.NamedTemporaryFile(mode="w", delete_on_close=False) as ca_file:
        ca_file.write(ca)
        ca_file.close()
        yield ca_file


class Testbed:
    """Fixture for handling multi-cloud juju deployments."""

    def __init__(self, ops_test: OpsTest):
        self._ops_test = ops_test
        logging.warning("When using Testbed, avoid interacting directly with ops_test.")
        pass

    def exec(self, cmd: str) -> str:
        """Executes a command on shell and returns the result."""
        return check_output(
            cmd,
            stderr=PIPE,
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

    def juju(self, *args, json_output=True) -> dict:
        """Runs a juju command and returns the result in JSON format."""
        _format_json = "" if not json_output else "--format json"
        res = self.exec(f"juju {' '.join(args)} {_format_json}")

        if json_output:
            return json.loads(res)

        return {}

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
            self.juju("add-model", "-c", controller_name, model_name, json_output=False)
            await asyncio.sleep(10)
            return await self.get_model(controller_name, model_name)

    def bootstrap_microk8s(self) -> None:
        """Bootstrap juju on microk8s cloud."""
        user_env_var = os.environ.get("USER", "root")
        os.system("sudo apt install -y jq")
        ip_addr = self.exec("ip -4 -j route get 2.2.2.2 | jq -r '.[] | .prefsrc'").strip()

        self.run_script(
            f"""
            # install microk8s
            sudo snap install microk8s --classic --channel=1.32

            # configure microk8s
            sudo usermod -a -G microk8s {user_env_var}
            mkdir -p ~/.kube
            chmod 0700 ~/.kube

            # ensure microk8s is up
            sudo microk8s status --wait-ready

            # enable required addons
            sudo microk8s enable dns
            sudo microk8s enable hostpath-storage
            sudo microk8s enable metallb:{ip_addr}-{ip_addr}

            # configure & bootstrap microk8s controller
            sudo mkdir -p /var/snap/juju/current/microk8s/credentials
            sudo microk8s config | sudo tee /var/snap/juju/current/microk8s/credentials/client.config
            sudo chown -R {user_env_var}:{user_env_var} /var/snap/juju/current/microk8s/credentials

            juju bootstrap microk8s
            sleep 90
        """
        )

    @property
    def lxd_controller(self) -> Optional[str]:
        """Returns the lxd controller name or None if not available."""
        return self.get_controller_name("localhost")

    @property
    def microk8s_controller(self) -> Optional[str]:
        """Returns the microk8s controller name or None if not available."""
        return self.get_controller_name("microk8s")
