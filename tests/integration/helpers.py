#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import re
import socket
import ssl
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import requests
import yaml
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.x509 import Certificate, load_pem_x509_certificate
from cryptography.x509.extensions import SubjectAlternativeName
from cryptography.x509.oid import ExtensionOID
from ops.model import Unit
from pytest_operator.plugin import OpsTest

from literals import DEFAULT_API_PORT

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
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


async def make_connect_api_request(
    ops_test: OpsTest,
    unit: Unit | None = None,
    method: str = "GET",
    endpoint: str = "",
    proto: str = "http",
    verbose: bool = True,
    **kwargs,
) -> requests.Response:
    """Makes a request to Kafka Connect API and returns the response.

    Args:
        ops_test (OpsTest): OpsTest object
        unit (Unit, optional): Connect worker unit used to make the request; if not supplied, uses the first unit in the Kafka Connect application `APP_NAME`.
        method (str, optional): Request method. Defaults to "GET".
        endpoint (str, optional): Connect API endpoint. Defaults to "".
        proto (str, optional): Connect API Protocol: "http" or "https". Defaults to "http".
        verbose (bool, optional): Enable verbose logging. Defaults to True.
        kwargs: Keyword arguments which will be passed to `requests.request` method.

    Returns:
        requests.Response: Response object.
    """
    target_unit = ops_test.model.applications[APP_NAME].units[0] if unit is None else unit

    unit_ip = await get_unit_ipv4_address(ops_test, target_unit)
    url = f"{proto}://{unit_ip}:{DEFAULT_API_PORT}/{endpoint}"

    response = requests.request(method, url, **kwargs)

    if verbose:
        print(f"{method} - {url}: {response.json()}")

    return response


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
