#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import contextlib
import http.server
import json
import logging
import os
import ssl
import subprocess
from multiprocessing import Process
from typing import Mapping
from unittest.mock import MagicMock

import pytest
from charms.tls_certificates_interface.v3.tls_certificates import (
    generate_ca,
    generate_certificate,
    generate_csr,
    generate_private_key,
)
from src.core.models import Context as CharmContext
from src.core.models import TLSContext
from src.core.workload import WorkloadBase
from src.literals import SUBSTRATE
from src.managers.tls import Sans, TLSManager

logger = logging.getLogger(__name__)

UNIT_NAME = "kafka-connect/0"
INTERNAL_ADDRESS = "10.10.10.10"
BIND_ADDRESS = "10.20.20.20"
KEYTOOL = "keytool"
JKS_UNIT_TEST_FILE = "tests/unit/TestJKS.java"


def _exec(
    command: list[str] | str,
    env: Mapping[str, str] | None = None,
    working_dir: str | None = None,
    _: bool = False,
) -> str:
    _command = " ".join(command) if isinstance(command, list) else command
    print(_command)

    for bin in ("chown", "chmod"):
        if _command.startswith(bin):
            return "ok"

    try:
        output = subprocess.check_output(
            command,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            shell=isinstance(command, str),
            env=env,
            cwd=working_dir,
        )
        return output
    except subprocess.CalledProcessError as e:
        raise e


try:
    _exec(KEYTOOL)
    _exec("java -version")
    JAVA_TESTS_DISABLED = False
except subprocess.CalledProcessError:
    JAVA_TESTS_DISABLED = True


@contextlib.contextmanager
def simple_ssl_server(certfile: str, keyfile: str, port: int = 10443):
    httpd = http.server.HTTPServer(("127.0.0.1", port), http.server.SimpleHTTPRequestHandler)
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
    httpd.socket = ctx.wrap_socket(httpd.socket, server_side=True)

    process = Process(target=httpd.serve_forever)
    process.start()
    yield

    process.kill()


class JKSError(Exception):
    """Error raised when JKS unit test fails."""


def java_jks_test(truststore_path: str, truststor_password: str, ssl_server_port: int = 10443):
    cmd = [
        "java",
        "-Djavax.net.debug=ssl:handshake",
        f"-Djavax.net.ssl.trustStore={truststore_path}",
        f'-Djavax.net.ssl.trustStorePassword="{truststor_password}"',
        JKS_UNIT_TEST_FILE,
        f"https://localhost:{ssl_server_port}",
    ]

    if os.system(" ".join(cmd)):
        raise JKSError("JKS unit test failed, Check logs for details.")


@pytest.fixture()
def tls_manager(tmp_path_factory):
    """A TLSManager instance with minimal functioning mock `Workload` and `Context`."""
    mock_workload = MagicMock(spec=WorkloadBase)
    mock_workload.write = lambda content, path: open(path, "w").write(content)
    mock_workload.exec = _exec
    mock_workload.paths.config_dir = tmp_path_factory.mktemp("workload")
    mock_workload.paths.keystore = f"{mock_workload.paths.config_dir}/keystore.p12"
    mock_workload.paths.truststore = f"{mock_workload.paths.config_dir}/truststore.jks"

    mock_context = MagicMock(spec=CharmContext)

    ca_key = generate_private_key()
    ca = generate_ca(private_key=ca_key, subject="TEST-CA")

    intermediate_key = generate_private_key()
    intermediate_csr = generate_csr(private_key=intermediate_key, subject="INTERMEDIATE-CA")
    intermediate_cert = generate_certificate(intermediate_csr, ca, ca_key)

    private_key = generate_private_key()
    csr = generate_csr(
        private_key=private_key,
        subject=UNIT_NAME,
        sans_ip=[INTERNAL_ADDRESS],
        sans_dns=[UNIT_NAME],
    )
    cert = generate_certificate(csr, ca, ca_key)

    data = {
        TLSContext.CA: ca.decode("utf-8"),
        TLSContext.CHAIN: json.dumps([intermediate_cert.decode("utf-8")]),
        TLSContext.CERT: cert.decode("utf-8"),
        TLSContext.PRIVATE_KEY: private_key.decode("utf-8"),
        TLSContext.KEYSTORE_PASSWORD: "keystore-password",
        TLSContext.TRUSTSTORE_PASSWORD: "truststore-password",
    }
    tls_context = TLSContext(MagicMock(), MagicMock(), None)
    tls_context.relation_data = data

    mock_context.worker_unit.tls = tls_context
    mock_context.worker_unit.internal_address = INTERNAL_ADDRESS
    mock_context.worker_unit.unit.name = UNIT_NAME
    mock_context.bind_address = BIND_ADDRESS

    mgr = TLSManager(context=mock_context, workload=mock_workload, substrate=SUBSTRATE)
    mgr.keytool = KEYTOOL
    yield mgr


@pytest.mark.skipif(
    JAVA_TESTS_DISABLED, reason=f"Can't locate {KEYTOOL} and/or java in the test environment."
)
@pytest.mark.parametrize(
    "tls_initialized", [False, True], ids=["TLS NOT initialized", "TLS initialized"]
)
@pytest.mark.parametrize(
    "with_intermediate_ca", [False, True], ids=["NO intermediate CA", "ONE intermediate CA"]
)
def test_lifecycle(
    tls_manager: TLSManager,
    caplog: pytest.LogCaptureFixture,
    tls_initialized: bool,
    with_intermediate_ca: bool,
    tmp_path_factory,
) -> None:
    """Tests the lifecycle of adding/removing certs from Java and TLSManager points of view."""
    if not tls_initialized:
        tls_manager.tls_context.relation_data = {}
        tls_manager.context.peer_workers.relation_data = {"tls": ""}

    if not with_intermediate_ca and tls_initialized:
        del tls_manager.tls_context.relation_data[TLSContext.CHAIN]

    caplog.set_level(logging.DEBUG)
    tls_manager.set_ca()
    tls_manager.set_chain()
    tls_manager.set_server_key()
    tls_manager.set_certificate()
    tls_manager.set_bundle()
    tls_manager.configure()

    if not tls_initialized:
        return

    # build another cert
    app_ca_key = generate_private_key()
    app_ca = generate_ca(private_key=app_ca_key, subject="SOME-CA")
    app_key = generate_private_key()
    csr = generate_csr(
        app_key, subject="some-app/0", sans_dns=["localhost"], sans_ip=["127.0.0.1"]
    )
    app_cert = generate_certificate(csr, app_ca, app_ca_key)

    tmp_dir = tmp_path_factory.mktemp("someapp")
    app_certfile = f"{tmp_dir}/app.pem"
    app_keyfile = f"{tmp_dir}/app.key"

    open(app_certfile, "w").write(app_cert.decode("utf-8"))
    open(app_keyfile, "w").write(app_key.decode("utf-8"))

    with simple_ssl_server(certfile=app_certfile, keyfile=app_keyfile):
        # since we don't have the app cert/ca in our truststore, JKS test should fail.
        with pytest.raises(JKSError):
            java_jks_test(
                tls_manager.workload.paths.truststore, tls_manager.tls_context.truststore_password
            )

        # Add the app cert
        tls_manager.import_cert(
            alias="some-app", filename="some-app", cert_content=app_cert.decode("utf-8")
        )

        # now the test should pass
        java_jks_test(
            tls_manager.workload.paths.truststore, tls_manager.tls_context.truststore_password
        )

        # import again with the same alias
        tls_manager.import_cert(
            alias="some-app", filename="other-file", cert_content=app_cert.decode("utf-8")
        )

        # check remove cert functionality
        tls_manager.remove_cert("some-app")

        # We don't have the cert anymore, so the JKS test should fail again.
        with pytest.raises(JKSError):
            java_jks_test(
                tls_manager.workload.paths.truststore, tls_manager.tls_context.truststore_password
            )

        # Now add the app's CA cert instead of its own cert
        tls_manager.import_cert(
            alias="some-app-ca", filename="some-app-ca", cert_content=app_ca.decode("utf-8")
        )

        # the test should pass again
        java_jks_test(
            tls_manager.workload.paths.truststore, tls_manager.tls_context.truststore_password
        )

    # remove some non-existing alias.
    tls_manager.remove_cert("other-app")
    log_record = caplog.records[-1]
    assert "alias <other-app> does not exist" in log_record.msg.lower()
    assert log_record.levelname == "WARNING"

    # check SANs
    current_sans = tls_manager.get_current_sans()
    assert current_sans and current_sans == Sans(sans_ip=[INTERNAL_ADDRESS], sans_dns=[UNIT_NAME])
    expected_sans = tls_manager.build_sans()
    assert expected_sans.sans_ip == current_sans.sans_ip
    assert expected_sans.sans_dns != current_sans.sans_dns

    # since we didn't add our FQDN to the cert SANS, we expect a change being detected:
    assert tls_manager.sans_change_detected

    # if with_intermediate_ca:
    #     import pdb
    #     pdb.set_trace()


def test_simulate_os_errors(tls_manager: TLSManager):
    """Checks TLSManager functionality when random OS Errors happen."""

    def _erroneous_hook(*args, **kwargs):
        raise subprocess.CalledProcessError(
            returncode=1, cmd="command", stderr="Some error occurred"
        )

    tls_manager.workload.exec = _erroneous_hook
    tls_manager.workload.write = _erroneous_hook

    for method in dir(TLSManager):
        if not method.startswith("set_") or method == "set_chain":
            continue

        with pytest.raises(subprocess.CalledProcessError):
            print(f"Calling {method}")
            getattr(tls_manager, method)()

    with pytest.raises(subprocess.CalledProcessError):
        tls_manager.remove_cert("some-alias")

    with pytest.raises(subprocess.CalledProcessError):
        tls_manager.get_current_sans()
