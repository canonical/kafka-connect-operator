#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Kafka TLS configuration."""

import glob
import logging
import socket
import subprocess
from functools import cached_property
from typing import TypedDict  # nosec B404

from ops.pebble import ExecError

from core.models import Context, TLSContext, WorkerUnitContext
from core.workload import WorkloadBase
from literals import CONFIG_DIR, GROUP, KEYSTORE_PATH, SNAP_NAME, TRUSTSTORE_PATH, USER, Substrates

logger = logging.getLogger(__name__)

Sans = TypedDict("Sans", {"sans_ip": list[str], "sans_dns": list[str]})


class TLSManager:
    """Manager for building necessary files for Java TLS auth."""

    def __init__(
        self,
        context: Context,
        workload: WorkloadBase,
        substrate: Substrates,
    ):
        self.unit_context: WorkerUnitContext = context.worker_unit
        self.tls_context: TLSContext = self.unit_context.tls
        self.workload = workload
        self.substrate = substrate

    @cached_property
    def keytool(self):
        """Returns the `keytool` utility depending on substrate."""
        return f"{SNAP_NAME}.keytool" if self.substrate == "vm" else "keytool"

    def generate_alias(self, app_name: str, relation_id: int) -> str:
        """Generate an alias from a relation. Used to identify ca certs."""
        return f"{app_name}-{relation_id}"

    def set_server_key(self) -> None:
        """Sets the private-key."""
        if not self.tls_context.private_key:
            logger.error("Can't set private-key to unit, missing private-key in relation data")
            return

        self.workload.write(
            content=self.tls_context.private_key,
            path=f"{CONFIG_DIR}/server.key",
        )

    def set_ca(self) -> None:
        """Sets the unit CA."""
        if not self.tls_context.ca:
            logger.error("Can't set CA to unit, missing CA in relation data")
            return

        self.workload.write(content=self.tls_context.ca, path=f"{CONFIG_DIR}/ca.pem")

    def set_certificate(self) -> None:
        """Sets the unit certificate."""
        if not self.tls_context.certificate:
            logger.error("Can't set certificate to unit, missing certificate in relation data")
            return

        self.workload.write(
            content=self.tls_context.certificate,
            path=f"{CONFIG_DIR}/server.pem",
        )

    def set_chain(self) -> None:
        """Sets the unit chain."""
        if not self.tls_context.chain:
            logger.error("Can't set chain to unit, missing chain in relation data")
            return

        for i, chain_cert in enumerate(self.tls_context.chain):
            self.workload.write(content=chain_cert, path=f"{CONFIG_DIR}/chain{i}.pem")

    def set_truststore(self) -> None:
        """Adds CA to JKS truststore."""
        trust_aliases = [f"chain{i}" for i in range(len(self.tls_context.chain))] + ["ca"]

        for alias in trust_aliases:
            self.import_cert(alias, f"{alias}.pem")

        self.workload.exec(f"chown {USER}:{GROUP} {TRUSTSTORE_PATH}".split())
        self.workload.exec(["chmod", "770", TRUSTSTORE_PATH])

    def set_keystore(self) -> None:
        """Creates and adds unit cert and private-key to the keystore."""
        command = f"openssl pkcs12 -export -in server.pem -inkey server.key -passin pass:{self.tls_context.keystore_password} -certfile server.pem -out {KEYSTORE_PATH} -password pass:{self.tls_context.keystore_password}"
        try:
            self.workload.exec(command=command.split(), working_dir=CONFIG_DIR)
            self.workload.exec(f"chown {USER}:{GROUP} {KEYSTORE_PATH}".split())
            self.workload.exec(["chmod", "770", KEYSTORE_PATH])
        except (subprocess.CalledProcessError, ExecError) as e:
            logger.error(e.stdout)
            raise e

    def import_cert(self, alias: str, filename: str, cert_content: str | None = None) -> None:
        """Add a certificate to the truststore."""
        if cert_content:
            self.workload.write(content=cert_content, path=f"{CONFIG_DIR}/{filename}.pem")

        command = f"{self.keytool} -import -v -alias {alias} -file {filename} -keystore {TRUSTSTORE_PATH} -storepass {self.tls_context.truststore_password} -noprompt"
        try:
            self.workload.exec(command=command.split(), working_dir=CONFIG_DIR)
        except (subprocess.CalledProcessError, ExecError) as e:
            # in case this reruns and fails
            if e.stdout and "already exists" in e.stdout:
                logger.debug(e.stdout)
                return
            logger.error(e.stdout)
            raise e

    def remove_cert(self, alias: str) -> None:
        """Remove a cert from the truststore."""
        try:
            command = f"{self.keytool} -delete -v -alias {alias} -keystore {KEYSTORE_PATH} -storepass {self.tls_context.truststore_password} -noprompt"
            self.workload.exec(command=command.split(), working_dir=CONFIG_DIR)
            self.workload.exec(f"rm -f {alias}.pem".split(), working_dir=CONFIG_DIR)
        except (subprocess.CalledProcessError, ExecError) as e:
            if e.stdout and "does not exist" in e.stdout:
                logger.warning(e.stdout)
                return
            logger.error(e.stdout)
            raise e

    def build_sans(
        self,
    ) -> Sans:
        """Builds a SAN dict of DNS names and IPs for the unit."""
        if self.substrate == "vm":
            return {
                "sans_ip": [
                    self.unit_context.internal_address,
                ],
                "sans_dns": [self.unit_context.unit.name, socket.getfqdn()],
            }
        else:
            raise NotImplementedError

    def get_current_sans(self) -> Sans | None:
        """Gets the current SANs for the unit cert."""
        if not self.tls_context.certificate:
            return

        command = ["openssl", "x509", "-noout", "-ext", "subjectAltName", "-in", "server.pem"]

        try:
            sans_lines = self.workload.exec(command=command, working_dir=CONFIG_DIR).splitlines()
        except (subprocess.CalledProcessError, ExecError) as e:
            logger.error(e.stdout)
            raise e

        for line in sans_lines:
            if "DNS" in line and "IP" in line:
                break

        sans_ip = []
        sans_dns = []
        for item in line.split(", "):
            san_type, san_value = item.split(":")

            if san_type.strip() == "DNS":
                sans_dns.append(san_value)
            if san_type.strip() == "IP Address":
                sans_ip.append(san_value)

        return {"sans_ip": sorted(sans_ip), "sans_dns": sorted(sans_dns)}

    def remove_stores(self) -> None:
        """Cleans up all keys/certs/stores on a unit."""
        for pattern in ["*.pem", "*.key", "*.p12", "*.jks"]:
            for file in glob.glob(f"{CONFIG_DIR}/{pattern}"):
                self.workload.remove(file)
