#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Kafka TLS configuration."""

import logging
import socket
import subprocess
from dataclasses import dataclass
from functools import cached_property

from ops.pebble import ExecError

from core.models import Context, TLSContext, WorkerUnitContext
from core.workload import WorkloadBase
from literals import GROUP, SNAP_NAME, USER, Substrates

logger = logging.getLogger(__name__)


@dataclass
class Sans:
    """Data class for modeling TLS SANs."""

    sans_ip: list[str]
    sans_dns: list[str]


class TLSManager:
    """Manager for building necessary files for Java TLS auth."""

    def __init__(
        self,
        context: Context,
        workload: WorkloadBase,
        substrate: Substrates,
    ):
        self.context = context
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
            path=f"{self.workload.paths.config_dir}/server.key",
        )

    def set_ca(self) -> None:
        """Sets the unit CA."""
        if not self.tls_context.ca:
            logger.error("Can't set CA to unit, missing CA in relation data")
            return

        self.workload.write(
            content=self.tls_context.ca, path=f"{self.workload.paths.config_dir}/ca.pem"
        )

    def set_certificate(self) -> None:
        """Sets the unit certificate."""
        if not self.tls_context.certificate:
            logger.error("Can't set certificate to unit, missing certificate in relation data")
            return

        self.workload.write(
            content=self.tls_context.certificate,
            path=f"{self.workload.paths.config_dir}/server.pem",
        )

    def set_bundle(self) -> None:
        """Sets the unit cert bundle."""
        if not self.tls_context.certificate or not self.tls_context.ca:
            logger.error(
                "Can't set cert bundle to unit, missing certificate or CA in relation data"
            )
            return

        self.workload.write(
            content="\n".join(self.tls_context.bundle),
            path=f"{self.workload.paths.config_dir}/bundle.pem",
        )

    def set_chain(self) -> None:
        """Sets the unit chain."""
        if not self.tls_context.chain:
            logger.error("Can't set chain to unit, missing chain in relation data")
            return

        for i, chain_cert in enumerate(self.tls_context.chain):
            self.workload.write(
                content=chain_cert, path=f"{self.workload.paths.config_dir}/bundle{i}.pem"
            )

    def set_truststore(self) -> None:
        """Adds CA to JKS truststore."""
        trust_aliases = [f"bundle{i}" for i in range(len(self.tls_context.bundle))] + ["ca"]

        for alias in trust_aliases:
            self.import_cert(alias, f"{alias}.pem")

        self.workload.exec(f"chown {USER}:{GROUP} {self.workload.paths.truststore}".split())
        self.workload.exec(["chmod", "770", self.workload.paths.truststore])

    def set_keystore(self) -> None:
        """Creates and adds unit cert and private-key to the keystore."""
        in_file = "bundle.pem" if self.tls_context.bundle else "server.pem"
        command = f"openssl pkcs12 -export -in {in_file} -inkey server.key -passin pass:{self.tls_context.keystore_password} -certfile server.pem -out {self.workload.paths.keystore} -password pass:{self.tls_context.keystore_password}"
        try:
            self.workload.exec(command=command.split(), working_dir=self.workload.paths.config_dir)
            self.workload.exec(f"chown {USER}:{GROUP} {self.workload.paths.keystore}".split())
            self.workload.exec(["chmod", "770", self.workload.paths.keystore])
        except (subprocess.CalledProcessError, ExecError) as e:
            logger.error(e.stdout)
            raise e

    def import_cert(self, alias: str, filename: str, cert_content: str | None = None) -> None:
        """Add a certificate to the truststore."""
        if cert_content:
            self.workload.write(
                content=cert_content, path=f"{self.workload.paths.config_dir}/{filename}.pem"
            )

        command = f"{self.keytool} -import -v -alias {alias} -file {filename} -keystore {self.workload.paths.truststore} -storepass {self.tls_context.truststore_password} -noprompt"
        try:
            self.workload.exec(command=command.split(), working_dir=self.workload.paths.config_dir)
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
            command = f"{self.keytool} -delete -v -alias {alias} -keystore {self.workload.paths.truststore} -storepass {self.tls_context.truststore_password} -noprompt"
            self.workload.exec(command=command.split(), working_dir=self.workload.paths.config_dir)
            self.workload.exec(
                f"rm -f {alias}.pem".split(), working_dir=self.workload.paths.config_dir
            )
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
            return Sans(
                sans_ip=[self.unit_context.internal_address],
                sans_dns=[self.unit_context.unit.name, socket.getfqdn()],
            )
        else:
            return Sans(
                sans_ip=sorted(
                    [
                        str(self.context.bind_address),
                        # self.unit_context.node_ip,
                    ]
                ),
                sans_dns=sorted(
                    [
                        self.unit_context.internal_address.split(".")[0],
                        self.unit_context.internal_address,
                        socket.getfqdn(),
                    ]
                ),
            )

    def get_current_sans(self) -> Sans | None:
        """Gets the current SANs for the unit cert."""
        if not self.tls_context.certificate:
            return

        command = ["openssl", "x509", "-noout", "-ext", "subjectAltName", "-in", "server.pem"]

        try:
            sans_lines = self.workload.exec(
                command=command, working_dir=self.workload.paths.config_dir
            ).splitlines()
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

        return Sans(sans_ip=sorted(sans_ip), sans_dns=sorted(sans_dns))

    @property
    def sans_change_detected(self) -> bool:
        """Checks whether SANs has changed or not based on a comparison of TLS context with the last state available to the manager."""
        current_sans = self.get_current_sans()
        expected_sans = self.build_sans()

        current_sans_ip = set(current_sans.sans_ip) if current_sans else set()
        expected_sans_ip = set(expected_sans.sans_ip) if current_sans else set()
        sans_ip_changed = current_sans_ip ^ expected_sans_ip

        current_sans_dns = set(current_sans.sans_dns) if current_sans else set()
        expected_sans_dns = set(expected_sans.sans_dns) if current_sans else set()
        sans_dns_changed = current_sans_dns ^ expected_sans_dns

        if not sans_ip_changed and not sans_dns_changed:
            return False

        logger.info(
            (
                f"SANs change detected - "
                f"OLD SANs IP = {current_sans_ip - expected_sans_ip}, "
                f"NEW SANs IP = {expected_sans_ip - current_sans_ip}, "
                f"OLD SANs DNS = {current_sans_dns - expected_sans_dns}, "
                f"NEW SANs DNS = {expected_sans_dns - current_sans_dns}"
            )
        )
        return True

    def remove_stores(self) -> None:
        """Cleans up all keys/certs/stores on a unit."""
        for pattern in ["*.pem", "*.key", "*.p12", "*.jks"]:
            self.workload.remove(f"{self.workload.paths.config_dir}/{pattern}", glob=True)
