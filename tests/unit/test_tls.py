import dataclasses
import json
import logging
from typing import cast
from unittest.mock import MagicMock

import pytest
from charms.tls_certificates_interface.v3.tls_certificates import (
    CertificateAvailableEvent,
    CertificateExpiringEvent,
    generate_ca,
    generate_certificate,
    generate_csr,
    generate_private_key,
)
from ops.testing import Context, PeerRelation, Relation, State
from src.charm import ConnectCharm
from src.literals import PEER_REL, TLS_REL, Status, TLSLiterals
from src.managers.tls import TLSManager

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("is_leader", [True, False])
def test_tls_relation_created(ctx: Context, base_state: State, is_leader: bool) -> None:
    """Checks TLS `relation-created` enables `tls` on peer app data interface."""
    # Given
    state_in = base_state
    peer_rel = PeerRelation(PEER_REL, PEER_REL)
    tls_rel = Relation(TLS_REL, TLS_REL)

    state_in = dataclasses.replace(base_state, relations=[tls_rel, peer_rel], leader=is_leader)

    # When
    state_out = ctx.run(ctx.on.relation_created(tls_rel), state_in)

    # Then
    assert state_out.unit_status == Status.MISSING_KAFKA.value.status
    if is_leader:
        assert peer_rel.local_app_data.get("tls") == "enabled"
    else:
        assert not peer_rel.local_app_data.get("tls")


@pytest.mark.parametrize("is_leader", [True, False])
def test_tls_relation_broken(ctx: Context, base_state: State, is_leader: bool) -> None:
    """Checks TLS `relation-broken` disables `tls` on peer app data interface and triggers proper cleanup."""
    # Given
    state_in = base_state
    peer_rel = PeerRelation(
        PEER_REL,
        PEER_REL,
        local_app_data={"tls": "enabled"},
        local_unit_data={
            TLSLiterals.CA: "ca",
            TLSLiterals.PRIVATE_KEY: "private-key",
            TLSLiterals.CERT: "cert",
        },
    )
    tls_rel = Relation(TLS_REL, TLS_REL)

    state_in = dataclasses.replace(base_state, relations=[tls_rel, peer_rel], leader=is_leader)

    # When
    state_out = ctx.run(ctx.on.relation_broken(tls_rel), state_in)

    # Then
    assert state_out.unit_status == Status.MISSING_KAFKA.value.status
    for key in TLSLiterals.KEYS:
        assert not peer_rel.local_unit_data.get(key)

    if is_leader:
        assert not peer_rel.local_app_data.get("tls")
    else:
        assert peer_rel.local_app_data.get("tls") == "enabled"


@pytest.mark.parametrize("is_leader", [True, False])
@pytest.mark.parametrize(
    "tls_init_data",
    [
        {},
        {
            TLSLiterals.PRIVATE_KEY: generate_private_key().decode("utf-8"),
            TLSLiterals.KEYSTORE_PASSWORD: "password",
            TLSLiterals.TRUSTSTORE_PASSWORD: "password",
        },
    ],
)
def test_tls_relation_joined(
    ctx: Context, base_state: State, is_leader: bool, tls_init_data: dict
) -> None:
    """Checks TLS `relation-joined` triggers appropriate setup of private key, keystore and truststore on the unit."""
    # Given
    state_in = base_state
    peer_rel = PeerRelation(
        PEER_REL,
        PEER_REL,
        local_app_data={"tls": "enabled"},
    )
    tls_rel = Relation(TLS_REL, TLS_REL)

    state_in = dataclasses.replace(base_state, relations=[tls_rel, peer_rel], leader=is_leader)

    # When
    # state_out = ctx.run(ctx.on.relation_joined(tls_rel), state_in)

    with ctx(ctx.on.relation_joined(tls_rel), state_in) as mgr:
        charm: ConnectCharm = cast(ConnectCharm, mgr.charm)
        if tls_init_data:
            charm.context.worker_unit.update(tls_init_data)
        state_out = mgr.run()

    secret_contents = {
        k: v for secret in state_out.secrets for k, v in secret.latest_content.items()
    }

    # Then
    assert len(secret_contents) > 0
    assert state_out.unit_status == Status.MISSING_KAFKA.value.status
    assert secret_contents.get(TLSLiterals.PRIVATE_KEY, "")
    assert secret_contents.get(TLSLiterals.CSR, "")
    assert secret_contents.get(TLSLiterals.KEYSTORE_PASSWORD, "")
    assert secret_contents.get(TLSLiterals.TRUSTSTORE_PASSWORD, "")

    if tls_init_data:
        assert secret_contents.get(TLSLiterals.KEYSTORE_PASSWORD, "") == "password"
        assert secret_contents.get(TLSLiterals.TRUSTSTORE_PASSWORD, "") == "password"


@pytest.mark.parametrize("chain", [[], ["cert-1", "cert-2", "cert-3"]])
def test_tls_certificate_available(ctx: Context, base_state: State, chain) -> None:
    """Checks on `certificate-available` event, local unit data is updated accordingly."""
    # Given
    ca_key = generate_private_key()
    ca = generate_ca(private_key=ca_key, subject="TEST-CA")
    private_key = generate_private_key()
    csr = generate_csr(private_key=private_key, subject="kafka-connect/0")

    state_in = base_state
    peer_rel = PeerRelation(
        PEER_REL,
        PEER_REL,
        local_app_data={"tls": "enabled"},
        local_unit_data={TLSLiterals.CSR: csr.decode("utf-8")},
    )
    tls_rel = Relation(TLS_REL, TLS_REL)
    tls_manager_mock = MagicMock(spec=TLSManager)
    event = CertificateAvailableEvent(
        handle=MagicMock(),
        certificate=generate_certificate(csr, ca, ca_key).decode("utf-8"),
        certificate_signing_request=csr.decode("utf-8"),
        ca=ca.decode("utf-8"),
        chain=chain,
    )

    state_in = dataclasses.replace(base_state, relations=[tls_rel, peer_rel])

    # When
    with (ctx(ctx.on.update_status(), state_in) as mgr,):
        charm: ConnectCharm = cast(ConnectCharm, mgr.charm)
        charm.tls.tls_manager = tls_manager_mock
        charm.tls._on_certificate_available(event)
        state_out = mgr.run()

    # Then
    assert state_out.unit_status == Status.MISSING_KAFKA.value.status
    assert tls_manager_mock.set_truststore.call_count == 1
    assert tls_manager_mock.set_keystore.call_count == 1
    assert tls_manager_mock.set_ca.call_count == 1
    assert tls_manager_mock.set_certificate.call_count == 1
    assert tls_manager_mock.set_chain.call_count == 1
    assert tls_manager_mock.set_server_key.call_count == 1
    assert peer_rel.local_unit_data.get(TLSLiterals.CERT, "")
    assert peer_rel.local_unit_data.get(TLSLiterals.CA, "")
    assert peer_rel.local_unit_data.get(TLSLiterals.CHAIN, "")
    assert len(json.loads(peer_rel.local_unit_data.get(TLSLiterals.CHAIN, ""))) == len(chain)


def test_tls_certificate_expiring(ctx: Context, base_state: State, active_service) -> None:
    """Checks `certificate-expiring` event leads to new CSR being submitted."""
    # Given
    ca_key = generate_private_key()
    ca = generate_ca(private_key=ca_key, subject="TEST-CA")
    private_key = generate_private_key()
    other_private_key = generate_private_key()
    csr = generate_csr(private_key=other_private_key, subject="kafka-connect/0")
    cert = generate_certificate(csr, ca, ca_key)

    state_in = base_state
    peer_rel = PeerRelation(
        PEER_REL,
        PEER_REL,
        local_app_data={"tls": "enabled"},
    )
    tls_rel = Relation(TLS_REL, TLS_REL)
    event = CertificateExpiringEvent(
        handle=MagicMock(), certificate=cert.decode("utf-8"), expiry="1990-01-01 00:00:00"
    )

    state_in = dataclasses.replace(base_state, relations=[tls_rel, peer_rel])

    # When
    with (ctx(ctx.on.update_status(), state_in) as mgr,):
        charm: ConnectCharm = cast(ConnectCharm, mgr.charm)
        charm.context.worker_unit.update(
            items={
                TLSLiterals.CSR: csr.decode("utf-8"),
                TLSLiterals.PRIVATE_KEY: private_key.decode("utf-8"),
            }
        )
        charm.tls._on_certificate_expiring(event)
        state_out = mgr.run()

    secret_contents = {
        k: v for secret in state_out.secrets for k, v in secret.latest_content.items()
    }

    # Then
    # TODO: better assertions?
    assert state_out.unit_status == Status.MISSING_KAFKA.value.status
    assert secret_contents.get(TLSLiterals.PRIVATE_KEY) == private_key.decode("utf-8")
    assert secret_contents.get(TLSLiterals.CSR) != csr.decode("utf-8")
