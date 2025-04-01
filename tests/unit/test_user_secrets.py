import dataclasses
import logging
from typing import cast
from unittest.mock import patch

import pytest
from ops.testing import Context, PeerRelation, Relation, Secret, State
from src.charm import ConnectCharm
from src.literals import KAFKA_CLIENT_REL, PEER_REL

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("secret_id", ["", "auth_secret", "wrong_secret_name"])
def test_set_credentials(
    ctx: Context, base_state: State, kafka_client_rel: dict, active_service, caplog, secret_id: str
) -> None:
    """Tests setting username/passwords through secrets."""
    auth_secret = Secret(
        id="auth_secret",
        tracked_content={"admin": "newpass"},
    )
    kafka_rel = Relation(KAFKA_CLIENT_REL, KAFKA_CLIENT_REL, remote_app_data=kafka_client_rel)
    peer_rel = PeerRelation(PEER_REL, PEER_REL, local_app_data={"admin-password": "oldpass"})
    state_in = dataclasses.replace(
        base_state,
        relations=[kafka_rel, peer_rel],
        secrets=[auth_secret],
        config={"auth": secret_id},
    )

    with (
        ctx(ctx.on.secret_changed(auth_secret), state_in) as mgr,
        patch("managers.connect.ConnectManager.restart_worker") as _restart,
    ):
        charm: ConnectCharm = cast(ConnectCharm, mgr.charm)
        previous_password = charm.context.peer_workers.admin_password
        _ = mgr.run()

    assert previous_password == "oldpass"

    match secret_id:
        case "auth_secret":
            assert charm.context.peer_workers.admin_password != previous_password
            assert charm.context.peer_workers.admin_password == "newpass"
            _restart.assert_called_once()
        case "":
            assert charm.context.peer_workers.admin_password == previous_password
        case _:
            # secret not found, we expect an ERROR log
            assert charm.context.peer_workers.admin_password == previous_password
            assert caplog.records[-1].levelname == "ERROR"


def test_remove_credentials(
    ctx: Context, base_state: State, kafka_client_rel: dict, active_service, workload_with_io
) -> None:
    """Tests removing users through secrets."""
    auth_secret = Secret(
        id="auth_secret",
        tracked_content={"admin": "newpass"},
    )
    kafka_rel = Relation(KAFKA_CLIENT_REL, KAFKA_CLIENT_REL, remote_app_data=kafka_client_rel)
    peer_rel = PeerRelation(PEER_REL, PEER_REL, local_app_data={"admin-password": "oldpass"})
    state_in = dataclasses.replace(
        base_state,
        relations=[kafka_rel, peer_rel],
        secrets=[auth_secret],
        # config={"auth": secret_id}
    )

    with (ctx(ctx.on.update_status(), state_in) as mgr,):
        charm: ConnectCharm = cast(ConnectCharm, mgr.charm)
        # Let's say we had an admin and 2 custom users configured.
        charm.workload.write(
            "admin: oldpass\nuser1: user1pass\nuser2: user2pass", charm.workload.paths.passwords
        )
        assert len(charm.auth_manager.credentials) == 3

        # fire update-status, which will fire config-changed
        _ = mgr.run()

    # since no secret is defined, we expect only admin user to remain
    assert len(charm.auth_manager.credentials) == 1
    assert charm.context.peer_workers.ADMIN_USERNAME in charm.auth_manager.credentials
