import asyncio
import logging

import pytest
from helpers import (
    APP_NAME,
    KAFKA_APP,
    KAFKA_CHANNEL,
    extract_sans,
    get_certificate,
    make_connect_api_request,
    run_command_on_unit,
    self_signed_ca,
)
from pytest_operator.plugin import OpsTest
from requests.exceptions import ConnectionError, SSLError

from literals import CONFIG_DIR

logger = logging.getLogger(__name__)

TLS_APP = "self-signed-certificates"
TLS_CHANNEL = "1/stable"
TLS_CONFIG = {"ca-common-name": "kafka"}


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
async def test_deploy_tls(ops_test: OpsTest, kafka_connect_charm):

    await asyncio.gather(
        ops_test.model.deploy(
            TLS_APP,
            channel=TLS_CHANNEL,
            config=TLS_CONFIG,
        ),
        ops_test.model.deploy(
            kafka_connect_charm,
            application_name=APP_NAME,
            series="noble",
            config={"profile": "testing"},
        ),
        ops_test.model.deploy(
            KAFKA_APP,
            channel=KAFKA_CHANNEL,
            application_name=KAFKA_APP,
            num_units=1,
            series="jammy",
            config={"roles": "broker,controller"},
        ),
    )

    await ops_test.model.add_relation(APP_NAME, KAFKA_APP)

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, KAFKA_APP, TLS_APP], idle_period=60, timeout=1200, status="active"
        )


@pytest.mark.abort_on_fail
async def test_enable_tls_on_rest_api(ops_test: OpsTest):
    """Checks enabling TLS makes REST interface connections secure."""
    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.add_relation(TLS_APP, APP_NAME)
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME], idle_period=30, timeout=600, status="active"
        )

    with pytest.raises(ConnectionError):
        _ = await make_connect_api_request(ops_test, proto="http")

    async with self_signed_ca(ops_test, TLS_APP) as ca_file:
        response = await make_connect_api_request(ops_test, proto="https", verify=ca_file.name)
        assert response.status_code == 200

    cert = await get_certificate(ops_test)

    assert TLS_CONFIG["ca-common-name"] in cert.issuer.rfc4514_string()
    assert f"{APP_NAME}/0" in extract_sans(cert)


async def test_tls_scale_out(ops_test: OpsTest):
    """Checks connect workers scaling functionality with TLS relation."""
    await ops_test.model.applications[APP_NAME].add_units(count=2)
    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME], idle_period=30, timeout=600, status="active", wait_for_exact_units=3
        )

    async with self_signed_ca(ops_test, TLS_APP) as ca_file:
        for unit in ops_test.model.applications[APP_NAME].units:
            response = await make_connect_api_request(
                ops_test, unit=unit, proto="https", verify=ca_file.name
            )
            assert response.status_code == 200


async def test_tls_broken(ops_test: OpsTest):
    """Checks broken TLS relation leads to fallback to HTTP on the REST interface."""
    await ops_test.juju("remove-relation", APP_NAME, TLS_APP)

    async with ops_test.fast_forward(fast_interval="60s"):
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, KAFKA_APP, TLS_APP], idle_period=60, timeout=1200
        )

    for unit in ops_test.model.applications[APP_NAME].units:
        with pytest.raises(SSLError):
            _ = await make_connect_api_request(ops_test, unit=unit, proto="https")

        response = await make_connect_api_request(ops_test, unit=unit, proto="http")

        assert response.status_code == 200

        res = await run_command_on_unit(ops_test, unit, f"sudo ls {CONFIG_DIR}")
        file_extensions = {f.split(".")[-1] for f in res.stdout.split() if f}

        assert not {"pem", "key", "p12", "jks"} & file_extensions
