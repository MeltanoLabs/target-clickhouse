"""Tests for get_sqlalchemy_url's credential encoding.

Before this fix, the SQLAlchemy URL was built via raw f-string interpolation
of username/password with no encoding, so a credential containing a
URL-delimiter character ("@", ":", "/") produced an unparseable URL. These
tests build a connector directly and inspect the returned URL string -- no
live ClickHouse needed.
"""

from __future__ import annotations

from unittest.mock import patch

from sqlalchemy.engine import make_url

from target_clickhouse.connectors import ClickhouseConnector

TUNNEL_LOCAL_PORT = 54321

CONFIG = {
    "host": "localhost",
    "port": 18123,
    "driver": "http",
    "username": "default",
    "password": "",
    "database": "default",
    "secure": False,
    "verify": False,
}


def test_special_characters_in_password_round_trip() -> None:
    """A password containing "@", ":", "/" must not break URL parsing."""
    special_password = "p@ss:w/rd?"  # noqa: S105
    config = {**CONFIG, "username": "test@user", "password": special_password}
    connector = ClickhouseConnector(config=config)

    url = connector.get_sqlalchemy_url(config)
    parsed = make_url(url)

    assert parsed.username == "test@user"
    assert parsed.password == special_password
    assert parsed.host == CONFIG["host"]
    assert parsed.port == CONFIG["port"]
    assert parsed.database == CONFIG["database"]


def test_empty_password_still_produces_valid_url() -> None:
    """The common case (empty password, plain username) is unaffected."""
    connector = ClickhouseConnector(config=CONFIG)

    url = connector.get_sqlalchemy_url(CONFIG)
    parsed = make_url(url)

    assert parsed.username == CONFIG["username"]
    assert parsed.password in (None, "")
    assert parsed.host == CONFIG["host"]
    assert parsed.port == CONFIG["port"]


def test_http_secure_with_verify_true_omits_verify_param() -> None:
    """verify=True must never be sent as the literal string "True".

    clickhouse_sqlalchemy's HTTP driver only special-cases the string "False"
    (see drivers/http/base.py) -- any other value, including "True", is
    forwarded verbatim to `requests`, which only treats `verify` as "use the
    default CA bundle" when it *is* the real `True` singleton. A stringified
    "True" gets misread as a literal CA-bundle file path and crashes with
    "Could not find a suitable TLS CA certificate bundle, invalid path: True"
    -- reproduced against a real TLS ClickHouse endpoint. So verify=True must
    be conveyed by omitting the param entirely, never by sending "True".
    """
    config = {**CONFIG, "secure": True, "verify": True}
    connector = ClickhouseConnector(config=config)

    parsed = make_url(connector.get_sqlalchemy_url(config))

    assert parsed.query["protocol"] == "https"
    assert "verify" not in parsed.query


def test_http_secure_with_verify_false_sets_verify_param() -> None:
    config = {**CONFIG, "secure": True, "verify": False}
    connector = ClickhouseConnector(config=config)

    parsed = make_url(connector.get_sqlalchemy_url(config))

    assert parsed.query["protocol"] == "https"
    assert parsed.query["verify"] == "False"


def test_http_insecure_sets_protocol_only() -> None:
    """The plain-http branch never had a `verify` param -- preserved as-is."""
    connector = ClickhouseConnector(config=CONFIG)

    parsed = make_url(connector.get_sqlalchemy_url(CONFIG))

    assert parsed.query["protocol"] == "http"
    assert "verify" not in parsed.query


def test_native_driver_verify_true_omits_verify_param() -> None:
    """Same "True" vs omitted-param fix, applied symmetrically to the native branch."""
    config = {
        **CONFIG,
        "driver": "native",
        "port": 19000,
        "secure": True,
        "verify": True,
    }
    connector = ClickhouseConnector(config=config)

    parsed = make_url(connector.get_sqlalchemy_url(config))

    assert parsed.query["secure"] == "True"
    assert "verify" not in parsed.query


def test_native_driver_verify_false_sets_verify_param() -> None:
    config = {
        **CONFIG,
        "driver": "native",
        "port": 19000,
        "secure": True,
        "verify": False,
    }
    connector = ClickhouseConnector(config=config)

    parsed = make_url(connector.get_sqlalchemy_url(config))

    assert parsed.query["secure"] == "True"
    assert parsed.query["verify"] == "False"


def test_ssh_tunnel_host_port_substitution_flows_into_url() -> None:
    """The tunnel's local endpoint, not the config host/port, must appear in the URL."""
    connector = ClickhouseConnector(config=CONFIG)
    tunnel_endpoint = ("127.0.0.1", TUNNEL_LOCAL_PORT)

    with patch.object(connector, "_tunneled_host_port", return_value=tunnel_endpoint):
        parsed = make_url(connector.get_sqlalchemy_url(CONFIG))

    assert parsed.host == "127.0.0.1"
    assert parsed.port == TUNNEL_LOCAL_PORT


def test_explicit_sqlalchemy_url_bypasses_url_building() -> None:
    """An explicit sqlalchemy_url is used verbatim (unchanged behavior)."""
    explicit_url = "clickhouse+http://default:@localhost:18123"
    config = {"sqlalchemy_url": explicit_url}
    connector = ClickhouseConnector(config=config)

    assert connector.get_sqlalchemy_url(config) == explicit_url
