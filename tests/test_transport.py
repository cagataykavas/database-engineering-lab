from __future__ import annotations

import json
import os
import ssl
from pathlib import Path

import pytest

from dbbench.cli import main
from dbbench.transport import TransportConfigError, audit_postgres_transport


def _system_ca_bundle() -> Path:
    cafile = ssl.get_default_verify_paths().cafile
    if not cafile:
        pytest.skip("system CA bundle is unavailable")
    path = Path(cafile).resolve()
    if not path.is_file():
        pytest.skip("system CA bundle is unavailable")
    return path


def _secure_conninfo(cafile: Path) -> str:
    return " ".join(
        [
            "host=db.internal,db-replica.internal",
            "port=5432,5432",
            "dbname=ledger",
            "user=app",
            "sslmode=verify-full",
            f"sslrootcert={cafile}",
            "ssl_min_protocol_version=TLSv1.2",
            "channel_binding=require",
            "connect_timeout=5",
            "target_session_attrs=read-write",
            "application_name=db-bench",
        ]
    )


def test_secure_multi_host_transport_passes_without_exposing_endpoint() -> None:
    audit = audit_postgres_transport(_secure_conninfo(_system_ca_bundle()))
    payload = audit.as_dict()

    assert audit.passed
    assert audit.endpoint_count == 2
    assert len(audit.policy_sha256) == 64
    assert len(audit.root_ca_sha256 or "") == 64
    assert "db.internal" not in json.dumps(payload)
    assert "app" not in json.dumps(payload)


def test_plaintext_inline_password_and_unbounded_connection_fail() -> None:
    audit = audit_postgres_transport(
        "host=db.internal dbname=ledger user=app password=hunter2 sslmode=prefer"
    )

    assert not audit.passed
    assert {finding.code for finding in audit.findings} >= {
        "sslmode",
        "channel_binding",
        "connect_timeout",
        "inline_secret",
        "root_ca_missing",
    }
    assert "hunter2" not in json.dumps(audit.as_dict())


def test_hostaddr_without_identity_and_mismatched_ports_fail() -> None:
    cafile = _system_ca_bundle()
    audit = audit_postgres_transport(
        _secure_conninfo(cafile).replace(
            "host=db.internal,db-replica.internal port=5432,5432",
            "hostaddr=192.0.2.10,192.0.2.11 port=5432,5433,5434",
        )
    )

    codes = {finding.code for finding in audit.findings}
    assert "host_missing" in codes
    assert "host_identity_missing" in codes
    assert "endpoint_arity" in codes


def test_private_credential_file_must_be_absolute_and_mode_0600(tmp_path: Path) -> None:
    cafile = _system_ca_bundle()
    passfile = tmp_path / "pgpass"
    passfile.write_text("db.internal:5432:ledger:app:not-a-real-secret\n", encoding="utf-8")
    os.chmod(passfile, 0o644)
    audit = audit_postgres_transport(f"{_secure_conninfo(cafile)} passfile={passfile}")

    assert not audit.passed
    assert "passfile_permissions" in {finding.code for finding in audit.findings}


def test_client_identity_requires_real_certificate_and_private_key(tmp_path: Path) -> None:
    cafile = _system_ca_bundle()
    key = tmp_path / "client.key"
    key.write_text("not-real-key-material\n", encoding="utf-8")
    os.chmod(key, 0o600)
    audit = audit_postgres_transport(
        f"{_secure_conninfo(cafile)} sslcert=/missing/client.pem sslkey={key}"
    )

    assert not audit.passed
    assert "sslcert_unreadable" in {finding.code for finding in audit.findings}


def test_cli_exit_codes_distinguish_policy_failure_from_invalid_syntax(capsys) -> None:
    assert main(["--dsn", "host=db.internal sslmode=disable", "audit-transport"]) == 1
    policy_payload = json.loads(capsys.readouterr().out)
    assert policy_payload["passed"] is False

    assert main(["--dsn", "host='unterminated", "audit-transport"]) == 2
    config_payload = json.loads(capsys.readouterr().out)
    assert config_payload["passed"] is False
    assert "valid libpq syntax" in config_payload["error"]


def test_conninfo_size_is_bounded_before_parsing() -> None:
    with pytest.raises(TransportConfigError, match="exceeds"):
        audit_postgres_transport("host=" + "a" * 5000)
