from __future__ import annotations

import hashlib
import json
import ssl
import stat
from dataclasses import asdict, dataclass
from pathlib import Path

from psycopg import ProgrammingError
from psycopg.conninfo import conninfo_to_dict

MAX_CONNINFO_BYTES = 4096
MAX_CA_BUNDLE_BYTES = 1_000_000


class TransportConfigError(ValueError):
    """Raised when connection configuration cannot be safely interpreted."""


@dataclass(frozen=True)
class TransportPolicy:
    max_connect_timeout_seconds: int = 10
    required_sslmode: str = "verify-full"
    required_channel_binding: str = "require"
    required_target_session_attrs: str = "read-write"
    allowed_minimum_tls_versions: tuple[str, ...] = ("TLSv1.2", "TLSv1.3")

    @property
    def sha256(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()


@dataclass(frozen=True)
class TransportFinding:
    code: str
    message: str


@dataclass(frozen=True)
class TransportAudit:
    passed: bool
    policy_sha256: str
    endpoint_count: int
    sslmode: str | None
    channel_binding: str | None
    connect_timeout_seconds: int | None
    target_session_attrs: str | None
    minimum_tls_version: str | None
    root_ca_sha256: str | None
    findings: tuple[TransportFinding, ...]

    def as_dict(self) -> dict:
        return {
            "passed": self.passed,
            "policy_sha256": self.policy_sha256,
            "endpoint_count": self.endpoint_count,
            "sslmode": self.sslmode,
            "channel_binding": self.channel_binding,
            "connect_timeout_seconds": self.connect_timeout_seconds,
            "target_session_attrs": self.target_session_attrs,
            "minimum_tls_version": self.minimum_tls_version,
            "root_ca_sha256": self.root_ca_sha256,
            "findings": [asdict(finding) for finding in self.findings],
        }


def _regular_file_mode(
    path_value: str,
    label: str,
) -> tuple[TransportFinding | None, int | None]:
    path = Path(path_value)
    if not path.is_absolute():
        return TransportFinding(f"{label}_relative", f"{label} must use an absolute path"), None
    if path.is_symlink():
        return TransportFinding(f"{label}_symlink", f"{label} must not be a symlink"), None
    try:
        metadata = path.stat()
    except OSError:
        return (
            TransportFinding(
                f"{label}_unreadable",
                f"{label} must be a readable regular file",
            ),
            None,
        )
    if not stat.S_ISREG(metadata.st_mode):
        return TransportFinding(f"{label}_not_regular", f"{label} must be a regular file"), None
    return None, stat.S_IMODE(metadata.st_mode)


def _regular_file_finding(path_value: str, label: str) -> TransportFinding | None:
    finding, _ = _regular_file_mode(path_value, label)
    return finding


def _private_file_finding(path_value: str, label: str) -> TransportFinding | None:
    finding, mode = _regular_file_mode(path_value, label)
    if finding:
        return finding
    if mode is not None and mode & 0o077:
        return TransportFinding(
            f"{label}_permissions",
            f"{label} must not grant group or other permissions",
        )
    return None


def _audit_root_ca(path_value: str | None) -> tuple[str | None, TransportFinding | None]:
    if not path_value:
        return None, TransportFinding(
            "root_ca_missing",
            "sslrootcert must name an explicit CA bundle",
        )
    path = Path(path_value)
    if not path.is_absolute():
        return None, TransportFinding("root_ca_relative", "sslrootcert must use an absolute path")
    if path.is_symlink():
        return None, TransportFinding("root_ca_symlink", "sslrootcert must not be a symlink")
    try:
        metadata = path.stat()
        if not stat.S_ISREG(metadata.st_mode):
            raise OSError
        if metadata.st_size <= 0 or metadata.st_size > MAX_CA_BUNDLE_BYTES:
            return None, TransportFinding(
                "root_ca_size",
                f"sslrootcert must be between 1 and {MAX_CA_BUNDLE_BYTES} bytes",
            )
        content = path.read_bytes()
        ssl.create_default_context(cafile=str(path))
    except (OSError, ssl.SSLError):
        return None, TransportFinding(
            "root_ca_invalid",
            "sslrootcert must be a readable regular PEM CA bundle",
        )
    if stat.S_IMODE(metadata.st_mode) & 0o022:
        return None, TransportFinding(
            "root_ca_writable",
            "sslrootcert must not be group or world writable",
        )
    return hashlib.sha256(content).hexdigest(), None


def _parse_timeout(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    return parsed


def audit_postgres_transport(
    conninfo: str,
    policy: TransportPolicy | None = None,
) -> TransportAudit:
    policy = policy or TransportPolicy()
    if not isinstance(conninfo, str) or not conninfo.strip():
        raise TransportConfigError("connection configuration must be a non-empty string")
    if len(conninfo.encode()) > MAX_CONNINFO_BYTES:
        raise TransportConfigError(f"connection configuration exceeds {MAX_CONNINFO_BYTES} bytes")
    if "\x00" in conninfo:
        raise TransportConfigError("connection configuration contains a NUL byte")
    try:
        parameters = conninfo_to_dict(conninfo)
    except (ProgrammingError, UnicodeError) as exc:
        raise TransportConfigError("connection configuration is not valid libpq syntax") from exc

    findings: list[TransportFinding] = []
    hosts = [host.strip() for host in parameters.get("host", "").split(",") if host.strip()]
    ports = [port.strip() for port in parameters.get("port", "").split(",") if port.strip()]
    if not hosts:
        findings.append(TransportFinding("host_missing", "an explicit TCP host is required"))
    elif any(host.startswith("/") for host in hosts):
        findings.append(
            TransportFinding("unix_socket", "Unix-domain socket hosts are not TCP endpoints")
        )
    if parameters.get("hostaddr") and not parameters.get("host"):
        findings.append(
            TransportFinding(
                "host_identity_missing",
                "hostaddr requires host so verify-full can check the intended server identity",
            )
        )
    if len(ports) > 1 and len(ports) != len(hosts):
        findings.append(
            TransportFinding(
                "endpoint_arity",
                "a multi-port connection must provide one port per host",
            )
        )

    sslmode = parameters.get("sslmode")
    if sslmode != policy.required_sslmode:
        findings.append(
            TransportFinding(
                "sslmode",
                f"sslmode must be {policy.required_sslmode}",
            )
        )
    channel_binding = parameters.get("channel_binding")
    if channel_binding != policy.required_channel_binding:
        findings.append(
            TransportFinding(
                "channel_binding",
                f"channel_binding must be {policy.required_channel_binding}",
            )
        )
    target_session_attrs = parameters.get("target_session_attrs")
    if target_session_attrs != policy.required_target_session_attrs:
        findings.append(
            TransportFinding(
                "target_session_attrs",
                f"target_session_attrs must be {policy.required_target_session_attrs}",
            )
        )
    minimum_tls_version = parameters.get("ssl_min_protocol_version")
    if minimum_tls_version not in policy.allowed_minimum_tls_versions:
        allowed = ", ".join(policy.allowed_minimum_tls_versions)
        findings.append(
            TransportFinding(
                "minimum_tls_version",
                f"ssl_min_protocol_version must be one of: {allowed}",
            )
        )

    connect_timeout = _parse_timeout(parameters.get("connect_timeout"))
    if connect_timeout is None or not 1 <= connect_timeout <= policy.max_connect_timeout_seconds:
        findings.append(
            TransportFinding(
                "connect_timeout",
                f"connect_timeout must be between 1 and {policy.max_connect_timeout_seconds} seconds",
            )
        )
    if not parameters.get("application_name", "").strip():
        findings.append(
            TransportFinding(
                "application_name",
                "application_name is required for connection attribution",
            )
        )

    if parameters.get("password") or parameters.get("sslpassword"):
        findings.append(
            TransportFinding(
                "inline_secret",
                "password and sslpassword must not be embedded in connection configuration",
            )
        )
    if (passfile := parameters.get("passfile")) and (
        finding := _private_file_finding(passfile, "passfile")
    ):
        findings.append(finding)

    sslcert = parameters.get("sslcert")
    sslkey = parameters.get("sslkey")
    if bool(sslcert) != bool(sslkey):
        findings.append(
            TransportFinding(
                "client_identity_pair",
                "sslcert and sslkey must be configured together",
            )
        )
    if sslcert and (finding := _regular_file_finding(sslcert, "sslcert")):
        findings.append(finding)
    if sslkey and (finding := _private_file_finding(sslkey, "sslkey")):
        findings.append(finding)

    root_ca_sha256, root_ca_finding = _audit_root_ca(parameters.get("sslrootcert"))
    if root_ca_finding:
        findings.append(root_ca_finding)

    return TransportAudit(
        passed=not findings,
        policy_sha256=policy.sha256,
        endpoint_count=len(hosts),
        sslmode=sslmode,
        channel_binding=channel_binding,
        connect_timeout_seconds=connect_timeout,
        target_session_attrs=target_session_attrs,
        minimum_tls_version=minimum_tls_version,
        root_ca_sha256=root_ca_sha256,
        findings=tuple(findings),
    )
