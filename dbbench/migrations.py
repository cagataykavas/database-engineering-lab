from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass
from importlib import resources

from dbbench.errors import MigrationDriftError


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    name: str
    sql: str
    checksum: str


@dataclass(frozen=True, slots=True)
class MigrationResult:
    version: int
    name: str
    checksum: str
    applied: bool
    duration_ms: float


_MIGRATION_NAME = re.compile(r"^(?P<version>[0-9]{3})_(?P<name>[a-z0-9_]+)\.sql$")


def discover_migrations() -> tuple[Migration, ...]:
    package = resources.files("dbbench.sql")
    migrations: list[Migration] = []
    for resource in package.iterdir():
        match = _MIGRATION_NAME.match(resource.name)
        if not match:
            continue
        sql = resource.read_text(encoding="utf-8")
        migrations.append(
            Migration(
                version=int(match.group("version")),
                name=match.group("name"),
                sql=sql,
                checksum=hashlib.sha256(sql.encode()).hexdigest(),
            )
        )
    migrations.sort(key=lambda migration: migration.version)
    versions = [migration.version for migration in migrations]
    if len(versions) != len(set(versions)):
        raise ValueError("migration versions must be unique")
    return tuple(migrations)


class MigrationRunner:
    """Checksum-verified migration runner serialized by a PostgreSQL advisory lock."""

    def __init__(self, connection, migrations: tuple[Migration, ...] | None = None) -> None:
        self.connection = connection
        self.migrations = migrations or discover_migrations()

    def apply(self) -> tuple[MigrationResult, ...]:
        previous_autocommit = self.connection.autocommit
        self.connection.autocommit = False
        results: list[MigrationResult] = []
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext('dbbench:migrations'))")
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS schema_migrations (
                        version INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        checksum TEXT NOT NULL,
                        duration_ms DOUBLE PRECISION NOT NULL,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cursor.execute("SELECT version, checksum FROM schema_migrations")
                applied = {row["version"]: row["checksum"] for row in cursor.fetchall()}
                for migration in self.migrations:
                    known_checksum = applied.get(migration.version)
                    if known_checksum and known_checksum != migration.checksum:
                        raise MigrationDriftError(
                            f"applied migration {migration.version:03d} checksum changed"
                        )
                    if known_checksum:
                        results.append(
                            MigrationResult(
                                migration.version,
                                migration.name,
                                migration.checksum,
                                False,
                                0.0,
                            )
                        )
                        continue
                    started = time.perf_counter()
                    cursor.execute(migration.sql)
                    duration_ms = (time.perf_counter() - started) * 1000
                    cursor.execute(
                        """
                        INSERT INTO schema_migrations(version, name, checksum, duration_ms)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (migration.version, migration.name, migration.checksum, duration_ms),
                    )
                    results.append(
                        MigrationResult(
                            migration.version,
                            migration.name,
                            migration.checksum,
                            True,
                            duration_ms,
                        )
                    )
            self.connection.commit()
            return tuple(results)
        except Exception:
            self.connection.rollback()
            raise
        finally:
            self.connection.autocommit = previous_autocommit
