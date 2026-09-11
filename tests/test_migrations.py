from dbbench.migrations import Migration, discover_migrations


def test_packaged_migrations_are_ordered_and_content_addressed() -> None:
    migrations = discover_migrations()
    assert [migration.version for migration in migrations] == [1, 2, 3]
    assert all(len(migration.checksum) == 64 for migration in migrations)
    assert "CREATE TABLE transfers" in migrations[0].sql
    assert "CREATE INDEX" in migrations[1].sql
    assert "durable_jobs" in migrations[2].sql


def test_migration_is_an_immutable_value_object() -> None:
    migration = Migration(1, "core", "SELECT 1", "abc")
    try:
        migration.version = 2
    except (AttributeError, TypeError):
        pass
    else:
        raise AssertionError("migration unexpectedly allowed mutation")
