"""Tests for pipecheck.stats."""
import pytest

from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.stats import SchemaStats, compute_stats, compare_stats


def _make_schema(
    name: str = "orders",
    version: str = "1.0",
    columns=None,
) -> PipelineSchema:
    if columns is None:
        columns = [
            ColumnSchema(name="id", data_type="integer", nullable=False),
            ColumnSchema(name="amount", data_type="float", nullable=True),
            ColumnSchema(name="status", data_type="string", nullable=True),
        ]
    return PipelineSchema(name=name, version=version, columns=columns)


class TestComputeStats:
    def test_total_columns(self):
        stats = compute_stats(_make_schema())
        assert stats.total_columns == 3

    def test_nullable_count(self):
        stats = compute_stats(_make_schema())
        assert stats.nullable_count == 2
        assert stats.non_nullable_count == 1

    def test_nullable_ratio(self):
        stats = compute_stats(_make_schema())
        assert stats.nullable_ratio == pytest.approx(2 / 3, rel=1e-4)

    def test_type_distribution(self):
        stats = compute_stats(_make_schema())
        assert stats.type_distribution["integer"] == 1
        assert stats.type_distribution["float"] == 1
        assert stats.type_distribution["string"] == 1

    def test_empty_schema(self):
        schema = PipelineSchema(name="empty", version="0.1", columns=[])
        stats = compute_stats(schema)
        assert stats.total_columns == 0
        assert stats.nullable_ratio == 0.0
        assert stats.type_distribution == {}

    def test_schema_name_and_version(self):
        stats = compute_stats(_make_schema(name="users", version="2.3"))
        assert stats.name == "users"
        assert stats.version == "2.3"

    def test_duplicate_types_counted(self):
        cols = [
            ColumnSchema(name="a", data_type="string", nullable=False),
            ColumnSchema(name="b", data_type="string", nullable=False),
            ColumnSchema(name="c", data_type="integer", nullable=False),
        ]
        stats = compute_stats(_make_schema(columns=cols))
        assert stats.type_distribution["string"] == 2
        assert stats.type_distribution["integer"] == 1


class TestSchemaStatsStr:
    def test_str_contains_name(self):
        stats = compute_stats(_make_schema())
        assert "orders" in str(stats)

    def test_str_contains_column_count(self):
        stats = compute_stats(_make_schema())
        assert "3 total" in str(stats)

    def test_str_contains_type(self):
        stats = compute_stats(_make_schema())
        assert "float" in str(stats)


class TestCompareStats:
    def _stats_a(self):
        return compute_stats(_make_schema())

    def _stats_b(self):
        cols = [
            ColumnSchema(name="id", data_type="integer", nullable=False),
            ColumnSchema(name="amount", data_type="float", nullable=True),
            ColumnSchema(name="status", data_type="string", nullable=True),
            ColumnSchema(name="note", data_type="string", nullable=True),
        ]
        return compute_stats(_make_schema(columns=cols))

    def test_column_delta(self):
        diff = compare_stats(self._stats_a(), self._stats_b())
        assert diff["column_delta"] == 1

    def test_nullable_delta(self):
        diff = compare_stats(self._stats_a(), self._stats_b())
        assert diff["nullable_delta"] == 1

    def test_no_added_or_removed_types(self):
        diff = compare_stats(self._stats_a(), self._stats_b())
        assert diff["added_types"] == []
        assert diff["removed_types"] == []

    def test_removed_type_detected(self):
        cols = [
            ColumnSchema(name="id", data_type="integer", nullable=False),
        ]
        stats_b = compute_stats(_make_schema(columns=cols))
        diff = compare_stats(self._stats_a(), stats_b)
        assert "float" in diff["removed_types"]
        assert "string" in diff["removed_types"]
