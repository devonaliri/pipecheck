"""Tests for pipecheck.promote."""
import pytest

from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.promote import promote_schema, PromoteChange, PromoteResult


def _col(name: str, data_type: str = "string", nullable: bool = True) -> ColumnSchema:
    return ColumnSchema(name=name, data_type=data_type, nullable=nullable)


def _make_schema(name: str, cols) -> PipelineSchema:
    return PipelineSchema(name=name, version="1.0", description="", columns=cols)


class TestPromoteResult:
    def test_has_changes_false_when_identical(self):
        src = _make_schema("pipe", [_col("id")])
        tgt = _make_schema("pipe", [_col("id")])
        result = promote_schema(src, tgt)
        assert not result.has_changes

    def test_has_changes_true_when_column_added(self):
        src = _make_schema("pipe", [_col("id"), _col("name")])
        tgt = _make_schema("pipe", [_col("id")])
        result = promote_schema(src, tgt)
        assert result.has_changes

    def test_is_safe_when_no_breaking(self):
        src = _make_schema("pipe", [_col("id"), _col("name")])
        tgt = _make_schema("pipe", [_col("id")])
        result = promote_schema(src, tgt)
        assert result.is_safe

    def test_is_not_safe_when_column_removed(self):
        src = _make_schema("pipe", [_col("id")])
        tgt = _make_schema("pipe", [_col("id"), _col("name")])
        result = promote_schema(src, tgt)
        assert not result.is_safe

    def test_dry_run_flag_preserved(self):
        src = _make_schema("pipe", [_col("id")])
        tgt = _make_schema("pipe", [_col("id")])
        result = promote_schema(src, tgt, dry_run=True)
        assert result.dry_run is True

    def test_promoted_schema_has_source_columns(self):
        src = _make_schema("pipe", [_col("id"), _col("ts")])
        tgt = _make_schema("pipe", [_col("id")])
        result = promote_schema(src, tgt)
        col_names = [c.name for c in result.promoted_schema.columns]
        assert "ts" in col_names

    def test_promoted_schema_retains_target_name(self):
        src = _make_schema("pipe_staging", [_col("id")])
        tgt = _make_schema("pipe_prod", [_col("id")])
        result = promote_schema(src, tgt)
        assert result.promoted_schema.name == "pipe_prod"

    def test_promoted_schema_has_source_version(self):
        src = _make_schema("pipe", [_col("id")])
        src.version = "2.0"
        tgt = _make_schema("pipe", [_col("id")])
        tgt.version = "1.0"
        result = promote_schema(src, tgt)
        assert result.promoted_schema.version == "2.0"

    def test_add_column_change_recorded(self):
        src = _make_schema("pipe", [_col("id"), _col("new_col", "integer")])
        tgt = _make_schema("pipe", [_col("id")])
        result = promote_schema(src, tgt)
        kinds = [c.kind for c in result.changes]
        assert "add_column" in kinds

    def test_remove_column_change_recorded(self):
        src = _make_schema("pipe", [_col("id")])
        tgt = _make_schema("pipe", [_col("id"), _col("old_col")])
        result = promote_schema(src, tgt)
        kinds = [c.kind for c in result.changes]
        assert "remove_column" in kinds

    def test_type_change_recorded(self):
        src = _make_schema("pipe", [_col("id", "bigint")])
        tgt = _make_schema("pipe", [_col("id", "integer")])
        result = promote_schema(src, tgt)
        kinds = [c.kind for c in result.changes]
        assert "update_type" in kinds

    def test_str_contains_env_names(self):
        src = _make_schema("pipe", [_col("id")])
        tgt = _make_schema("pipe", [_col("id")])
        result = promote_schema(src, tgt, source_env="dev", target_env="prod")
        text = str(result)
        assert "dev" in text
        assert "prod" in text

    def test_str_lists_changes(self):
        src = _make_schema("pipe", [_col("id"), _col("extra")])
        tgt = _make_schema("pipe", [_col("id")])
        result = promote_schema(src, tgt)
        text = str(result)
        assert "extra" in text
