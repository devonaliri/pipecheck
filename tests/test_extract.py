"""Tests for pipecheck.extract."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.extract import ExtractResult, extract_schema


def _col(name: str, dtype: str = "string", nullable: bool = False) -> ColumnSchema:
    return ColumnSchema(name=name, type=dtype, nullable=nullable)


def _make_schema(*col_names: str) -> PipelineSchema:
    return PipelineSchema(
        name="orders",
        version="1",
        description="test",
        columns=[_col(n) for n in col_names],
    )


class TestExtractResult:
    def test_has_changes_false_when_nothing_dropped(self):
        r = ExtractResult(source_name="s", extracted=[_col("a")], dropped=[])
        assert not r.has_changes()

    def test_has_changes_true_when_dropped(self):
        r = ExtractResult(source_name="s", extracted=[_col("a")], dropped=[_col("b")])
        assert r.has_changes()

    def test_str_shows_extracted(self):
        r = ExtractResult(source_name="s", extracted=[_col("id", "int")], dropped=[])
        out = str(r)
        assert "id" in out
        assert "int" in out

    def test_str_shows_dropped(self):
        r = ExtractResult(source_name="s", extracted=[_col("a")], dropped=[_col("b")])
        out = str(r)
        assert "- b" in out

    def test_str_no_match_message(self):
        r = ExtractResult(source_name="s", extracted=[], dropped=[_col("a")])
        assert "No columns matched" in str(r)

    def test_to_schema_uses_source_name(self):
        r = ExtractResult(source_name="src", extracted=[_col("x")], dropped=[])
        s = r.to_schema()
        assert s.name == "src"

    def test_to_schema_uses_new_name(self):
        r = ExtractResult(source_name="src", extracted=[_col("x")], dropped=[])
        s = r.to_schema(new_name="dest")
        assert s.name == "dest"

    def test_to_schema_columns_match_extracted(self):
        cols = [_col("a"), _col("b")]
        r = ExtractResult(source_name="s", extracted=cols, dropped=[])
        assert r.to_schema().columns == cols


class TestExtractSchema:
    def test_raises_when_no_selector(self):
        schema = _make_schema("a", "b")
        with pytest.raises(ValueError):
            extract_schema(schema)

    def test_extract_by_name_list(self):
        schema = _make_schema("id", "name", "email")
        result = extract_schema(schema, columns=["id", "email"])
        assert [c.name for c in result.extracted] == ["id", "email"]
        assert [c.name for c in result.dropped] == ["name"]

    def test_extract_by_pattern(self):
        schema = _make_schema("created_at", "updated_at", "id")
        result = extract_schema(schema, pattern="*_at")
        assert [c.name for c in result.extracted] == ["created_at", "updated_at"]
        assert [c.name for c in result.dropped] == ["id"]

    def test_extract_combined_name_and_pattern(self):
        schema = _make_schema("id", "created_at", "status")
        result = extract_schema(schema, columns=["id"], pattern="*_at")
        names = [c.name for c in result.extracted]
        assert "id" in names
        assert "created_at" in names
        assert "status" not in names

    def test_no_match_returns_empty_extracted(self):
        schema = _make_schema("a", "b")
        result = extract_schema(schema, columns=["z"])
        assert result.extracted == []
        assert len(result.dropped) == 2

    def test_source_name_preserved(self):
        schema = _make_schema("a")
        result = extract_schema(schema, columns=["a"])
        assert result.source_name == "orders"
