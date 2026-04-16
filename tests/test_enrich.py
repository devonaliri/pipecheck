import pytest
from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.enrich import EnrichChange, EnrichResult, enrich_schema


def _col(name, data_type="string", nullable=False, description="", tags=None):
    return ColumnSchema(name=name, data_type=data_type, nullable=nullable,
                        description=description, tags=tags or [])


def _make_schema(name, columns):
    return PipelineSchema(name=name, version="1", description="", columns=columns)


class TestEnrichResult:
    def test_has_changes_false_when_empty(self):
        schema = _make_schema("s", [])
        r = EnrichResult(schema=schema, changes=[])
        assert not r.has_changes()

    def test_has_changes_true_when_present(self):
        schema = _make_schema("s", [])
        r = EnrichResult(schema=schema, changes=[EnrichChange("col", "description", "", "desc")])
        assert r.has_changes()

    def test_str_no_changes(self):
        schema = _make_schema("pipe", [])
        r = EnrichResult(schema=schema, changes=[])
        assert "nothing to enrich" in str(r)

    def test_str_with_changes(self):
        schema = _make_schema("pipe", [])
        r = EnrichResult(schema=schema, changes=[
            EnrichChange("col_a", "description", "", "A column")
        ])
        text = str(r)
        assert "1 enrichment" in text
        assert "col_a" in text


class TestEnrichSchema:
    def test_no_changes_when_already_documented(self):
        target = _make_schema("t", [_col("id", description="identifier", tags=["pk"])])
        ref = _make_schema("r", [_col("id", description="other desc", tags=["ref"])])
        result = enrich_schema(target, ref)
        assert not result.has_changes()
        assert result.schema.columns[0].description == "identifier"

    def test_fills_missing_description(self):
        target = _make_schema("t", [_col("id")])
        ref = _make_schema("r", [_col("id", description="Primary key")])
        result = enrich_schema(target, ref)
        assert result.has_changes()
        assert result.schema.columns[0].description == "Primary key"

    def test_fills_missing_tags(self):
        target = _make_schema("t", [_col("email")])
        ref = _make_schema("r", [_col("email", tags=["pii"])])
        result = enrich_schema(target, ref)
        assert result.has_changes()
        assert result.schema.columns[0].tags == ["pii"]

    def test_column_not_in_ref_unchanged(self):
        target = _make_schema("t", [_col("extra")])
        ref = _make_schema("r", [])
        result = enrich_schema(target, ref)
        assert not result.has_changes()
        assert result.schema.columns[0].name == "extra"

    def test_schema_name_preserved(self):
        target = _make_schema("my_pipeline", [_col("col")])
        ref = _make_schema("ref_pipeline", [_col("col", description="desc")])
        result = enrich_schema(target, ref)
        assert result.schema.name == "my_pipeline"

    def test_change_str_format(self):
        c = EnrichChange("user_id", "description", "", "The user identifier")
        assert "user_id" in str(c)
        assert "description" in str(c)
