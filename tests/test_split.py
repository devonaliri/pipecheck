"""Tests for pipecheck.split."""
from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.split import SplitResult, split_schema


def _col(name: str, dtype: str = "string", nullable: bool = True, tags=None) -> ColumnSchema:
    return ColumnSchema(name=name, dtype=dtype, nullable=nullable, tags=tags or [])


def _make_schema(*cols: ColumnSchema) -> PipelineSchema:
    return PipelineSchema(
        name="orders",
        version="1.0",
        description="test schema",
        columns=list(cols),
    )


class TestSplitResult:
    def test_has_matched_true(self):
        schema = _make_schema(_col("id", "integer"))
        result = split_schema(schema, lambda c: c.dtype == "integer")
        assert result.has_matched is True

    def test_has_matched_false(self):
        schema = _make_schema(_col("name", "string"))
        result = split_schema(schema, lambda c: c.dtype == "integer")
        assert result.has_matched is False

    def test_has_remainder_true(self):
        schema = _make_schema(_col("id", "integer"), _col("name", "string"))
        result = split_schema(schema, lambda c: c.dtype == "integer")
        assert result.has_remainder is True

    def test_has_remainder_false(self):
        schema = _make_schema(_col("id", "integer"))
        result = split_schema(schema, lambda c: c.dtype == "integer")
        assert result.has_remainder is False

    def test_str_contains_source_name(self):
        schema = _make_schema(_col("id", "integer"))
        result = split_schema(schema, lambda c: c.dtype == "integer")
        assert "orders" in str(result)

    def test_str_shows_matched_columns(self):
        schema = _make_schema(_col("id", "integer"), _col("name", "string"))
        result = split_schema(schema, lambda c: c.dtype == "integer")
        assert "id" in str(result)

    def test_str_shows_remainder_columns(self):
        schema = _make_schema(_col("id", "integer"), _col("name", "string"))
        result = split_schema(schema, lambda c: c.dtype == "integer")
        assert "name" in str(result)


class TestSplitSchema:
    def test_matched_columns_correct(self):
        schema = _make_schema(_col("id", "integer"), _col("name", "string"), _col("age", "integer"))
        result = split_schema(schema, lambda c: c.dtype == "integer")
        assert result.matched_columns == ["id", "age"]

    def test_remainder_columns_correct(self):
        schema = _make_schema(_col("id", "integer"), _col("name", "string"), _col("age", "integer"))
        result = split_schema(schema, lambda c: c.dtype == "integer")
        assert result.remainder_columns == ["name"]

    def test_default_matched_name(self):
        schema = _make_schema(_col("id", "integer"))
        result = split_schema(schema, lambda c: True)
        assert result.matched.name == "orders_matched"

    def test_default_remainder_name(self):
        schema = _make_schema(_col("id", "integer"))
        result = split_schema(schema, lambda c: False)
        assert result.remainder.name == "orders_remainder"

    def test_custom_matched_name(self):
        schema = _make_schema(_col("id", "integer"))
        result = split_schema(schema, lambda c: True, matched_name="ids_only")
        assert result.matched.name == "ids_only"

    def test_custom_remainder_name(self):
        schema = _make_schema(_col("id", "integer"))
        result = split_schema(schema, lambda c: False, remainder_name="leftovers")
        assert result.remainder.name == "leftovers"

    def test_version_preserved_in_matched(self):
        schema = _make_schema(_col("id", "integer"))
        result = split_schema(schema, lambda c: True)
        assert result.matched.version == schema.version

    def test_version_preserved_in_remainder(self):
        schema = _make_schema(_col("id", "integer"))
        result = split_schema(schema, lambda c: False)
        assert result.remainder.version == schema.version

    def test_split_by_nullable(self):
        schema = _make_schema(_col("id", nullable=False), _col("note", nullable=True))
        result = split_schema(schema, lambda c: not c.nullable)
        assert result.matched_columns == ["id"]
        assert result.remainder_columns == ["note"]

    def test_split_by_tag(self):
        schema = _make_schema(
            _col("ssn", tags=["pii"]),
            _col("name", tags=["pii"]),
            _col("amount", tags=[]),
        )
        result = split_schema(schema, lambda c: "pii" in c.tags)
        assert set(result.matched_columns) == {"ssn", "name"}
        assert result.remainder_columns == ["amount"]
