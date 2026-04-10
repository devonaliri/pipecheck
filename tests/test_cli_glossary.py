"""Tests for pipecheck.cli_glossary."""
import argparse
import pytest
from pathlib import Path
from pipecheck.cli_glossary import cmd_glossary, add_glossary_parser
from pipecheck.glossary import add_term, GlossaryTerm


@pytest.fixture()
def base_dir(tmp_path: Path) -> Path:
    return tmp_path


def _args(base_dir, action, **kwargs):
    ns = argparse.Namespace(dir=str(base_dir), glossary_action=action, **kwargs)
    return ns


class TestCmdGlossary:
    def test_add_exits_zero(self, base_dir):
        args = _args(base_dir, "add", name="churn", definition="Customer churn rate", aliases="")
        assert cmd_glossary(args) == 0

    def test_add_with_aliases(self, base_dir):
        args = _args(base_dir, "add", name="rev", definition="Revenue", aliases="revenue,total_rev")
        assert cmd_glossary(args) == 0

    def test_remove_existing_exits_zero(self, base_dir):
        add_term(base_dir, GlossaryTerm("churn", "Customer churn"))
        args = _args(base_dir, "remove", name="churn")
        assert cmd_glossary(args) == 0

    def test_remove_missing_exits_one(self, base_dir):
        args = _args(base_dir, "remove", name="nonexistent")
        assert cmd_glossary(args) == 1

    def test_lookup_existing_exits_zero(self, base_dir, capsys):
        add_term(base_dir, GlossaryTerm("arpu", "Average revenue per user"))
        args = _args(base_dir, "lookup", name="arpu")
        code = cmd_glossary(args)
        assert code == 0
        out = capsys.readouterr().out
        assert "arpu" in out

    def test_lookup_by_alias(self, base_dir, capsys):
        add_term(base_dir, GlossaryTerm("arpu", "Average revenue per user", aliases=["avg_rev"]))
        args = _args(base_dir, "lookup", name="avg_rev")
        assert cmd_glossary(args) == 0

    def test_lookup_missing_exits_one(self, base_dir):
        args = _args(base_dir, "lookup", name="unknown")
        assert cmd_glossary(args) == 1

    def test_list_empty_exits_zero(self, base_dir, capsys):
        args = _args(base_dir, "list")
        assert cmd_glossary(args) == 0
        assert "empty" in capsys.readouterr().out

    def test_list_shows_terms(self, base_dir, capsys):
        add_term(base_dir, GlossaryTerm("dau", "Daily active users"))
        args = _args(base_dir, "list")
        cmd_glossary(args)
        assert "dau" in capsys.readouterr().out


class TestAddGlossaryParser:
    def test_parser_registers_glossary_command(self):
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers(dest="command")
        add_glossary_parser(sub)
        args = parser.parse_args(["glossary", "list"])
        assert args.glossary_action == "list"

    def test_add_subcommand_parsed(self):
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers(dest="command")
        add_glossary_parser(sub)
        args = parser.parse_args(["glossary", "add", "term", "definition"])
        assert args.name == "term"
        assert args.definition == "definition"
