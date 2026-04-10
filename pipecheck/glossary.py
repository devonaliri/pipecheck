"""Glossary: attach human-readable term definitions to schema columns."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class GlossaryTerm:
    name: str
    definition: str
    aliases: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        parts = [f"{self.name}: {self.definition}"]
        if self.aliases:
            parts.append(f"  aliases: {', '.join(self.aliases)}")
        return "\n".join(parts)

    def to_dict(self) -> dict:
        return {"name": self.name, "definition": self.definition, "aliases": self.aliases}

    @classmethod
    def from_dict(cls, data: dict) -> "GlossaryTerm":
        return cls(
            name=data["name"],
            definition=data["definition"],
            aliases=data.get("aliases", []),
        )


def _glossary_path(base_dir: Path) -> Path:
    return base_dir / ".pipecheck" / "glossary.json"


def load_glossary(base_dir: Path) -> Dict[str, GlossaryTerm]:
    path = _glossary_path(base_dir)
    if not path.exists():
        return {}
    with path.open() as fh:
        raw = json.load(fh)
    return {entry["name"]: GlossaryTerm.from_dict(entry) for entry in raw}


def save_glossary(base_dir: Path, terms: Dict[str, GlossaryTerm]) -> None:
    path = _glossary_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump([t.to_dict() for t in terms.values()], fh, indent=2)


def add_term(base_dir: Path, term: GlossaryTerm) -> None:
    terms = load_glossary(base_dir)
    terms[term.name] = term
    save_glossary(base_dir, terms)


def remove_term(base_dir: Path, name: str) -> bool:
    terms = load_glossary(base_dir)
    if name not in terms:
        return False
    del terms[name]
    save_glossary(base_dir, terms)
    return True


def lookup(base_dir: Path, name: str) -> Optional[GlossaryTerm]:
    terms = load_glossary(base_dir)
    if name in terms:
        return terms[name]
    for term in terms.values():
        if name in term.aliases:
            return term
    return None
