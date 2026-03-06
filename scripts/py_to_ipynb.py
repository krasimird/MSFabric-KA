#!/usr/bin/env python3
"""Convert .py notebooks (with cell-marker convention) to Fabric-compatible .ipynb files.

Cell marker format:
    # CELL <N> ── <Title> ───

The script parses each .py file in notebooks/, splits on cell markers,
and writes a valid nbformat-4 .ipynb into build/.
"""

import json
import os
import re
import sys
from pathlib import Path

CELL_MARKER_RE = re.compile(
    r"^# CELL \d+ (?:──|─-) .+ (?:───|─--)$"
)

FABRIC_KERNEL_SPEC = {
    "display_name": "Python 3 (ipykernel)",
    "language": "python",
    "name": "python3",
}

FABRIC_LANGUAGE_INFO = {
    "name": "python",
    "version": "3.10.0",
    "mimetype": "text/x-python",
    "file_extension": ".py",
    "codemirror_mode": {"name": "ipython", "version": 3},
    "pygments_lexer": "ipython3",
    "nbconvert_exporter": "python",
}

NBFORMAT_METADATA = {
    "kernelspec": FABRIC_KERNEL_SPEC,
    "language_info": FABRIC_LANGUAGE_INFO,
}


def parse_cells(source_text: str) -> list[dict]:
    """Split source text on cell markers and return a list of notebook cells."""
    lines = source_text.split("\n")
    cells: list[dict] = []
    current_lines: list[str] = []
    in_cell = False

    for line in lines:
        if CELL_MARKER_RE.match(line):
            # Flush previous cell
            if in_cell:
                cells.append(_make_cell(current_lines))
            current_lines = []
            in_cell = True
            continue
        if in_cell:
            current_lines.append(line)

    # Flush last cell
    if in_cell and current_lines:
        cells.append(_make_cell(current_lines))

    return cells


def _make_cell(lines: list[str]) -> dict:
    """Create a notebook code cell from lines."""
    # Strip leading/trailing blank lines but preserve internal structure
    while lines and lines[0].strip() == "":
        lines = lines[1:]
    while lines and lines[-1].strip() == "":
        lines = lines[:-1]

    source = [line + "\n" for line in lines]
    if source:
        source[-1] = source[-1].rstrip("\n")  # last line has no trailing newline

    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source,
    }


def build_notebook(cells: list[dict]) -> dict:
    """Wrap cells in a valid nbformat 4 notebook structure."""
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": NBFORMAT_METADATA,
        "cells": cells,
    }


def convert_file(src: Path, dst: Path) -> None:
    """Convert a single .py file to .ipynb."""
    text = src.read_text(encoding="utf-8")
    cells = parse_cells(text)
    if not cells:
        print(f"  SKIP {src.name} (no cell markers found)")
        return
    nb = build_notebook(cells)
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(json.dumps(nb, indent=1, ensure_ascii=False), encoding="utf-8")
    print(f"  OK   {src.name} -> {dst.name}  ({len(cells)} cells)")


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    notebooks_dir = repo_root / "notebooks"
    build_dir = repo_root / "build"

    if not notebooks_dir.exists():
        print("ERROR: notebooks/ directory not found", file=sys.stderr)
        sys.exit(1)

    build_dir.mkdir(parents=True, exist_ok=True)

    py_files = sorted(notebooks_dir.glob("*.py"))
    if not py_files:
        print("No .py files found in notebooks/")
        sys.exit(0)

    print(f"Converting {len(py_files)} notebook(s) …")
    converted = 0
    for py_file in py_files:
        ipynb_path = build_dir / py_file.with_suffix(".ipynb").name
        convert_file(py_file, ipynb_path)
        converted += 1

    print(f"\nDone. {converted} file(s) processed → build/")


if __name__ == "__main__":
    main()

