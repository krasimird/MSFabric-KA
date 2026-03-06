# GenDWH Documentation Library Notebook
# =======================================
# This notebook is a shared library loaded by each phase notebook via %run.
# It contains ALL configuration, helpers, and phase functions.
# No cell executes anything — definitions only.

# CELL 0 ── Pip Installs ───
# Install required packages (idempotent in Fabric)
%pip install anthropic --quiet
%pip install requests --quiet

# CELL 1 ── Imports, CONFIG & LIB_VERSION ───
import json
import os
import re
import time
import requests
from datetime import datetime, timezone

LIB_VERSION = "0.1.0"

CONFIG = {
    # ── Claude / Anthropic ──────────────────────────
    "CLAUDE_MODEL": "claude-sonnet-4-20250514",
    "CLAUDE_MAX_TOKENS": 16384,

    # ── Fabric API ──────────────────────────────────
    "FABRIC_API_BASE": "https://api.fabric.microsoft.com/v1",

    # ── Environment Detection ───────────────────────
    #   Workspace name suffix → environment label
    "ENV_MAP": {
        "_WS_D": "Dev",
        "_WS_T": "Test",
        "_WS_P": "Prod",
    },
    "TARGET_ENV": "Dev",

    # ── Lakehouse ───────────────────────────────────
    "DEFAULT_LAKEHOUSE": "GenDWH_Documentation_LH",
}

# CELL 2 ── Brand & Style Classes ───
class Brand:
    """Visual branding constants for HTML reports."""
    PRIMARY = "#0078D4"
    SECONDARY = "#50E6FF"
    BACKGROUND = "#F3F2F1"
    FONT_FAMILY = "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif"

class Style:
    """CSS style helpers for notebook display output."""

    @staticmethod
    def header(title: str, subtitle: str = "") -> str:
        """Return styled HTML header string."""
        sub = f"<p style='color:#555;'>{subtitle}</p>" if subtitle else ""
        return (
            f"<div style='font-family:{Brand.FONT_FAMILY}; padding:12px; "
            f"background:{Brand.BACKGROUND}; border-left:4px solid {Brand.PRIMARY};'>"
            f"<h2 style='color:{Brand.PRIMARY}; margin:0;'>{title}</h2>{sub}</div>"
        )

    @staticmethod
    def status_badge(label: str, ok: bool = True) -> str:
        """Return a coloured status badge."""
        colour = "#107C10" if ok else "#D83B01"
        return f"<span style='color:{colour}; font-weight:bold;'>[{label}]</span>"

# CELL 3 ── API Helpers ───
def get_fabric_headers() -> dict:
    """Return authorisation headers for the Fabric REST API.

    In a Fabric notebook the access token is available via
    mssparkutils.credentials.getToken().
    """
    # Stub — actual implementation will call mssparkutils
    return {"Authorization": "Bearer <token>", "Content-Type": "application/json"}


def call_claude(prompt: str, system: str = "") -> str:
    """Send a prompt to the Anthropic Claude API and return the response text.

    Uses CONFIG values for model and max_tokens.
    """
    # Stub — actual implementation will use the anthropic SDK
    return ""


def log(message: str) -> None:
    """Print a timestamped log message."""
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {message}")



# CELL 4 ── Phase 0 — Discovery ───
def phase_0_discovery() -> dict:
    """Discover warehouse objects (schemas, tables, views, procedures).

    Returns a catalogue dict summarising what was found.
    """
    log("Phase 0 — Discovery: starting …")
    # Stub
    return {"schemas": [], "tables": [], "views": [], "procedures": []}

# CELL 5 ── Phase 1 — Profiling ───
def phase_1_profiling(catalogue: dict) -> dict:
    """Profile each table/view — row counts, column stats, sample data.

    Returns enriched catalogue with profiling metadata.
    """
    log("Phase 1 — Profiling: starting …")
    # Stub
    return catalogue

# CELL 6 ── Phase 2 — AI Documentation ───
def phase_2_ai_documentation(catalogue: dict) -> dict:
    """Use Claude to generate natural-language documentation for each object.

    Returns catalogue with AI-generated descriptions attached.
    """
    log("Phase 2 — AI Documentation: starting …")
    # Stub
    return catalogue

# CELL 7 ── Phase 3 — Relationship Mapping ───
def phase_3_relationship_mapping(catalogue: dict) -> dict:
    """Detect and document foreign-key and logical relationships.

    Returns catalogue with relationship graph.
    """
    log("Phase 3 — Relationship Mapping: starting …")
    # Stub
    return catalogue

# CELL 8 ── Phase 4 — Publishing ───
def phase_4_publishing(catalogue: dict) -> None:
    """Publish the documentation artefacts (HTML, JSONL) to the Lakehouse.

    Writes files to CONFIG['DEFAULT_LAKEHOUSE'].
    """
    log("Phase 4 — Publishing: starting …")
    # Stub
    pass

# CELL 9 ── Phase 5 — Versioning ───
def phase_5_versioning(catalogue: dict) -> None:
    """Snapshot the current documentation version for change tracking.

    Stores a versioned copy in the Lakehouse.
    """
    log("Phase 5 — Versioning: starting …")
    # Stub
    pass

# CELL 10 ── run_all & Library Loaded Confirmation ───
def run_all() -> None:
    """Execute all phases in sequence."""
    cat = phase_0_discovery()
    cat = phase_1_profiling(cat)
    cat = phase_2_ai_documentation(cat)
    cat = phase_3_relationship_mapping(cat)
    phase_4_publishing(cat)
    phase_5_versioning(cat)
    log("All phases complete.")

print(f"✔ GenDWH_Documentation_LIB v{LIB_VERSION} loaded  |  env={CONFIG['TARGET_ENV']}")