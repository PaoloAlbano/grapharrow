"""Verify the badge's denominator and its per-scenario regression check."""

import json
import runpy
from collections import Counter
from pathlib import Path
from xml.etree import ElementTree

import pytest

BADGE_SCRIPT = Path(__file__).resolve().parents[2] / ".github" / "opencypher_badge.py"


def test_opencypher_badge_detects_a_regression_with_the_same_pass_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    badge = runpy.run_path(str(BADGE_SCRIPT))
    baseline = tmp_path / "opencypher-baseline.json"
    summary = tmp_path / "step-summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))

    def write_report(**scenarios: str) -> None:
        (tmp_path / "cases.jsonl").write_text(
            "".join(
                json.dumps({"id": scenario, "status": status}) + "\n"
                for scenario, status in scenarios.items()
            )
        )
        (tmp_path / "cases.summary.json").write_text(
            json.dumps(
                {
                    "total": len(scenarios),
                    "statuses": Counter(scenarios.values()),
                    "features_sha256": "pinned-corpus",
                }
            )
        )

    write_report(read="passed", bad_query="negative_passed", blocked="blocked_setup")
    assert badge["report_status"](tmp_path, baseline) == ("66.7% (2/3)", True)
    initial = baseline.read_bytes()

    write_report(read="query_rejected", bad_query="negative_passed", blocked="passed")
    assert badge["report_status"](tmp_path, baseline) == ("66.7% (2/3)", False)
    assert baseline.read_bytes() == initial
    assert "`read`" in summary.read_text()


def test_opencypher_failure_badge_has_accessible_svg_text() -> None:
    badge = runpy.run_path(str(BADGE_SCRIPT))
    root = ElementTree.fromstring(badge["render_badge"]("audit failed", "#b91c1c"))
    assert root.attrib["aria-label"] == "openCypher M23: audit failed"
