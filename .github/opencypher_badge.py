"""Render the README badge and compare successive openCypher audit results."""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from pathlib import Path
from xml.sax.saxutils import escape

PASSING = {"passed", "negative_passed"}
UNEXPECTED = {
    "error_side_effects",
    "harness_error",
    "header_mismatch",
    "internal_error",
    "invalid_query_accepted",
    "result_mismatch",
    "side_effect_mismatch",
    "timeout",
}


def render_badge(message: str, color: str) -> str:
    label = "openCypher M23"
    message = escape(message, {'"': "&quot;"})
    left, right = 112, 154
    width = left + right
    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="20"
     role="img" aria-label="{label}: {message}">
  <title>{label}: {message}</title>
  <rect width="{width}" height="20" rx="3" fill="{color}"/>
  <path d="M3 0H{left}V20H3a3 3 0 0 1-3-3V3a3 3 0 0 1 3-3" fill="#555"/>
  <g fill="#fff" font-family="Verdana, sans-serif" font-size="11" text-anchor="middle">
    <text x="{left // 2}" y="14">{label}</text>
    <text x="{left + right // 2}" y="14">{message}</text>
  </g>
</svg>
"""


def report_status(report: Path, baseline: Path) -> tuple[str, bool]:
    summary = json.loads((report / "cases.summary.json").read_text())
    rows = [json.loads(line) for line in (report / "cases.jsonl").read_text().splitlines()]
    counts = Counter(row["status"] for row in rows)
    if (
        not rows
        or len({row["id"] for row in rows}) != len(rows)
        or len(rows) != summary["total"]
        or counts != summary["statuses"]
    ):
        raise ValueError("Scenario results do not match the audit summary")

    passed = {row["id"] for row in rows if row["status"] in PASSING}
    previous = json.loads(baseline.read_text()) if baseline.exists() else None
    if previous and previous["features_sha256"] != summary["features_sha256"]:
        raise ValueError("The audit corpus differs from the published baseline")
    lost = sorted(set(previous["passing_ids"]) - passed) if previous else []
    unexpected = sum(counts[status] for status in UNEXPECTED)
    percentage = len(passed) / len(rows)
    message = f"{percentage:.1%} ({len(passed):,}/{len(rows):,})"
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as output:
        output.write(f"## openCypher M23\n\n**{message} scenarios passed.**\n\n")
        for status, count in sorted(counts.items()):
            output.write(f"- `{status}` {count:,}\n")
        if lost:
            output.write(f"\n{len(lost)} previously passing scenarios regressed.\n")
            for scenario in lost[:25]:
                output.write(f"- `{scenario}`\n")

    if unexpected or lost:
        return message, False
    baseline.write_text(
        json.dumps(
            {
                "features_sha256": summary["features_sha256"],
                "passing_ids": sorted(passed),
            },
            indent=2,
        )
        + "\n"
    )
    return message, True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    message, passed = "audit failed", False
    try:
        message, passed = report_status(args.report, args.output / "opencypher-baseline.json")
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Unable to score the audit: {error}")
    color = "#007ec6" if passed else "#b91c1c"
    (args.output / "opencypher.svg").write_text(render_badge(message, color))
    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
