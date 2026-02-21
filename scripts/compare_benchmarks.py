#!/usr/bin/env python3
"""compare_benchmarks.py — Compare Criterion benchmark results against stored baselines.

Usage:
    python3 scripts/compare_benchmarks.py [--baseline DIR] [--current DIR] [--warn PCT] [--fail PCT] [--output FILE]

Defaults:
    --baseline  benches/baselines/
    --current   target/criterion/
    --warn      10     (±10% produces a warning)
    --fail      15     (>15% regression fails the check)
    --output    benchmark_report.md
"""

import argparse
import json
import os
import sys
from pathlib import Path


def parse_criterion_estimates(criterion_dir: Path) -> dict[str, float]:
    """Walk a Criterion output tree and extract median times (ns)."""
    results = {}
    for estimates_file in criterion_dir.rglob("new/estimates.json"):
        # Path: <group>/<benchmark>/new/estimates.json
        parts = estimates_file.relative_to(criterion_dir).parts
        if len(parts) >= 3:
            bench_name = "/".join(parts[:-2])  # e.g. "tpch/query/q01"
        else:
            bench_name = str(estimates_file.parent.parent)

        try:
            with open(estimates_file) as f:
                data = json.load(f)
            median_ns = data.get("median", {}).get("point_estimate", 0.0)
            if median_ns > 0:
                results[bench_name] = median_ns
        except (json.JSONDecodeError, KeyError, OSError):
            continue
    return results


def load_baseline(baseline_dir: Path) -> dict[str, float]:
    """Load baseline from a simple JSON file."""
    baseline_file = baseline_dir / "baseline.json"
    if not baseline_file.exists():
        return {}
    try:
        with open(baseline_file) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_baseline(baseline_dir: Path, results: dict[str, float]) -> None:
    """Save current results as the new baseline."""
    baseline_dir.mkdir(parents=True, exist_ok=True)
    baseline_file = baseline_dir / "baseline.json"
    with open(baseline_file, "w") as f:
        json.dump(results, f, indent=2, sort_keys=True)
    print(f"Baseline saved to {baseline_file}")


def compare(
    baseline: dict[str, float],
    current: dict[str, float],
    warn_pct: float,
    fail_pct: float,
) -> tuple[list[dict], bool]:
    """Compare current results against baseline, return (rows, has_failure)."""
    rows = []
    has_failure = False

    all_benchmarks = sorted(set(baseline.keys()) | set(current.keys()))
    for name in all_benchmarks:
        base_ns = baseline.get(name)
        curr_ns = current.get(name)

        if base_ns is None:
            rows.append({
                "name": name,
                "baseline_ms": "—",
                "current_ms": f"{curr_ns / 1e6:.3f}" if curr_ns else "—",
                "change_pct": "NEW",
                "status": "🆕",
            })
            continue

        if curr_ns is None:
            rows.append({
                "name": name,
                "baseline_ms": f"{base_ns / 1e6:.3f}",
                "current_ms": "—",
                "change_pct": "REMOVED",
                "status": "🗑️",
            })
            continue

        change_pct = ((curr_ns - base_ns) / base_ns) * 100.0

        if change_pct > fail_pct:
            status = "❌ REGRESSION"
            has_failure = True
        elif change_pct > warn_pct:
            status = "⚠️ WARNING"
        elif change_pct < -warn_pct:
            status = "🚀 FASTER"
        else:
            status = "✅ OK"

        rows.append({
            "name": name,
            "baseline_ms": f"{base_ns / 1e6:.3f}",
            "current_ms": f"{curr_ns / 1e6:.3f}",
            "change_pct": f"{change_pct:+.1f}%",
            "status": status,
        })

    return rows, has_failure


def render_markdown(rows: list[dict], warn_pct: float, fail_pct: float) -> str:
    """Render comparison table as GitHub-flavored markdown."""
    lines = [
        "## Benchmark Results",
        "",
        f"Thresholds: warning ±{warn_pct}%, failure >{fail_pct}%",
        "",
        "| Benchmark | Baseline (ms) | Current (ms) | Change | Status |",
        "|-----------|--------------|-------------|--------|--------|",
    ]

    for row in rows:
        lines.append(
            f"| `{row['name']}` | {row['baseline_ms']} | {row['current_ms']} "
            f"| {row['change_pct']} | {row['status']} |"
        )

    # Summary
    total = len(rows)
    regressions = sum(1 for r in rows if "REGRESSION" in r["status"])
    warnings = sum(1 for r in rows if "WARNING" in r["status"])
    faster = sum(1 for r in rows if "FASTER" in r["status"])
    ok = sum(1 for r in rows if r["status"] == "✅ OK")

    lines.extend([
        "",
        "### Summary",
        f"- **Total benchmarks**: {total}",
        f"- ✅ OK: {ok}",
        f"- 🚀 Faster: {faster}",
        f"- ⚠️ Warnings: {warnings}",
        f"- ❌ Regressions: {regressions}",
    ])

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Compare benchmark results against baselines")
    parser.add_argument("--baseline", default="benches/baselines", help="Baseline directory")
    parser.add_argument("--current", default="target/criterion", help="Current Criterion output")
    parser.add_argument("--warn", type=float, default=10.0, help="Warning threshold (%%)")
    parser.add_argument("--fail", type=float, default=15.0, help="Failure threshold (%%)")
    parser.add_argument("--output", default="benchmark_report.md", help="Output markdown file")
    parser.add_argument("--save-baseline", action="store_true", help="Save current as new baseline")
    args = parser.parse_args()

    baseline_dir = Path(args.baseline)
    current_dir = Path(args.current)

    # Parse current results
    current = parse_criterion_estimates(current_dir)
    if not current:
        print(f"No benchmark results found in {current_dir}", file=sys.stderr)
        print("Run benchmarks first: cargo bench", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(current)} benchmark results in {current_dir}")

    # Save baseline if requested
    if args.save_baseline:
        save_baseline(baseline_dir, current)
        return

    # Load baseline
    baseline = load_baseline(baseline_dir)
    if not baseline:
        print(f"No baseline found in {baseline_dir}. Creating initial baseline.")
        save_baseline(baseline_dir, current)
        print("Run again after next benchmark to compare.")
        sys.exit(0)

    print(f"Loaded {len(baseline)} baseline entries from {baseline_dir}")

    # Compare
    rows, has_failure = compare(baseline, current, args.warn, args.fail)

    # Render report
    report = render_markdown(rows, args.warn, args.fail)
    output_path = Path(args.output)
    output_path.write_text(report)
    print(f"Report written to {output_path}")
    print()
    print(report)

    if has_failure:
        print("\n❌ Performance regression detected!", file=sys.stderr)
        sys.exit(1)
    else:
        print("\n✅ No regressions detected.")


if __name__ == "__main__":
    main()
