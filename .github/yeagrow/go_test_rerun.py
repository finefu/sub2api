#!/usr/bin/env python3
"""Run `go test` for the YeaGrow execution-plane CI with one visible rerun.

Upstream sub2api has a few memory/allocation-sensitive tests (process-wide
heap or AllocsPerRun thresholds) that occasionally fail on loaded runners.
This wrapper runs the whole suite once; packages that fail are rerun ONCE.
Every first-attempt failure is reported as a GitHub ::warning:: annotation and
in the job summary, so a rerun is never silent. The job fails if:
  - any package still fails on its rerun,
  - a package failed to build (no rerun),
  - more than MAX_RERUN_PACKAGES packages failed (not treated as flaky).

Usage: go_test_rerun.py <label> [extra go test flags...]   (run in backend/)
Origin: finefu/sub2api branch yeagrow/execution-plane.
"""
import json
import os
import subprocess
import sys

MAX_RERUN_PACKAGES = 3


def run(args, log_path):
    with open(log_path, "w") as log:
        proc = subprocess.run(["go", "test", "-json", *args], stdout=log, stderr=subprocess.STDOUT)
    return proc.returncode


def parse(log_path):
    """Return (failed_packages, failed_tests, build_failed_packages, counts)."""
    failed_pkgs, failed_tests, build_failed = set(), {}, set()
    counts = {"pass": 0, "fail": 0, "skip": 0}
    with open(log_path) as log:
        for line in log:
            line = line.strip()
            if not line.startswith("{"):
                if line.startswith("FAIL") and "[build failed]" in line:
                    build_failed.add(line.split()[1])
                continue
            try:
                event = json.loads(line)
            except ValueError:
                continue
            action, pkg, test = event.get("Action"), event.get("Package"), event.get("Test")
            if test and action in counts:
                counts[action] += 1
                if action == "fail":
                    failed_tests.setdefault(pkg, []).append(test)
            elif not test and action == "fail" and pkg:
                failed_pkgs.add(pkg)
            if event.get("Action") == "output" and "[build failed]" in event.get("Output", "") and pkg:
                build_failed.add(pkg)
            if action == "build-fail":
                build_failed.add(event.get("ImportPath", pkg or "?"))
    return failed_pkgs, failed_tests, build_failed, counts


def summary(text):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a") as f:
            f.write(text + "\n")
    print(text)


def top_level(tests):
    return sorted({t.split("/")[0] for t in tests})


def main():
    label, extra = sys.argv[1], sys.argv[2:]
    first_log = f"go-test-{label}.json"
    code = run([*extra, "./..."], first_log)
    failed_pkgs, failed_tests, build_failed, counts = parse(first_log)
    summary(f"### go test ({label})\n\nFirst attempt: {counts['pass']} passed, {counts['fail']} failed, {counts['skip']} skipped.")
    if code == 0 and not failed_pkgs:
        summary("No failures, no rerun.")
        return 0
    if build_failed:
        summary(f"**Build failure (no rerun):** {', '.join(sorted(build_failed))}")
        return 1
    if not failed_pkgs:
        summary(f"go test exited {code} without a failing package; failing the job.")
        return 1
    if len(failed_pkgs) > MAX_RERUN_PACKAGES:
        summary(f"**{len(failed_pkgs)} packages failed (> {MAX_RERUN_PACKAGES}); not treated as flaky, no rerun.**")
        for pkg in sorted(failed_pkgs):
            summary(f"- `{pkg}`: {', '.join(top_level(failed_tests.get(pkg, [])))}")
        return 1

    summary("\n| package | failed on first attempt | rerun |\n| --- | --- | --- |")
    still_failing = []
    for index, pkg in enumerate(sorted(failed_pkgs)):
        tests = top_level(failed_tests.get(pkg, []))
        for test in tests:
            print(f"::warning title=Flaky test rerun ({label})::{pkg} {test} failed on the first attempt; package is rerun once")
        rerun_log = f"go-test-{label}-rerun-{index}.json"
        rerun_code = run(["-count=1", *extra, pkg], rerun_log)
        rerun_failed, rerun_tests, _, rerun_counts = parse(rerun_log)
        ok = rerun_code == 0 and not rerun_failed
        result = f"passed ({rerun_counts['pass']} tests)" if ok else f"FAILED: {', '.join(top_level(sum(rerun_tests.values(), [])))}"
        summary(f"| `{pkg}` | {', '.join(f'`{t}`' for t in tests) or '(package-level)'} | {result} |")
        if ok:
            print(f"::warning title=Passed only on rerun ({label})::{pkg} passed on its single rerun after: {', '.join(tests)}")
        else:
            print(f"::error title=Failed on rerun ({label})::{pkg} failed again: {', '.join(top_level(sum(rerun_tests.values(), [])))}")
            still_failing.append(pkg)
    if still_failing:
        summary(f"\n**Still failing after one rerun:** {', '.join(still_failing)}")
        return 1
    summary("\nAll first-attempt failures passed on their single rerun (see warnings above).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
