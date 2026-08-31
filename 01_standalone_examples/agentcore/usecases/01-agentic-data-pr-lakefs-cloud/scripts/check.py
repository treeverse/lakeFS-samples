"""`make check` -- verify all prerequisites before deployment."""

from __future__ import annotations

from orchestrator.checks import run_checks, summarize


def main() -> int:
    print("Running readiness checks...\n")
    results = run_checks()
    ok, report = summarize(results)
    print(report)
    if ok:
        print("\nAll mandatory checks passed. You can `make deploy`.")
        return 0
    print("\nFix the mandatory failures above before deploying.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
