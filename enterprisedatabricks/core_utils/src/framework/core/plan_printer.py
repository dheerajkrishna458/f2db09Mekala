# framework/core/plan_printer.py

import json


def print_plan(plan: dict) -> None:
    """
    Pretty-print the dry-run execution plan for Spoke teams.

    This is what makes dry-run **actually useful** — a human-readable,
    PR-reviewable, CI-friendly output that describes what the pipeline
    would do if executed for real.

    Parameters
    ----------
    plan : dict
        The execution plan dictionary built by ``build_execution_plan()``.
    """
    print("\nDRY RUN - EXECUTION PLAN")
    print("=" * 60)
    print(json.dumps(plan, indent=2, default=str))
    print("=" * 60)
    print("[OK] Configuration valid. No actions were executed.\n")
