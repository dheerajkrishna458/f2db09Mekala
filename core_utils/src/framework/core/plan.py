# framework/core/plan.py

from typing import Dict, Any


def build_execution_plan(config) -> Dict[str, Any]:
    """
    Build a side-effect-free description of the pipeline execution.

    Takes a validated config (the parsed YAML dict) and produces a
    plan dictionary that describes *what would happen* without
    actually doing it.  This is the heart of the dry-run mechanism.

    Parameters
    ----------
    config : dict
        The full validated YAML pipeline config.

    Returns
    -------
    dict
        A structured plan describing pipeline, source, targets, and
        feature flags.
    """
    pipeline = config.get("pipeline_metadata", {})
    source = config.get("source", {})
    destination = config.get("destination", {})

    plan = {
        "pipeline": {
            "name": pipeline.get("name"),
            "environment": pipeline.get("environment"),
            "layer": pipeline.get("layer"),
            "run_mode": pipeline.get("run_mode"),
        },
        "source": {
            "type": source.get("type"),
            "incremental": getattr(source, "incremental", None)
                           if hasattr(source, "incremental")
                           else source.get("incremental"),
        },
        "targets": [],
        "features": {},
    }

    # Bronze
    if pipeline.get("layer") == "bronze":
        plan["targets"].append({
            "table": destination.get("table"),
            "write_mode": destination.get("mode"),
            "corrupt_record": destination.get("corrupt_record"),
        })

    # Silver
    if pipeline.get("layer") == "silver":
        target = config.get("destination", {})
        plan["targets"].append({
            "table": target.get("table"),
            "primary_keys": target.get("primary_keys"),
            "soft_deletes": target.get("enable_soft_deletes"),
            "schema_evolution": target.get("schema_evolution"),
        })

    # Gold
    if pipeline.get("layer") == "gold":
        # Gold may have multiple targets
        gold_targets = config.get("target", config.get("destination", {}))

        if isinstance(gold_targets, list):
            for t in gold_targets:
                plan["targets"].append({
                    "name": t.get("name"),
                    "target": t.get("target"),
                    "materialization": t.get("materialization"),
                    "write_mode": t.get("write_mode"),
                    "empty_result": t.get("empty_result"),
                })
        else:
            # Single target treated as dict
            plan["targets"].append({
                "table": gold_targets.get("table"),
                "write_mode": gold_targets.get("mode"),
            })

    # Feature flags (safe to extend later)
    plan["features"] = {
        "security": bool(config.get("security")),
        "data_quality": bool(config.get("data_quality")),
        "governance": bool(config.get("governance")),
    }

    return plan
