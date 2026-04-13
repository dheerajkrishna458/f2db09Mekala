# framework/core/runtime.py

from dataclasses import dataclass
from typing import Optional


@dataclass
class RuntimeContext:
    """
    Lightweight runtime context object — no Spark dependency.

    Bundles the key runtime state needed to describe the current
    pipeline execution.  Used by the dry-run plan builder and kept
    deliberately free of any PySpark imports so it can be constructed
    and serialised without a running cluster.
    """
    environment: str
    pipeline_name: str
    run_mode: str
    dry_run: bool
    job_run_id: Optional[str] = None
