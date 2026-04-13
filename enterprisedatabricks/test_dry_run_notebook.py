# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Dry Run Test Notebook
# MAGIC 
# MAGIC **Purpose**: Validate the dry-run mechanism works correctly before building the `.whl` file.
# MAGIC 
# MAGIC **How to use**: 
# MAGIC 1. Clone the repo into your Databricks workspace
# MAGIC 2. Open this notebook
# MAGIC 3. Update the `CONFIG_DIR` and `CONFIG_FILE` variables below to point to your YAML
# MAGIC 4. Run all cells

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: Add source code to Python path
# MAGIC This lets us import directly from the repo without building a WHL.

# COMMAND ----------

import sys
import os

# Point to the src folder in your repo
# Adjust this path based on where your repo is cloned in the workspace
REPO_ROOT = os.path.dirname(os.path.abspath("__file__"))  # notebook location
SRC_PATH = os.path.join(REPO_ROOT, "core_utils", "src")

# If running from the repo root, this will work.
# Otherwise, set the full path manually:
# SRC_PATH = "/Workspace/Repos/<your-user>/EnterpriseDatabricksDeltaLake/core_utils/src"

if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

print(f"✅ Added to sys.path: {SRC_PATH}")
print(f"📁 Contents: {os.listdir(SRC_PATH) if os.path.exists(SRC_PATH) else 'PATH NOT FOUND - update SRC_PATH!'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Configuration — Set your YAML details here

# COMMAND ----------

# ──────────────────────────────────────────────────
# UPDATE THESE TO MATCH YOUR CONFIG
# ──────────────────────────────────────────────────
CONFIG_DIR  = "/Workspace/Repos/<your-user>/EnterpriseDatabricksDeltaLake/deltalake_dabs/configs"
CONFIG_FILE = "fiserv_email.yml"
LAYER       = "silver"
EMAIL_ID    = "test@company.com"
# ──────────────────────────────────────────────────

print(f"Config path: {CONFIG_DIR}/{LAYER}/{CONFIG_FILE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Simulate CLI arguments
# MAGIC Since we're in a notebook (not CLI), we fake the `sys.argv` that `argparse` reads from.

# COMMAND ----------

import sys

# Simulate the CLI arguments that would be passed in a Databricks job task
sys.argv = [
    "test_notebook",                          # script name (ignored by argparse)
    "--config_file_name", CONFIG_FILE,
    "--config_directory_path", CONFIG_DIR,
    "--email_id", EMAIL_ID,
    "--layer", LAYER,
    "--dry-run",                              # ← THIS IS THE KEY FLAG
]

print(f"✅ Simulated CLI args: {sys.argv[1:]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Run the dry-run — Step by Step
# MAGIC This runs each piece individually so you can see what's happening.

# COMMAND ----------

# Step 1: Parse arguments
from framework.core.arg_parser import ArgParser

args = ArgParser.parse()
print(f"\n✅ Args parsed. dry_run = {args.dry_run}")

# COMMAND ----------

# Step 2: Load YAML
from pathlib import Path
from framework.main import load_yaml

yaml_path = str(Path(args.config_directory_path) / args.layer / args.config_file_name)
print(f"📄 Loading YAML from: {yaml_path}")

config = load_yaml(yaml_path)
print(f"✅ YAML loaded. Keys: {list(config.keys())}")

# COMMAND ----------

# Step 3: Validate config
from framework.core.config import ConfigManager

ConfigManager.validate_config(["pipeline_metadata", "source"], config)
print("✅ Config validation passed — 'pipeline_metadata' and 'source' keys present.")

# COMMAND ----------

# Step 4: Build RuntimeContext
from framework.core.runtime import RuntimeContext

pipeline = config.get("pipeline_metadata", {})
context = RuntimeContext(
    environment=pipeline.get("environment", "unknown"),
    pipeline_name=pipeline.get("name", "unknown"),
    run_mode=pipeline.get("run_mode", "execute"),
    dry_run=True,
)
print(f"✅ RuntimeContext: {context}")

# COMMAND ----------

# Step 5: IngestionFactory validation (spark=None for dry-run)
from framework.factory.ingestion_factory import IngestionFactory

ingester = IngestionFactory.create(
    spark=None,       # None = dry-run mode, no Spark operations
    config=config,
    env_manager=None,
)
print(f"✅ IngestionFactory validated. Ingester = {ingester} (None is expected in dry-run)")

# COMMAND ----------

# Step 6: Build and print execution plan
from framework.core.plan import build_execution_plan
from framework.core.plan_printer import print_plan

plan = build_execution_plan(config)
print_plan(plan)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: Run it all at once (single call)
# MAGIC This is what the actual WHL entry point does.

# COMMAND ----------

# Reset sys.argv for a clean run
sys.argv = [
    "test_notebook",
    "--config_file_name", CONFIG_FILE,
    "--config_directory_path", CONFIG_DIR,
    "--email_id", EMAIL_ID,
    "--layer", LAYER,
    "--dry-run",
]

from framework.main import main
main()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: Test error handling — bad source type

# COMMAND ----------

import yaml

bad_config = {
    "pipeline_metadata": {"name": "bad_test", "layer": "bronze"},
    "source": {"type": "ftp"},  # ← invalid source type!
}

try:
    IngestionFactory.create(spark=None, config=bad_config, env_manager=None)
except ValueError as e:
    print(f"✅ Correctly caught error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: Test error handling — missing source key

# COMMAND ----------

incomplete_config = {
    "pipeline_metadata": {"name": "incomplete_test", "layer": "bronze"},
    # missing "source" entirely!
}

try:
    ConfigManager.validate_config(["pipeline_metadata", "source"], incomplete_config)
except ValueError as e:
    print(f"✅ Correctly caught error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ If all cells pass, your dry-run mechanism is working correctly!
# MAGIC 
# MAGIC You can now safely build the `.whl` file.
