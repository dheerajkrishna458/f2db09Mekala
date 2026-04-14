# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Dry Run Test Notebook
# MAGIC 
# MAGIC **Purpose**: Validate the dry-run mechanism works correctly before building the `.whl` file.
# MAGIC 
# MAGIC **How to use**: 
# MAGIC 1. Clone the repo into your Databricks workspace
# MAGIC 2. Open this notebook
# MAGIC 3. Update the `CONFIG_DIR`, `CONFIG_FILE`, and `LAYER` variables below to point to your YAML
# MAGIC 4. Run all cells
# MAGIC 
# MAGIC **What this tests**: The real entry points (`run_bronze`, `run_silver`, `run_gold`) with `--dry-run`.

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
# MAGIC ## Cell 3: Dry-run via entry point
# MAGIC This simulates `sys.argv` and calls the real entry point — the same
# MAGIC code path that a Databricks job task would execute.

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

# Run through the real entry point
from framework.main import run_silver
run_silver()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Test error handling — bad source type
# MAGIC Verifies the `IngestionFactory` rejects unknown source types.

# COMMAND ----------

from framework.factory.ingestion_factory import IngestionFactory

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
# MAGIC ## Cell 5: Test error handling — missing source key
# MAGIC Verifies `ConfigManager.validate_config` catches missing required keys.

# COMMAND ----------

from framework.core.config import ConfigManager

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
