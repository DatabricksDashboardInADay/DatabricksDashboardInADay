# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Dashboard in a Day — one-click setup
# MAGIC
# MAGIC Run this notebook **once** to stand up everything the labs need. Just press
# MAGIC **Run all** at the top (serverless — no cluster to pick). It:
# MAGIC 1. **Creates the catalog** (default `sunny_bay_roastery`) — or reuses it if it
# MAGIC    already exists / an admin pre-created it.
# MAGIC 2. **Deploys the bundle** — the setup job, the Lakeflow medallion pipeline, and
# MAGIC    the two AI/BI dashboards (`[Template]` and `[Final]` Sunny Bay Sales Report).
# MAGIC 3. **Runs the setup job** end-to-end — generates the Sunny Bay sales data, runs
# MAGIC    the bronze→silver→gold pipeline, builds the metric view, and pre-builds the
# MAGIC    Sales Genie.
# MAGIC
# MAGIC When the last cell finishes green, every asset the labs reference is ready and
# MAGIC you can head to **Lab 1**.
# MAGIC
# MAGIC > **Why a notebook and not just the "Deploy" button?** A Lakeflow pipeline's target
# MAGIC > `catalog` is validated by Unity Catalog **at bundle-deploy time** — earlier than
# MAGIC > any job task can run — so the catalog has to exist *before* deploy. On Free
# MAGIC > Edition the catalog can only be made with SQL `CREATE CATALOG`, so this notebook
# MAGIC > creates it first, then deploys and runs — one ordered, Free-Edition-safe path.

# COMMAND ----------
# MAGIC %md
# MAGIC ## ⚙️ Configuration
# MAGIC
# MAGIC The defaults work as-is on Free Edition. Change these only if you need to:
# MAGIC - **`catalog`** — the Unity Catalog catalog to build everything in. On a shared
# MAGIC   workshop, give each person their own name so tables don't collide.
# MAGIC - **`prefix`** — set this **only** if several people share the *same* catalog and
# MAGIC   schema (e.g. your name). It's added to every object name so your objects stay
# MAGIC   separate. Leave it empty if you're working on your own.
# MAGIC - **`target`** — the bundle target to deploy. `dev` is the only one and the default.

# COMMAND ----------

dbutils.widgets.text("catalog", "sunny_bay_roastery")
dbutils.widgets.text("prefix", "")
dbutils.widgets.text("target", "dev")

catalog = dbutils.widgets.get("catalog").strip()
target = dbutils.widgets.get("target").strip() or "dev"

# Normalize the prefix: add a trailing underscore so names read cleanly
# (e.g. "alice" -> "alice_dim_customer"). An empty prefix leaves names unchanged.
prefix = dbutils.widgets.get("prefix").strip()
if prefix and not prefix.endswith("_"):
    prefix += "_"

if not catalog:
    raise ValueError("Set the 'catalog' widget to a catalog name (default sunny_bay_roastery).")

# The gold schema is fixed by the bundle (the `dev` target defines it). The dashboard
# JSON below and the metric view are built against it.
GOLD_SCHEMA = "gold"

print(f"catalog : {catalog}")
print(f"prefix  : {prefix or '(none)'}")
print(f"target  : {target}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Create the catalog (or reuse an existing one)

# COMMAND ----------

# CREATE CATALOG works on Free Edition (SQL path); if creation is restricted (a
# locked-down workspace where an admin pre-provisions catalogs), fall back to USE.
# Only fail if the catalog can neither be created nor accessed.
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    print(f"✅ Catalog ready (created or already existed): {catalog}")
except Exception as create_err:
    try:
        spark.sql(f"USE CATALOG `{catalog}`")
        print(f"✅ Catalog `{catalog}` already exists and is usable "
              f"(creation was restricted, using the existing one).")
    except Exception as use_err:
        raise RuntimeError(
            f"Cannot create or access catalog `{catalog}`.\n"
            f"  - creation failed: {create_err}\n"
            f"  - access failed:   {use_err}\n"
            "Ask an admin to create it (or grant you CREATE CATALOG), or set the "
            "'catalog' widget to a catalog you can write to."
        ) from use_err

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Locate the bundle

# COMMAND ----------

import os
import re
import subprocess
import tempfile

# This notebook lives at <repo>/bundle/src/notebooks/bootstrap inside the Git Folder,
# so the bundle root (where databricks.yml lives) is two directories up, and the repo
# root (the Git Folder) is one more up.
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
notebook_path = "/Workspace" + ctx.notebookPath().get()
bundle_root = os.path.abspath(os.path.join(os.path.dirname(notebook_path), "..", ".."))
repo_root = os.path.dirname(bundle_root)

if not os.path.exists(os.path.join(bundle_root, "databricks.yml")):
    raise RuntimeError(
        f"databricks.yml not found at {bundle_root}. This notebook must run from inside "
        "the cloned Git Folder (bundle/src/notebooks/bootstrap). If you copied it "
        "elsewhere, move it back next to the bundle/ tree."
    )
print(f"Bundle root: {bundle_root}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Point the `[Final]` dashboard at your catalog
# MAGIC
# MAGIC The `[Final]` dashboard's dataset binds to the **metric view** by fully-qualified
# MAGIC name, so the JSON ships with `__CATALOG__`/`__GOLD_SCHEMA__`/`__PREFIX__`
# MAGIC placeholders. DABs uploads the file as-is, so we substitute your values in before
# MAGIC deploy. We start from the pristine, committed version each run, so re-running with
# MAGIC a different catalog re-targets correctly.

# COMMAND ----------

dash_rel = "bundle/src/dashboards/dashboard_final.lvdash.json"
dash_abs = os.path.join(repo_root, dash_rel)

# Start from the pristine (placeholder) version committed to git so re-runs re-substitute
# correctly. Fall back to the working-tree copy if git isn't available for any reason.
try:
    pristine = subprocess.run(
        ["git", "-C", repo_root, "show", f"HEAD:{dash_rel}"],
        capture_output=True, text=True, check=True,
    ).stdout
except Exception:
    with open(dash_abs, encoding="utf-8") as f:
        pristine = f.read()

serialized = (
    pristine.replace("__CATALOG__", catalog)
            .replace("__GOLD_SCHEMA__", GOLD_SCHEMA)
            .replace("__PREFIX__", prefix)
)

leftover = [p for p in ("__CATALOG__", "__GOLD_SCHEMA__", "__PREFIX__") if p in serialized]
if leftover:
    raise ValueError(f"Unsubstituted placeholders remain in the dashboard JSON: {leftover}")

with open(dash_abs, "w", encoding="utf-8") as f:
    f.write(serialized)
print(f"✅ Dashboard pointed at {catalog}.{GOLD_SCHEMA}.{prefix or ''}sm_fact_coffee_sales_genie")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Install the Databricks CLI

# COMMAND ----------

# The setup script chooses its own bin dir and prints
# "Installed Databricks CLI vX.Y.Z at <path>." — parse that path.
_install_dir = tempfile.mkdtemp(prefix="dbcli_")
_p = subprocess.run(
    ["bash", "-c",
     f"curl -fsSL -m 90 https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh "
     f"| sh -s -- {_install_dir}"],
    capture_output=True, text=True,
)
_m = re.search(r"Installed Databricks CLI \S+ at (\S+?)\.?$", _p.stdout.strip(), re.M)
if _m and os.path.exists(_m.group(1)):
    CLI = _m.group(1)
elif os.path.exists(os.path.join(_install_dir, "databricks")):
    CLI = os.path.join(_install_dir, "databricks")
else:
    _which = subprocess.run(["bash", "-c", "command -v databricks || true"],
                            capture_output=True, text=True).stdout.strip()
    CLI = _which if _which and os.path.exists(_which) else None

if not CLI:
    raise RuntimeError(f"Could not install the Databricks CLI.\nstdout={_p.stdout}\nstderr={_p.stderr}")

_ver = subprocess.run([CLI, "--version"], capture_output=True, text=True).stdout.strip()
print(f"✅ {_ver} at {CLI}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Authenticate with the notebook's own token
# MAGIC The CLI picks up `DATABRICKS_HOST` / `DATABRICKS_TOKEN` from the environment — the
# MAGIC running user's own workspace credentials, so deploy/run act as you.

# COMMAND ----------

cli_env = dict(
    os.environ,
    DATABRICKS_HOST=ctx.apiUrl().get(),
    DATABRICKS_TOKEN=ctx.apiToken().get(),
    # Deploying from a Git Folder: pin the bundle root so the CLI finds databricks.yml.
    DATABRICKS_BUNDLE_ROOT=bundle_root,
)

# Only pass a --var when the value differs from the bundle default, so the command line
# stays clean. catalog is always passed (it drives the whole workshop).
var_args = ["--var", f"catalog={catalog}"]
if prefix:
    var_args += ["--var", f"prefix={prefix}"]


def run_cli(args, **kw):
    """Run the CLI streaming combined output into the notebook, raise on failure."""
    print(f"$ databricks {' '.join(args)}")
    proc = subprocess.Popen(
        [CLI, *args], cwd=bundle_root, env=cli_env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
    )
    for line in proc.stdout:
        print(line, end="")
    proc.wait()
    if proc.returncode != 0:
        raise RuntimeError(f"`databricks {' '.join(args)}` failed (exit {proc.returncode}).")


run_cli(["current-user", "me", "-o", "json"])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Deploy the bundle
# MAGIC Creates the **Sunny Bay Roastery Job**, the medallion **Lakeflow pipeline**, and
# MAGIC the two AI/BI **dashboards**, all pointed at the catalog you chose above.

# COMMAND ----------

run_cli(["bundle", "deploy", "-t", target, *var_args, "--force-lock"])
print("✅ Bundle deployed (job, pipeline, and dashboards).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Run the setup job end-to-end
# MAGIC This is the long one (~10–15 min): it generates the sales data, runs the
# MAGIC bronze→silver→gold pipeline, builds the metric view, and pre-builds the Sales
# MAGIC Genie. When this cell finishes green, the whole workshop is ready.

# COMMAND ----------

run_cli(["bundle", "run", "sunny_bay_roastery_job", "-t", target, *var_args, "--restart"])

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Setup complete!

# COMMAND ----------

p = prefix or ""
print("=" * 65)
print("✅  SETUP COMPLETE  —  you are ready for Dashboard in a Day!")
print("=" * 65)
print()
print(f"Everything is in catalog `{catalog}`:")
print(f"  🥉 Bronze raw    : {catalog}.bronze.raw (Volume) + generated CSV/Parquet")
print(f"  🥈 Silver / 🥇 Gold star schema : {catalog}.{GOLD_SCHEMA}.{p}fact_coffee_sales + {p}dim_*")
print(f"  📐 Metric view   : {catalog}.{GOLD_SCHEMA}.{p}sm_fact_coffee_sales_genie")
print(f"  🧞 Sales Genie   : pre-built over the metric view")
print(f"  📊 Dashboards    : \"[Template]\" and \"[Final]\" Sunny Bay Roastery - Sales Report")
print()
print("Next: open  labs/Lab 1  and follow along!")
