# Data Analyst in a Day

## Step 1 – Create a Databricks Free Account

Sunny Bay uses **Databricks** for analytics; you will too.

### ✅ Instructions
1. Go to [https://databricks.com/try-databricks](https://databricks.com/try-databricks)
2. Click **"Try Databricks for free"**
3. Sign up with GitHub or Google.
4. Select **Community Edition** (or Free Trial if available).
5. When ready, note your workspace URL.

---

## Step 2 – Clone This Repo & Set Up the Workshop with Lab 0

**Clone the GitHub repository into your Databricks workspace**.

### ✅ Instructions
1. In Databricks, go to the sidebar and select **Workspace**.
2. Navigate to your desired folder (e.g., under your username).
3. Click **Create > Git folder**.
4. Enter the current URL: e.g., `https://github.com/DatabricksDashboardInADay/DatabricksDashboardInADay.git`
5. Click **Create**.
6. Expand the repo folder and open the `Lab 0` notebook in the folder `labs`.

## Step 3 – Choose Your Starting Point
**Decide where to start the workshop**.

### ✅ Instructions
Once Lab 0 has finished deploying the initial assets, you can decide where you want to start the workshop. Because Lab 0 pre-deploys the necessary Spark Declarative Pipelines, Metric View (`sm_fact_coffee_sales_fallback`), and AI/BI Dashboards (`[Final] Sunny Bay Roastery - Sales Report`), you have the flexibility to skip ahead to the topics that interest you most.

---

### 🔀 Choose Your Path

| Path | Best For | Lab Sequence |
|------|----------|-------------|
| **SDP (default)** | Data engineers, pipeline-oriented users | Lab 1 → Lab 2 → Lab 3 → Lab 4 |
| **SQL Analyst** | SQL analysts, BI analysts | Lab 1 [SQL] → Lab 2 [SQL] → Lab 3 [SQL] → Lab 2 → Lab 3 → Lab 4 |

**SQL Analyst track:**
1. **Lab 1 [SQL]** – Build the medallion architecture with pure SQL ([guide](labs/Lab%201%20-%20%5BSQL%5D%20Data%20Integration%20and%20Transformation.md))
2. **Lab 2 [SQL]** – Explore data, create views, use the Databricks Assistant ([guide](labs/Lab%202%20-%20%5BSQL%5D%20SQL%20Analyst%20Essentials.md))
3. **Lab 3 [SQL]** – Schedule queries, set up alerts, upload CSV data, explore Databricks One ([guide](labs/Lab%203%20-%20%5BSQL%5D%20Monitoring%20and%20Self-Service.md))
4. Then continue with shared **Lab 2** (Metric Views) → **Lab 3** (Dashboards) → **Lab 4** (Genie)

For facilitator routing guidance and detailed scope, see [SQL Analyst Lab Takeover Guide](docs/sql_analyst_lab_takeover.md).
