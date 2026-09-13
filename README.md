# Databricks Dashboard in a Day

A hands-on workshop covering the full Databricks BI stack — from data integration to dashboards and AI-powered analytics. Use it for instructor-led trainings or self-paced learning. This project is open source — contributions and feedback are welcome!

<div style="text-align:left;">
  <img src="./labs/artifacts/screenshots/Dashboard_Final.png" width="100%">
</div>

## Step 1 – Create a Databricks Free Account

Sunny Bay uses **Databricks** for analytics; you will too.

### ✅ Instructions
1. Go to [https://databricks.com/try-databricks](https://databricks.com/try-databricks)
2. Click **"Try Databricks for free"**
3. Sign up with GitHub or Google.
4. Select **Free Edition**.
5. When ready, note your workspace URL.

---

## Step 2 – Clone This Repo & Set Up the Workshop with Lab 0

**Clone the GitHub repository into your Databricks workspace**.

### ✅ Instructions
1. In Databricks, go to the sidebar and select **Workspace**.
2. Navigate to your desired folder (e.g., under your username).
3. Click **Create > Git folder**.
4. Enter the repository URL: `https://github.com/DatabricksDashboardInADay/DatabricksDashboardInADay`
5. Click **Create**.
6. Expand the repo folder and open the `Lab 0` notebook in the folder `labs`.

## Step 3 – Choose Your Starting Point

### ✅ Instructions
The labs are modular — you can start from any lab. Lab 0 pre-deploys all necessary assets (pipelines, Metric View, dashboards), so feel free to skip ahead to the topics that interest you most. Lab 1 is an **exploration lab** for analysts and analytics engineers: since Lab 0 already built the data, Lab 1 focuses on getting to know the workspace, Unity Catalog, lineage, and Genie Code. Prefer to build the medallion architecture yourself? Two optional Deep Dives ([SDP] and [SQL]) cover that data-engineering path.

| Lab | Topic | Guide |
|-----|-------|-------|
| **Lab 0** | Setup: Clone the repo, deploy assets, and configure the workspace | [guide](labs/Lab%200%20-%20Intro.ipynb) |
| **Lab 1** | Explore Your Data: Tour the workspace, Unity Catalog, lineage, and Genie Code | [guide](labs/Lab%201%20-%20Explore%20Your%20Data%20with%20Unity%20Catalog%20and%20Genie%20Code.md) |
| **Lab 2** | Data Modelling: Create Metric Views to add business semantics to gold data | [guide](labs/Lab%202%20-%20Data%20Modelling.md) |
| **Lab 3** | Dashboard Creation: Build interactive AI/BI Dashboards | [guide](labs/Lab%203%20-%20Dashboard%20Creation.md) |
| **Lab 4** | BI Meets AI: Build a Genie Agent for natural-language analytics | [guide](labs/Lab%204%20-%20BI%20Meets%20AI.md) |

### 🔎 Deep Dives (Optional)

Deep dives are standalone labs that go deeper into a specific topic. They can be completed at any point after their prerequisite lab and are independent of each other.

| Deep Dive | What to Expect | Prerequisite |
|-----------|---------------|--------------|
| **[SDP] Building the Medallion Pipeline** ([guide](labs/Deep%20Dives/%5BSDP%5D%20Building%20the%20Medallion%20Pipeline.md)) | Build the bronze → silver → gold medallion yourself with Spark Declarative Pipelines: add data-quality expectations, derived columns, and an aggregated gold table. | Lab 0 |
| **[SQL] Building the Medallion with SQL** ([guide](labs/Deep%20Dives/%5BSQL%5D%20Building%20the%20Medallion%20with%20SQL.md)) | Build the same medallion architecture with pure SQL on a SQL Warehouse (creates parallel `*_sql` tables so nothing collides with the deployed data). | Lab 0 |
| **[SQL] SQL Analyst Essentials** ([guide](labs/Deep%20Dives/%5BSQL%5D%20SQL%20Analyst%20Essentials.md)) | Explore data with ad-hoc queries, create reusable SQL views, and use Genie Code to generate and optimise SQL. | [SQL] Building the Medallion with SQL |
| **[SQL] Monitoring and Self-Service** ([guide](labs/Deep%20Dives/%5BSQL%5D%20Monitoring%20and%20Self-Service.md)) | Schedule recurring queries, set up SQL alerts, upload CSV data for self-service analysis, and explore Genie One (formerly Databricks One). | [SQL] SQL Analyst Essentials |
