This asset bundle deploys everything the **Dashboard in a Day** workshop needs: the
setup job, the bronze→silver→gold Lakeflow pipeline, the metric view, the pre-built
Sales Genie, and the two AI/BI dashboards.

## Getting Started (recommended: one-click)

You don't deploy this bundle by hand. Open **[Lab 0 – Intro](../labs/Lab%200%20-%20Intro.ipynb)**
in your Databricks workspace and click **Run all**: it creates your catalog, deploys this
bundle, and runs the setup job end-to-end — no manual steps. The top-level README points
you to the same notebook.

## Advanced: deploy from the Databricks CLI

If you have the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) configured
locally, you can deploy and run the bundle directly:

```bash
databricks bundle deploy -t dev --var catalog=sunny_bay_roastery
databricks bundle run sunny_bay_roastery_job -t dev --var catalog=sunny_bay_roastery
```

The catalog must exist before deploy (the pipeline's target catalog is validated at
deploy time) — create it first with `CREATE CATALOG IF NOT EXISTS`, or use Lab 0, which
handles the ordering for you. On a shared catalog, add `--var prefix=<name>`.

## Managing Resources

- Use the **Create** dropdown to add resources to the asset bundle.
- Click **Schedule** on a notebook within the asset bundle to create a **job definition** that schedules the notebook.

## Documentation

- For information on using **Databricks Asset Bundles in the workspace**, see: [Databricks Asset Bundles in the workspace](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
- For details on the **Databricks Asset Bundles format** used in this asset bundle, see: [Databricks Asset Bundles Configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)
