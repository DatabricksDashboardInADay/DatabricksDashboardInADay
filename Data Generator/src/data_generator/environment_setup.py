def setup_catalog_and_schemas(
    catalog_name='sunny_bay_roastery',
    bronze_schema_name='bronze',
    silver_schema_name='silver',
    gold_schema_name='gold',
    volume_name='raw'
):
    from pyspark.sql import SparkSession
    from IPython.display import display, HTML

    spark = SparkSession.builder.getOrCreate()

    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
        display(HTML('<span style="color:green;font-size:20px;">&#x2705;</span> Catalog creation attempted successfully.'))
    except Exception:
        display(HTML('<span style="color:red;font-size:20px;">&#x26D4;</span> There is a problem with creating catalogs in the databricks workspaces of the FE Vending Machine.'))
    else:
        display(HTML('<span style="color:green;font-size:20px;">&#x2705;</span> Catalog created or already exists.'))

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{bronze_schema_name}")
    display(HTML(f'<span style="color:green;font-size:20px;">&#x2705;</span> Schema {bronze_schema_name} created or already exists in catalog {catalog_name}.'))

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{silver_schema_name}")
    display(HTML(f'<span style="color:green;font-size:20px;">&#x2705;</span> Schema {silver_schema_name} created or already exists in catalog {catalog_name}.'))

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{gold_schema_name}")
    display(HTML(f'<span style="color:green;font-size:20px;">&#x2705;</span> Schema {gold_schema_name} created or already exists in catalog {catalog_name}.'))

    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{bronze_schema_name}.{volume_name}")
    display(HTML(f'<span style="color:green;font-size:20px;">&#x2705;</span> Volume {volume_name} created or already exists in schema {bronze_schema_name} of catalog {catalog_name}.'))