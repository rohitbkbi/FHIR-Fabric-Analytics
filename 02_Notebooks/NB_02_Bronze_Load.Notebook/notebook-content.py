# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8fb7d13b-34f4-4972-8735-c477be902ff0",
# META       "default_lakehouse_name": "LH_FHIR_Analytics",
# META       "default_lakehouse_workspace_id": "c106b799-22cc-4246-939e-2a7c46460064",
# META       "known_lakehouses": [
# META         {
# META           "id": "8fb7d13b-34f4-4972-8735-c477be902ff0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

print(f">>> ACTUAL resource_type in this run: {resource_type}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    input_file_name,
    regexp_extract,
    explode,
    to_json
)
from datetime import datetime, timezone

spark = SparkSession.builder.getOrCreate()

ingestion_date = None
source_system = "HAPI_FHIR_R4"
api_url = "https://hapi.fhir.org/baseR4"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timezone
import json

target_date = ingestion_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
raw_path = f"Files/raw/fhir/{resource_type.lower()}/ingestion_date={target_date}"

batch_folders = mssparkutils.fs.ls(raw_path)
print(f"Found {len(batch_folders)} batch(es) for {resource_type} on {target_date}")

valid_batches = []

for batch in batch_folders:
    manifest_path = f"{batch.path}/_manifest.json"

    try:
        manifest_content = mssparkutils.fs.head(manifest_path, 10000)
        manifest = json.loads(manifest_content)

        files_in_folder = [
            f.name
            for f in mssparkutils.fs.ls(batch.path)
            if f.name != "_manifest.json"
        ]

        expected_files = manifest["files"]

        if set(files_in_folder) == set(expected_files):
            valid_batches.append(batch.path)
            print(
                f"Batch {manifest['batch_id']} validated - "
                f"{manifest['total_records']} records, "
                f"{manifest['page_count']} pages"
            )
        else:
            print(
                f"Batch at {batch.path} incomplete - "
                f"expected {expected_files}, found {files_in_folder}. Skipping."
            )

    except Exception as e:
        print(f"Could not read manifest for {batch.path}: {e}. Skipping.")

if not valid_batches:
    raise Exception(f"No valid batches found for {resource_type} on {target_date}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_file_paths = []

for batch_path in valid_batches:
    files = [
        f.path
        for f in mssparkutils.fs.ls(batch_path)
        if f.name.endswith(".json") and f.name != "_manifest.json"
    ]
    all_file_paths.extend(files)

print(f"Reading {len(all_file_paths)} page files")

raw_df = (
    spark.read
         .option("multiLine", True)
         .json(all_file_paths)
         .withColumn("file_name", input_file_name())
)

exploded_df = raw_df.select(
    col("_extraction_metadata.*"),
    col("file_name"),
    explode(col("fhir_bundle.entry")).alias("entry")
)

print(f"Exploded to {exploded_df.count()} individual {resource_type} records")
exploded_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_df = exploded_df.select(
    col("entry.resource").alias("resource_data"),
    col("entry.fullUrl").alias("full_url"),
    to_json(col("entry.resource")).alias("resource_json"),
    col("resource_type").alias("source_resource_type"),
    col("batch_id"),
    col("extraction_timestamp").cast("timestamp"),
    col("file_name"),
    lit(source_system).alias("source_system"),
    lit(api_url).alias("api_url"),
    current_timestamp().alias("load_timestamp"),
    lit(target_date).alias("ingestion_date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(bronze_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

table_name = f"bronze_{resource_type.lower()}"
table_exists = spark.catalog.tableExists(table_name)

bronze_df = bronze_df.withColumn("resource_id", col("resource_data.id"))

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

drift_log_records = []

if not table_exists:
    (
        bronze_df.write
        .format("delta")
        .partitionBy("ingestion_date")
        .option("mergeSchema", "true")
        .mode("overwrite")
        .saveAsTable(table_name)
    )

    print(f"Created new table {table_name} with {bronze_df.count()} rows")

    drift_log_records.append({
        "event": "TABLE_CREATED",
        "table_name": table_name,
        "resource_type": resource_type,
        "batch_id": bronze_df.select("batch_id").first()["batch_id"],
        "column_count": len(bronze_df.columns),
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

else:
    delta_table = DeltaTable.forName(spark, table_name)

    existing_schema_fields = set(
        f.name for f in delta_table.toDF().schema.fields
    )

    incoming_schema_fields = set(
        f.name for f in bronze_df.schema.fields
    )

    new_columns = incoming_schema_fields - existing_schema_fields
    missing_columns = existing_schema_fields - incoming_schema_fields

    if new_columns:
        print(f"Schema drift detected: {new_columns}")

        drift_log_records.append({
            "event": "SCHEMA_DRIFT_NEW_COLUMNS",
            "table_name": table_name,
            "resource_type": resource_type,
            "new_columns": list(new_columns),
            "batch_id": bronze_df.select("batch_id").first()["batch_id"],
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

    if missing_columns:
        print(f"Columns missing in current batch: {missing_columns}")

    try:
        (
            delta_table.alias("target")
            .merge(
                bronze_df.alias("source"),
                "target.resource_id = source.resource_id AND target.batch_id = source.batch_id"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"Merged batch into existing table {table_name}")

    except AnalysisException as e:
        print(f"MERGE failed for {table_name}")

        drift_log_records.append({
            "event": "SCHEMA_DRIFT_MERGE_FAILED",
            "table_name": table_name,
            "resource_type": resource_type,
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

        raise
if "log_buffer" not in dir():
    log_buffer = []

for record in drift_log_records:
    log_buffer.append(record)
    print(f"[SCHEMA_DRIFT_LOG] {record}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"OPTIMIZE {table_name}")

print(f"Optimization complete for {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"DESCRIBE DETAIL {table_name}").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 1. Row count sanity check
# MAGIC SELECT COUNT(*) AS total_rows FROM bronze_patient;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 2. Confirm no duplicate resource_id + batch_id combinations (idempotency check)
# MAGIC SELECT resource_id, batch_id, COUNT(*) AS cnt
# MAGIC FROM bronze_patient
# MAGIC GROUP BY resource_id, batch_id
# MAGIC HAVING COUNT(*) > 1;
# MAGIC -- Expect: 0 rows returned


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC 
# MAGIC -- 3. Metadata completeness check — no nulls in required lineage columns
# MAGIC SELECT COUNT(*) AS rows_missing_metadata
# MAGIC FROM bronze_patient
# MAGIC WHERE batch_id IS NULL
# MAGIC    OR extraction_timestamp IS NULL
# MAGIC    OR source_system IS NULL
# MAGIC    OR api_url IS NULL;
# MAGIC -- Expect: 0

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- 3. Metadata completeness check — no nulls in required lineage columns
# MAGIC SELECT COUNT(*) AS rows_missing_metadata
# MAGIC FROM bronze_patient
# MAGIC WHERE batch_id IS NULL
# MAGIC    OR extraction_timestamp IS NULL
# MAGIC    OR source_system IS NULL
# MAGIC    OR api_url IS NULL;
# MAGIC -- Expect: 0

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- 4. Partition check — confirm partitioning is active
# MAGIC DESCRIBE DETAIL bronze_patient;
# MAGIC -- Check partitionColumns = ["ingestion_date"]


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 5. Preview actual FHIR data landed correctly
# MAGIC SELECT resource_id, resource_data.name, resource_data.gender, resource_data.birthDate, ingestion_date
# MAGIC FROM bronze_patient
# MAGIC LIMIT 10;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SHOW TABLES IN dbo LIKE 'bronze_*';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Quick check cell — run this before re-running NB_02 for each resource
for rt in ["patient", "encounter", "observation", "condition"]:
    path = f"Files/raw/fhir/{rt}"
    try:
        contents = mssparkutils.fs.ls(path)
        print(f"{rt}: {len(contents)} ingestion_date folder(s)")
    except Exception as e:
        print(f"{rt}: EMPTY or missing — {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
