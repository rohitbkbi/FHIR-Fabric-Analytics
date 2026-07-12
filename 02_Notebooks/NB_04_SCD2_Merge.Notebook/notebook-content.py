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

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, sha2, concat_ws, coalesce, when
)
from delta.tables import DeltaTable
from datetime import datetime, timezone

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def log_event(level, message, **context):
    timestamp = datetime.now(timezone.utc).isoformat()
    record = {"timestamp": timestamp, "level": level, "message": message, **context}
    print(f"[{timestamp}] [{level}] {message} | {context}")
    return record

log_buffer = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_table = f"silver_{resource_type.lower()}"
target_table = f"dim_{resource_type.lower()}"

source_df = spark.table(source_table)
pk_col = f"{resource_type.lower()}_id"

log_buffer.append(log_event("INFO", f"Read {source_df.count()} rows from {source_table}"))

technical_cols = {"batch_id", "extraction_timestamp", "load_timestamp",
                   "ingestion_date", "silver_load_timestamp", "record_hash"}

source_df = source_df.withColumn("scd_effective_start_date", current_timestamp().cast("date")) \
                      .withColumn("scd_effective_end_date", lit(None).cast("date")) \
                      .withColumn("scd_current_flag", lit(True))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_exists = spark.catalog.tableExists(target_table)

if not table_exists:
    (source_df.write
        .format("delta")
        .option("mergeSchema", "true")
        .mode("overwrite")
        .saveAsTable(target_table))

    log_buffer.append(log_event(
        "INFO", f"Initialized {target_table} with {source_df.count()} current records"
    ))
    print(f"✓ {target_table} created — SCD2 tracking starts now")
else:
    print(f"{target_table} already exists — proceeding to change detection + merge")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if table_exists:
    dim_table = DeltaTable.forName(spark, target_table)
    dim_current_df = dim_table.toDF().filter(col("scd_current_flag") == True)


    comparison_df = source_df.alias("src").join(
        dim_current_df.select(pk_col, "record_hash").alias("tgt"),
        on=pk_col, how="left"
    ).select(
        col("src.*"),
        col("tgt.record_hash").alias("existing_hash")
    )

    new_records = comparison_df.filter(col("existing_hash").isNull())
    changed_records = comparison_df.filter(
        (col("existing_hash").isNotNull()) & (col("existing_hash") != col("record_hash"))
    )
    unchanged_count = comparison_df.filter(
        (col("existing_hash").isNotNull()) & (col("existing_hash") == col("record_hash"))
    ).count()

    new_count = new_records.count()
    changed_count = changed_records.count()

    log_buffer.append(log_event(
        "INFO", "Change detection complete",
        resource_type=resource_type,
        new_records=new_count,
        changed_records=changed_count,
        unchanged_records=unchanged_count
    ))
    print(f"New: {new_count} | Changed: {changed_count} | Unchanged: {unchanged_count}")

 
    if changed_count > 0:
        changed_pks = [row[pk_col] for row in changed_records.select(pk_col).distinct().collect()]

        (dim_table.alias("target")
            .merge(
                spark.createDataFrame([(pk,) for pk in changed_pks], [pk_col]).alias("source"),
                f"target.{pk_col} = source.{pk_col} AND target.scd_current_flag = true"
            )
            .whenMatchedUpdate(set={
                "scd_current_flag": lit(False),
                "scd_effective_end_date": current_timestamp().cast("date")
            })
            .execute())

        log_buffer.append(log_event("INFO", f"Closed out {changed_count} old record versions"))

        (changed_records.drop("existing_hash").write
            .format("delta")
            .mode("append")
            .saveAsTable(target_table))

        log_buffer.append(log_event("INFO", f"Inserted {changed_count} new current versions"))

    if new_count > 0:
        (new_records.drop("existing_hash").write
            .format("delta")
            .mode("append")
            .saveAsTable(target_table))

        log_buffer.append(log_event("INFO", f"Inserted {new_count} brand-new records"))

    print(f"✓ SCD2 merge complete for {target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT patient_id, COUNT(*) AS current_count
# MAGIC FROM dim_patient
# MAGIC WHERE scd_current_flag = true
# MAGIC GROUP BY patient_id
# MAGIC HAVING COUNT(*) > 1;
# MAGIC -- Expect: 0 rows
# MAGIC 
# MAGIC 
# MAGIC SELECT patient_id, city, state, record_hash,
# MAGIC        scd_effective_start_date, scd_effective_end_date, scd_current_flag
# MAGIC FROM dim_patient
# MAGIC ORDER BY patient_id, scd_effective_start_date;
# MAGIC 
# MAGIC 
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     SUM(CASE WHEN scd_current_flag THEN 1 ELSE 0 END) AS current_rows,
# MAGIC     SUM(CASE WHEN NOT scd_current_flag THEN 1 ELSE 0 END) AS historical_rows
# MAGIC FROM dim_patient;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- 1. Confirm exactly one current record per condition_id
# MAGIC SELECT condition_id, COUNT(*) AS current_count
# MAGIC FROM dim_condition
# MAGIC WHERE scd_current_flag = true
# MAGIC GROUP BY condition_id
# MAGIC HAVING COUNT(*) > 1;
# MAGIC -- Expect: 0 rows
# MAGIC 
# MAGIC -- 2. View history for conditions (run again after a second NB_04
# MAGIC -- run once source data changes, to see 2+ rows per condition_id)
# MAGIC SELECT condition_id, patient_id, clinical_status, verification_status,
# MAGIC        condition_text, record_hash,
# MAGIC        scd_effective_start_date, scd_effective_end_date, scd_current_flag
# MAGIC FROM dim_condition
# MAGIC ORDER BY condition_id, scd_effective_start_date;
# MAGIC 
# MAGIC -- 3. Overall counts — current vs historical split
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     SUM(CASE WHEN scd_current_flag THEN 1 ELSE 0 END) AS current_rows,
# MAGIC     SUM(CASE WHEN NOT scd_current_flag THEN 1 ELSE 0 END) AS historical_rows
# MAGIC FROM dim_condition;
# MAGIC 
# MAGIC -- 4. Sanity check: every closed-out (non-current) record should
# MAGIC -- have a non-null effective_end_date
# MAGIC SELECT COUNT(*) AS bad_closed_records
# MAGIC FROM dim_condition
# MAGIC WHERE scd_current_flag = false AND scd_effective_end_date IS NULL;
# MAGIC -- Expect: 0 rows
# MAGIC 
# MAGIC -- 5. Sanity check: every current record should have a NULL end_date
# MAGIC SELECT COUNT(*) AS bad_current_records
# MAGIC FROM dim_condition
# MAGIC WHERE scd_current_flag = true AND scd_effective_end_date IS NOT NULL;
# MAGIC -- Expect: 0 rows
# MAGIC 
# MAGIC -- 6. Referential check: do dim_condition's patient_ids exist in dim_patient?
# MAGIC SELECT COUNT(*) AS orphaned_conditions
# MAGIC FROM dim_condition c
# MAGIC LEFT ANTI JOIN dim_patient p ON c.patient_id = p.patient_id
# MAGIC WHERE c.scd_current_flag = true;
# MAGIC -- Some orphans may exist due to HAPI's messy public test data — note the count, not necessarily zero

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
