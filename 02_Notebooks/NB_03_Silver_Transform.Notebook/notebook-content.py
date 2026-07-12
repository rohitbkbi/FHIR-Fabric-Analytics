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
    when,
    trim,
    upper,
    row_number,
    sha2,
    concat_ws,
    coalesce,
    to_date,
    size
)
from pyspark.sql.window import Window
from datetime import datetime, timezone

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")


resource_type = "Condition"
ingestion_date = None

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

from pyspark.sql.functions import regexp_extract

bronze_table = f"bronze_{resource_type.lower()}"
bronze_df = spark.table(bronze_table)

if ingestion_date:
    bronze_df = bronze_df.filter(col("ingestion_date") == ingestion_date)

log_buffer.append(log_event("INFO", f"Read {bronze_df.count()} rows from {bronze_table}"))


def flatten_patient(df):
    return df.select(
        col("resource_id").alias("patient_id"),
        col("resource_data.gender").alias("gender"),
        to_date(col("resource_data.birthDate")).alias("birth_date"),
        col("resource_data.name")[0]["family"].alias("family_name"),
        col("resource_data.name")[0]["given"][0].alias("given_name"),
        col("resource_data.address")[0]["city"].alias("city"),
        col("resource_data.address")[0]["state"].alias("state"),
        col("resource_data.address")[0]["postalCode"].alias("postal_code"),
        col("resource_data.address")[0]["country"].alias("country"),
        col("resource_data.maritalStatus.text").alias("marital_status"),
        coalesce(col("resource_data.active"), lit(True)).alias("active_flag"),
        col("batch_id"), col("extraction_timestamp"), col("load_timestamp"),
        col("source_system"), col("ingestion_date")
    )

def flatten_encounter(df):
    return df.select(
        col("resource_id").alias("encounter_id"),
        col("resource_data.status").alias("status"),
        col("resource_data.class.code").alias("encounter_class"),
        col("resource_data.type")[0]["text"].alias("encounter_type"),
        # subject reference format is "Patient/123" — extract the id
        regexp_extract(col("resource_data.subject.reference"), r"Patient/(.+)", 1).alias("patient_id"),
        to_date(col("resource_data.period.start")).alias("period_start"),
        to_date(col("resource_data.period.end")).alias("period_end"),
        col("resource_data.serviceProvider.display").alias("service_provider"),
        col("batch_id"), col("extraction_timestamp"), col("load_timestamp"),
        col("source_system"), col("ingestion_date")
    )

def flatten_observation(df):
    return df.select(
        col("resource_id").alias("observation_id"),
        col("resource_data.status").alias("status"),
        col("resource_data.code.text").alias("observation_code_text"),
        col("resource_data.code.coding")[0]["code"].alias("observation_code"),
        col("resource_data.code.coding")[0]["system"].alias("coding_system"),
        regexp_extract(col("resource_data.subject.reference"), r"Patient/(.+)", 1).alias("patient_id"),
        regexp_extract(col("resource_data.encounter.reference"), r"Encounter/(.+)", 1).alias("encounter_id"),
        col("resource_data.valueQuantity.value").alias("value_numeric"),
        col("resource_data.valueQuantity.unit").alias("value_unit"),
        col("resource_data.valueString").alias("value_string"),
        to_date(col("resource_data.effectiveDateTime")).alias("effective_date"),
        col("batch_id"), col("extraction_timestamp"), col("load_timestamp"),
        col("source_system"), col("ingestion_date")
    )

def flatten_condition(df):
    return df.select(
        col("resource_id").alias("condition_id"),
        col("resource_data.clinicalStatus.coding")[0]["code"].alias("clinical_status"),
        col("resource_data.verificationStatus.coding")[0]["code"].alias("verification_status"),
        col("resource_data.code.text").alias("condition_text"),
        col("resource_data.code.coding")[0]["code"].alias("condition_code"),
        regexp_extract(col("resource_data.subject.reference"), r"Patient/(.+)", 1).alias("patient_id"),
        regexp_extract(col("resource_data.encounter.reference"), r"Encounter/(.+)", 1).alias("encounter_id"),
        to_date(col("resource_data.onsetDateTime")).alias("onset_date"),
        to_date(col("resource_data.recordedDate")).alias("recorded_date"),
        col("batch_id"), col("extraction_timestamp"), col("load_timestamp"),
        col("source_system"), col("ingestion_date")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


flatten_dispatch = {
    "patient": flatten_patient,
    "encounter": flatten_encounter,
    "observation": flatten_observation,
    "condition": flatten_condition
}

flatten_fn = flatten_dispatch[resource_type.lower()]
flat_df = flatten_fn(bronze_df)

log_buffer.append(log_event("INFO", f"Flattened to {flat_df.count()} rows, {len(flat_df.columns)} columns"))
flat_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

primary_key_col = f"{resource_type.lower()}_id"

total_rows = flat_df.count()
null_pk_count = flat_df.filter(col(primary_key_col).isNull()).count()

log_buffer.append(log_event(
    "INFO", "Pre-cleansing validation",
    resource_type=resource_type,
    total_rows=total_rows,
    null_primary_key_count=null_pk_count,
    null_pk_pct=round(null_pk_count / total_rows * 100, 2) if total_rows > 0 else 0
))


quarantine_df = flat_df.filter(col(primary_key_col).isNull())
clean_df = flat_df.filter(col(primary_key_col).isNotNull())

if quarantine_df.count() > 0:
    quarantine_path = f"Files/quarantine/{resource_type.lower()}/ingestion_date={datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
    quarantine_df.write.mode("append").json(quarantine_path)
    log_buffer.append(log_event(
        "WARN", f"Quarantined {quarantine_df.count()} rows with null primary key",
        path=quarantine_path
    ))

from pyspark.sql.functions import year

date_cols = [f.name for f in clean_df.schema.fields if str(f.dataType) == "DateType()"]

for dc in date_cols:
    implausible = clean_df.filter(
        (col(dc).isNotNull()) &
        ((year(col(dc)) < 1900) | (year(col(dc)) > 2026))
    )
    bad_count = implausible.count()
    if bad_count > 0:
        log_buffer.append(log_event(
            "WARN", f"Implausible dates found in column '{dc}'",
            resource_type=resource_type, column=dc, bad_row_count=bad_count
        ))
        clean_df = clean_df.withColumn(
            dc,
            when((year(col(dc)) < 1900) | (year(col(dc)) > 2026), None).otherwise(col(dc))
        )


string_cols = [f.name for f in clean_df.schema.fields if str(f.dataType) == "StringType()"]
for c in string_cols:
    clean_df = clean_df.withColumn(c, trim(col(c)))


status_like_cols = [c for c in clean_df.columns if "status" in c.lower() or c == "gender"]
for c in status_like_cols:
    if c in clean_df.columns:
        clean_df = clean_df.withColumn(c, upper(col(c)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_spec = Window.partitionBy(primary_key_col).orderBy(col("extraction_timestamp").desc())

deduped_df = (clean_df
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num"))

duplicates_removed = clean_df.count() - deduped_df.count()

log_buffer.append(log_event(
    "INFO", "Deduplication complete",
    resource_type=resource_type,
    rows_before=clean_df.count(),
    rows_after=deduped_df.count(),
    duplicates_removed=duplicates_removed
))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = (deduped_df
    .withColumn("silver_load_timestamp", current_timestamp())
    .withColumn("record_hash", sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) for c in deduped_df.columns if c not in ("batch_id", "extraction_timestamp", "load_timestamp", "ingestion_date")]), 256))
)

table_name = f"silver_{resource_type.lower()}"

(silver_df.write
    .format("delta")
    .option("mergeSchema", "true")
    .mode("overwrite")
    .saveAsTable(table_name))

log_buffer.append(log_event(
    "INFO", f"Silver write complete: {table_name}",
    row_count=silver_df.count()
))

print(f"✓ {table_name} created/updated with {silver_df.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT 'silver_patient' AS tbl, COUNT(*) AS cnt FROM silver_patient
# MAGIC UNION ALL
# MAGIC SELECT 'silver_encounter', COUNT(*) FROM silver_encounter
# MAGIC UNION ALL
# MAGIC SELECT 'silver_observation', COUNT(*) FROM silver_observation
# MAGIC UNION ALL
# MAGIC SELECT 'silver_condition', COUNT(*) FROM silver_condition;
# MAGIC 
# MAGIC SELECT patient_id, COUNT(*) AS cnt
# MAGIC FROM silver_patient
# MAGIC GROUP BY patient_id
# MAGIC HAVING COUNT(*) > 1;
# MAGIC 
# MAGIC SELECT
# MAGIC     patient_id,
# MAGIC     gender,
# MAGIC     birth_date,
# MAGIC     family_name,
# MAGIC     given_name,
# MAGIC     city,
# MAGIC     state
# MAGIC FROM silver_patient
# MAGIC LIMIT 10;
# MAGIC 
# MAGIC SELECT COUNT(*) AS orphaned_encounters
# MAGIC FROM silver_encounter e
# MAGIC LEFT ANTI JOIN silver_patient p
# MAGIC ON e.patient_id = p.patient_id;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
