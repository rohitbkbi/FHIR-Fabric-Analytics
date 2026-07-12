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
    col, lit, current_timestamp, monotonically_increasing_id,
    row_number, sequence, explode, to_date, date_format,
    year, month, dayofmonth, dayofweek, quarter, weekofyear,
    when
)
from pyspark.sql.window import Window
from datetime import datetime, timezone, date

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

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

date_range_df = spark.sql("""
    SELECT explode(sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day)) AS full_date
""")

dim_date_df = (date_range_df
    .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("int"))
    .withColumn("year", year(col("full_date")))
    .withColumn("quarter", quarter(col("full_date")))
    .withColumn("month", month(col("full_date")))
    .withColumn("month_name", date_format(col("full_date"), "MMMM"))
    .withColumn("day", dayofmonth(col("full_date")))
    .withColumn("day_name", date_format(col("full_date"), "EEEE"))
    .withColumn("day_of_week", dayofweek(col("full_date")))
    .withColumn("week_of_year", weekofyear(col("full_date")))
    .withColumn("is_weekend", col("day_of_week").isin([1, 7]))
)

(dim_date_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("dim_date"))

log_buffer.append(log_event("INFO", f"DimDate created with {dim_date_df.count()} rows"))
print(f"✓ dim_date created — {dim_date_df.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import datediff, current_date, floor, when, col

scd_patient_df = spark.table("dim_patient").filter(col("scd_current_flag") == True)

window_spec = Window.orderBy("patient_id")
gold_dim_patient_df = (scd_patient_df
    .withColumn("patient_sk", row_number().over(window_spec))  # surrogate key
    .withColumn(
        "age",
        when(
            col("birth_date").isNotNull(),
            floor(datediff(current_date(), col("birth_date")) / 365.25)
        ).otherwise(None).cast("int")
    )
    .withColumn(
        "age_group",
        when(col("age").isNull(), "Unknown")
        .when(col("age") < 18, "0-17")
        .when(col("age") < 35, "18-34")
        .when(col("age") < 50, "35-49")
        .when(col("age") < 65, "50-64")
        .otherwise("65+")
    )
    .withColumn(
        "age_group_sort_order",
        when(col("age_group") == "0-17", 1)
        .when(col("age_group") == "18-34", 2)
        .when(col("age_group") == "35-49", 3)
        .when(col("age_group") == "50-64", 4)
        .when(col("age_group") == "65+", 5)
        .otherwise(99)   # "Unknown" sorts last, always
        .cast("int")
    )
    .select(
        "patient_sk",
        col("patient_id").alias("patient_business_key"),
        "gender", "birth_date", "age", "age_group", "age_group_sort_order",
        "family_name", "given_name",
        "city", "state", "postal_code", "country",
        "marital_status", "active_flag",
        "scd_effective_start_date", "scd_effective_end_date"
    )
)

(gold_dim_patient_df.write
    .format("delta")
    .option("overwriteSchema", "true")
    .mode("overwrite")
    .saveAsTable("gold_dim_patient"))

log_buffer.append(log_event("INFO", f"gold_dim_patient created with {gold_dim_patient_df.count()} rows, including age/age_group/sort_order"))
print(f"✓ gold_dim_patient created — {gold_dim_patient_df.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

scd_condition_df = spark.table("dim_condition").filter(col("scd_current_flag") == True)

window_spec = Window.orderBy("condition_id")
gold_dim_condition_df = (scd_condition_df
    .withColumn("condition_sk", row_number().over(window_spec))
    .select(
        "condition_sk",
        col("condition_id").alias("condition_business_key"),
        "clinical_status", "verification_status", "condition_text", "condition_code",
        col("patient_id").alias("patient_business_key"),
        "onset_date", "recorded_date",
        "scd_effective_start_date", "scd_effective_end_date"
    )
)

(gold_dim_condition_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold_dim_condition"))

log_buffer.append(log_event("INFO", f"gold_dim_condition created with {gold_dim_condition_df.count()} rows"))
print(f"✓ gold_dim_condition created — {gold_dim_condition_df.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ------------------------------------------------------------
# FactEncounter: one row per encounter event, joined to
# DimPatient and DimDate via surrogate keys
# ------------------------------------------------------------
silver_encounter_df = spark.table("silver_encounter")
gold_dim_patient_df = spark.table("gold_dim_patient")

fact_encounter_df = (silver_encounter_df
    .join(
        gold_dim_patient_df.select("patient_sk", "patient_business_key"),
        silver_encounter_df["patient_id"] == gold_dim_patient_df["patient_business_key"],
        "left"
    )
    .withColumn("start_date_key", date_format(col("period_start"), "yyyyMMdd").cast("int"))
    .withColumn("end_date_key", date_format(col("period_end"), "yyyyMMdd").cast("int"))
    .withColumn(
        "length_of_stay_days",
        when(col("period_end").isNotNull() & col("period_start").isNotNull(),
             col("period_end").cast("long") / 86400 - col("period_start").cast("long") / 86400)
        .otherwise(None)
    )
    .withColumn(
        "los_bucket",
        when(col("length_of_stay_days").isNull(), "Not Applicable")
        .when(col("length_of_stay_days") <= 1, "0 – 1 day")
        .when(col("length_of_stay_days") <= 3, "2 – 3 days")
        .when(col("length_of_stay_days") <= 7, "4 – 7 days")
        .when(col("length_of_stay_days") <= 14, "8 – 14 days")
        .when(col("length_of_stay_days") <= 30, "15 – 30 days")
        .otherwise("30+ days")
    )
    .withColumn(
        "los_bucket_sort_order",
        when(col("los_bucket") == "0 – 1 day", 1)
        .when(col("los_bucket") == "2 – 3 days", 2)
        .when(col("los_bucket") == "4 – 7 days", 3)
        .when(col("los_bucket") == "8 – 14 days", 4)
        .when(col("los_bucket") == "15 – 30 days", 5)
        .when(col("los_bucket") == "30+ days", 6)
        .otherwise(99)   # "Not Applicable" sorts last
        .cast("int")
    )
    .select(
        "encounter_id",
        "patient_sk",
        "start_date_key",
        "end_date_key",
        "status",
        "encounter_class",
        "encounter_type",
        "service_provider",
        "period_start",
        "period_end",
        "length_of_stay_days",
        "los_bucket",
        "los_bucket_sort_order"
    )
)

(fact_encounter_df.write
    .format("delta")
    .option("overwriteSchema", "true")   # required — adding 2 new columns
    .mode("overwrite")
    .saveAsTable("fact_encounter"))

log_buffer.append(log_event("INFO", f"fact_encounter created with {fact_encounter_df.count()} rows, including los_bucket"))
print(f"✓ fact_encounter created — {fact_encounter_df.count()} rows")

orphan_count = fact_encounter_df.filter(col("patient_sk").isNull()).count()
if orphan_count > 0:
    log_buffer.append(log_event("WARN", f"{orphan_count} encounters have no matching patient (orphaned reference)"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_observation_df = spark.table("silver_observation")

fact_observation_df = (silver_observation_df
    .join(
        gold_dim_patient_df.select("patient_sk", "patient_business_key"),
        silver_observation_df["patient_id"] == gold_dim_patient_df["patient_business_key"],
        "left"
    )
    .withColumn("observation_date_key", date_format(col("effective_date"), "yyyyMMdd").cast("int"))
    .select(
        "observation_id",
        "patient_sk",
        "encounter_id",
        "observation_date_key",
        "status",
        "observation_code_text",
        "observation_code",
        "coding_system",
        "value_numeric",
        "value_unit",
        "value_string",
        "effective_date"
    )
)

(fact_observation_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("fact_observation"))

log_buffer.append(log_event("INFO", f"fact_observation created with {fact_observation_df.count()} rows"))
print(f"✓ fact_observation created — {fact_observation_df.count()} rows")

orphan_count = fact_observation_df.filter(col("patient_sk").isNull()).count()
if orphan_count > 0:
    log_buffer.append(log_event("WARN", f"{orphan_count} observations have no matching patient"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- sql/views/vw_fact_encounter.sql
# MAGIC CREATE OR REPLACE VIEW vw_fact_encounter AS
# MAGIC SELECT
# MAGIC     fe.encounter_id,
# MAGIC     fe.status,
# MAGIC     fe.encounter_class,
# MAGIC     fe.encounter_type,
# MAGIC     fe.service_provider,
# MAGIC     fe.length_of_stay_days,
# MAGIC     dp.patient_business_key,
# MAGIC     dp.gender,
# MAGIC     dp.city AS patient_city,
# MAGIC     dp.state AS patient_state,
# MAGIC     dd.full_date AS encounter_start_date,
# MAGIC     dd.year,
# MAGIC     dd.month_name,
# MAGIC     dd.quarter
# MAGIC FROM fact_encounter fe
# MAGIC LEFT JOIN gold_dim_patient dp ON fe.patient_sk = dp.patient_sk
# MAGIC LEFT JOIN dim_date dd ON fe.start_date_key = dd.date_key

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE VIEW vw_fact_observation AS
# MAGIC SELECT
# MAGIC     fo.observation_id,
# MAGIC     fo.status,
# MAGIC     fo.observation_code_text,
# MAGIC     fo.value_numeric,
# MAGIC     fo.value_unit,
# MAGIC     dp.patient_business_key,
# MAGIC     dp.gender,
# MAGIC     dd.full_date AS observation_date,
# MAGIC     dd.year,
# MAGIC     dd.month_name
# MAGIC FROM fact_observation fo
# MAGIC LEFT JOIN gold_dim_patient dp ON fo.patient_sk = dp.patient_sk
# MAGIC LEFT JOIN dim_date dd ON fo.observation_date_key = dd.date_key

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE VIEW vw_condition_distribution AS
# MAGIC SELECT
# MAGIC     dc.condition_text,
# MAGIC     dc.clinical_status,
# MAGIC     dp.gender,
# MAGIC     COUNT(*) AS condition_count
# MAGIC FROM gold_dim_condition dc
# MAGIC LEFT JOIN gold_dim_patient dp ON dc.patient_business_key = dp.patient_business_key
# MAGIC GROUP BY dc.condition_text, dc.clinical_status, dp.gender

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Row counts across the whole star schema
# MAGIC SELECT 'dim_date' AS tbl, COUNT(*) AS cnt FROM dim_date
# MAGIC UNION ALL SELECT 'gold_dim_patient', COUNT(*) FROM gold_dim_patient
# MAGIC UNION ALL SELECT 'gold_dim_condition', COUNT(*) FROM gold_dim_condition
# MAGIC UNION ALL SELECT 'fact_encounter', COUNT(*) FROM fact_encounter
# MAGIC UNION ALL SELECT 'fact_observation', COUNT(*) FROM fact_observation;
# MAGIC 
# MAGIC -- Confirm no duplicate surrogate keys
# MAGIC SELECT patient_sk, COUNT(*) FROM gold_dim_patient GROUP BY patient_sk HAVING COUNT(*) > 1;
# MAGIC -- Expect: 0 rows
# MAGIC 
# MAGIC -- Confirm fact-to-dim join integrity
# MAGIC SELECT COUNT(*) AS unmatched_encounters
# MAGIC FROM fact_encounter WHERE patient_sk IS NULL;
# MAGIC 
# MAGIC -- Preview the final reporting view
# MAGIC SELECT * FROM vw_fact_encounter LIMIT 10;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM vw_fact_encounter LIMIT 5;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM vw_fact_observation LIMIT 5;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row

config_data = [
    Row(resource_type="Patient", active_flag=True, load_order=1),
    Row(resource_type="Encounter", active_flag=True, load_order=2),
    Row(resource_type="Observation", active_flag=True, load_order=3),
    Row(resource_type="Condition", active_flag=True, load_order=4),
]

config_df = spark.createDataFrame(config_data)
config_df.write.format("delta").mode("overwrite").saveAsTable("meta_resource_config")

spark.sql("SELECT * FROM meta_resource_config ORDER BY load_order").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
