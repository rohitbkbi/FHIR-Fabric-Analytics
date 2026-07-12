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

# from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType

# log_schema = StructType([
#     StructField("run_id", StringType(), False),
#     StructField("pipeline_name", StringType(), False),
#     StructField("activity_name", StringType(), True),
#     StructField("resource_type", StringType(), True),
#     StructField("stage", StringType(), True),          # raw | bronze | silver | scd2 | gold
#     StructField("status", StringType(), False),         # SUCCESS | FAILED | PARTIAL_SUCCESS
#     StructField("start_time", TimestampType(), True),
#     StructField("end_time", TimestampType(), True),
#     StructField("duration_seconds", LongType(), True),
#     StructField("records_processed", LongType(), True),
#     StructField("error_message", StringType(), True),
#     StructField("log_timestamp", TimestampType(), False)
# ])

# empty_df = spark.createDataFrame([], log_schema)
# empty_df.write.format("delta").mode("overwrite").saveAsTable("meta_pipeline_execution_log")

# print("✓ meta_pipeline_execution_log created")
# spark.sql("DESCRIBE TABLE meta_pipeline_execution_log").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime, timezone
import uuid

spark = SparkSession.builder.getOrCreate()

pipeline_run_id = "manual_test"
pipeline_name = "PL_FHIR_ETL_PIPELINE"
overall_status = "SUCCESS"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql.utils import AnalysisException

def safe_count(table_name):
    try:
        return spark.table(table_name).count()
    except AnalysisException:
        return None

counts = {
    "gold_dim_patient": safe_count("gold_dim_patient"),
    "gold_dim_condition": safe_count("gold_dim_condition"),
    "fact_encounter": safe_count("fact_encounter"),
    "fact_observation": safe_count("fact_observation")
}

total_records = sum(v for v in counts.values() if v is not None)

print(f"Run ID: {pipeline_run_id}")
print(f"Status: {overall_status}")
print(f"Counts: {counts}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

log_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("activity_name", StringType(), True),
    StructField("resource_type", StringType(), True),
    StructField("stage", StringType(), True),
    StructField("status", StringType(), False),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("duration_seconds", LongType(), True),
    StructField("records_processed", LongType(), True),
    StructField("error_message", StringType(), True),
    StructField("log_timestamp", TimestampType(), False)
])

log_record = [{
    "run_id": pipeline_run_id,
    "pipeline_name": pipeline_name,
    "activity_name": "FULL_PIPELINE",
    "resource_type": "ALL",
    "stage": "gold",
    "status": overall_status,
    "start_time": None,
    "end_time": datetime.now(timezone.utc),
    "duration_seconds": None,
    "records_processed": total_records,
    "error_message": None,
    "log_timestamp": datetime.now(timezone.utc)
}]

log_df = spark.createDataFrame(log_record, schema=log_schema)
log_df.write.format("delta").mode("append").saveAsTable("meta_pipeline_execution_log")

print(f"✓ Logged run {pipeline_run_id} — {total_records} total records across Gold tables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
