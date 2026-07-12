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

resource_types = ["Patient", "Encounter", "Observation", "Condition"]

results = []

for rt in resource_types:
    print(f"\n{'='*60}\nProcessing: {rt}\n{'='*60}")

    try:

        raw_result = mssparkutils.notebook.run(
            "NB_01_FHIR_API_Ingestion",
            timeout_seconds=300,
            arguments={"resource_type": rt}
        )
        print(f"NB_01 completed for {rt}: {raw_result}")

        bronze_result = mssparkutils.notebook.run(
            "NB_02_Bronze_Load",
            timeout_seconds=300,
            arguments={"resource_type": rt}
        )
        print(f"NB_02 completed for {rt}: {bronze_result}")

        results.append({"resource_type": rt, "status": "SUCCESS"})

    except Exception as e:
        print(f"FAILED for {rt}: {e}")
        results.append({"resource_type": rt, "status": "FAILED", "error": str(e)})
        
        continue

print("\n\n=== SUMMARY ===")
for r in results:
    print(r)

# METADATA ********************

# META {
# META   "language": "python",
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

print("=== TABLES CHECK ===")
tables = spark.sql("SHOW TABLES").collect()
bronze_tables = [t.tableName for t in tables if t.tableName.startswith("bronze_")]
print(f"Bronze tables found: {bronze_tables}")

print("\n=== ROW COUNTS ===")
for t in bronze_tables:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {t}").collect()[0]["cnt"]
    print(f"{t}: {count} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
