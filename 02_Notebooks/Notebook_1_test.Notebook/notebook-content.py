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

# ------------------------------------------------------------
# Diagnostic: check what actually landed on disk and in tables
# ------------------------------------------------------------
print("=== FILES CHECK ===")
for rt in ["patient", "encounter", "observation", "condition"]:
    path = f"Files/raw/fhir/{rt}"
    try:
        contents = mssparkutils.fs.ls(path)
        print(f"{rt}: {len(contents)} item(s) found")
        for c in contents:
            print(f"   - {c.name}")
    except Exception as e:
        print(f"{rt}: NOTHING FOUND — {e}")

print("\n=== TABLES CHECK ===")
tables = spark.sql("SHOW TABLES").collect()
bronze_tables = [t.tableName for t in tables if t.tableName.startswith("bronze_")]
print(f"Bronze tables found: {bronze_tables}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

url = "https://hapi.fhir.org/baseR4/Observation"
params = {"_count": 5}

response = requests.get(url, params=params, headers={"Accept": "application/fhir+json"}, timeout=30)

print("Status code:", response.status_code)
print("URL called:", response.url)
bundle = response.json()
print("Total entries returned:", len(bundle.get("entry", [])))
print("Bundle type:", bundle.get("resourceType"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_path = "Files/debug_test/hello.json"
mssparkutils.fs.mkdirs("Files/debug_test")
mssparkutils.fs.put(test_path, '{"test": "hello"}', overwrite=True)

# Now read it back
content = mssparkutils.fs.head(test_path, 1000)
print("Read back:", content)

# List it
print(mssparkutils.fs.ls("Files/debug_test"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 1. All bronze/silver tables populated
# MAGIC SELECT 'bronze_patient' AS tbl, COUNT(*) AS cnt FROM bronze_patient
# MAGIC UNION ALL SELECT 'bronze_encounter', COUNT(*) FROM bronze_encounter
# MAGIC UNION ALL SELECT 'bronze_observation', COUNT(*) FROM bronze_observation
# MAGIC UNION ALL SELECT 'bronze_condition', COUNT(*) FROM bronze_condition
# MAGIC UNION ALL SELECT 'silver_patient', COUNT(*) FROM silver_patient
# MAGIC UNION ALL SELECT 'silver_encounter', COUNT(*) FROM silver_encounter
# MAGIC UNION ALL SELECT 'silver_observation', COUNT(*) FROM silver_observation
# MAGIC UNION ALL SELECT 'silver_condition', COUNT(*) FROM silver_condition;
# MAGIC 
# MAGIC -- 2. SCD2 tables current after automated run
# MAGIC SELECT
# MAGIC     (SELECT COUNT(*) FROM dim_patient WHERE scd_current_flag = true) AS current_patients,
# MAGIC     (SELECT COUNT(*) FROM dim_condition WHERE scd_current_flag = true) AS current_conditions;
# MAGIC 
# MAGIC -- 3. Gold star schema populated from THIS pipeline run (not stale from earlier manual runs)
# MAGIC SELECT 'gold_dim_patient' AS tbl, COUNT(*) AS cnt FROM gold_dim_patient
# MAGIC UNION ALL SELECT 'gold_dim_condition', COUNT(*) FROM gold_dim_condition
# MAGIC UNION ALL SELECT 'fact_encounter', COUNT(*) FROM fact_encounter
# MAGIC UNION ALL SELECT 'fact_observation', COUNT(*) FROM fact_observation;
# MAGIC 
# MAGIC -- 4. Confirm fact tables actually joined correctly (not all nulls from a broken run)
# MAGIC SELECT COUNT(*) AS total, SUM(CASE WHEN patient_sk IS NULL THEN 1 ELSE 0 END) AS orphaned
# MAGIC FROM fact_encounter;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 'bronze_patient' AS tbl, COUNT(*) AS cnt FROM bronze_patient
# MAGIC UNION ALL SELECT 'bronze_encounter', COUNT(*) FROM bronze_encounter
# MAGIC UNION ALL SELECT 'bronze_observation', COUNT(*) FROM bronze_observation
# MAGIC UNION ALL SELECT 'bronze_condition', COUNT(*) FROM bronze_condition
# MAGIC UNION ALL SELECT 'gold_dim_patient', COUNT(*) FROM gold_dim_patient
# MAGIC UNION ALL SELECT 'gold_dim_condition', COUNT(*) FROM gold_dim_condition
# MAGIC UNION ALL SELECT 'fact_encounter', COUNT(*) FROM fact_encounter
# MAGIC UNION ALL SELECT 'fact_observation', COUNT(*) FROM fact_observation;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- If incremental load is working, you should see MULTIPLE distinct
# MAGIC -- ingestion_date values accumulating over time (one per day you've run it)
# MAGIC SELECT ingestion_date, COUNT(*) AS row_count, COUNT(DISTINCT batch_id) AS batch_count
# MAGIC FROM bronze_patient
# MAGIC GROUP BY ingestion_date
# MAGIC ORDER BY ingestion_date DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- A resource_id should NOT have multiple rows within the SAME batch_id
# MAGIC SELECT resource_id, batch_id, COUNT(*) AS cnt
# MAGIC FROM bronze_patient
# MAGIC GROUP BY resource_id, batch_id
# MAGIC HAVING COUNT(*) > 1;
# MAGIC -- Expect: 0 rows
# MAGIC 
# MAGIC -- A resource_id CAN legitimately appear across DIFFERENT batch_ids
# MAGIC -- (that's expected — it means the same patient was re-extracted on
# MAGIC -- multiple days, which is normal with a 3-day lookback window)
# MAGIC SELECT resource_id, COUNT(DISTINCT batch_id) AS times_reingested
# MAGIC FROM bronze_patient
# MAGIC GROUP BY resource_id
# MAGIC HAVING COUNT(DISTINCT batch_id) > 1
# MAGIC ORDER BY times_reingested DESC
# MAGIC LIMIT 20;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Silver should have exactly ONE row per patient_id, even though
# MAGIC -- bronze may have multiple (from overlapping re-ingestion)
# MAGIC SELECT patient_id, COUNT(*) AS cnt
# MAGIC FROM silver_patient
# MAGIC GROUP BY patient_id
# MAGIC HAVING COUNT(*) > 1;
# MAGIC -- Expect: 0 rows — this proves the deduplication window (Step 5.6) works
# MAGIC 
# MAGIC -- Compare bronze row count vs silver row count for the same resource —
# MAGIC -- silver should be <= bronze (since bronze may have re-ingested duplicates
# MAGIC -- across batches, but silver dedupes to one row per patient_id)
# MAGIC SELECT
# MAGIC     (SELECT COUNT(*) FROM bronze_patient) AS bronze_total_rows,
# MAGIC     (SELECT COUNT(DISTINCT resource_id) FROM bronze_patient) AS bronze_distinct_patients,
# MAGIC     (SELECT COUNT(*) FROM silver_patient) AS silver_total_rows;
# MAGIC -- silver_total_rows should equal bronze_distinct_patients

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Count how many versions each patient has in the SCD2 table.
# MAGIC -- Most patients should have EXACTLY 1 version (nothing changed since
# MAGIC -- they were first loaded). Only patients whose data genuinely changed
# MAGIC -- between runs should have 2+.
# MAGIC SELECT version_count, COUNT(*) AS num_patients
# MAGIC FROM (
# MAGIC     SELECT patient_id, COUNT(*) AS version_count
# MAGIC     FROM dim_patient
# MAGIC     GROUP BY patient_id
# MAGIC ) t
# MAGIC GROUP BY version_count
# MAGIC ORDER BY version_count;
# MAGIC 
# MAGIC -- If you see mostly "1 version" and a few "2+ versions", SCD2 change
# MAGIC -- detection (record_hash comparison) is working correctly.
# MAGIC -- If EVERY patient has as many versions as pipeline runs you've done,
# MAGIC -- the hash comparison is broken and it's creating a new version every run.

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- For a specific patient with multiple versions, inspect what changed
# MAGIC SELECT patient_id, city, state, record_hash,
# MAGIC        scd_effective_start_date, scd_effective_end_date, scd_current_flag
# MAGIC FROM dim_patient
# MAGIC WHERE patient_id = (
# MAGIC     SELECT patient_id FROM dim_patient GROUP BY patient_id HAVING COUNT(*) > 1 LIMIT 1
# MAGIC )
# MAGIC ORDER BY scd_effective_start_date;
# MAGIC -- Manually eyeball: did city/state (or whatever field) actually change
# MAGIC -- between the two rows? If the hash differs but the visible data looks
# MAGIC -- identical, there's likely a technical column leaking into the hash
# MAGIC -- (check Step 5.7 — record_hash should exclude batch_id/timestamps)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC     (SELECT COUNT(DISTINCT ingestion_date) FROM bronze_patient) AS distinct_load_days,
# MAGIC     (SELECT COUNT(*) FROM bronze_patient) AS bronze_total_rows,
# MAGIC     (SELECT COUNT(*) FROM silver_patient) AS silver_current_rows,
# MAGIC     (SELECT COUNT(*) FROM dim_patient) AS dim_total_versions,
# MAGIC     (SELECT COUNT(*) FROM dim_patient WHERE scd_current_flag = true) AS dim_current_versions,
# MAGIC     (SELECT COUNT(*) FROM dim_patient WHERE scd_current_flag = false) AS dim_historical_versions;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
