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

import requests
import json
import uuid
import time
from datetime import datetime, timezone
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

fhir_base_url = "https://hapi.fhir.org/baseR4"
page_size = 50
lookback_days = 3
max_retries = 3
retry_backoff_seconds = 5

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def log_event(level, message, **context):
    timestamp = datetime.now(timezone.utc).isoformat()
    record = {
        "timestamp": timestamp,
        "level": level,
        "message": message,
        **context
    }
    print(f"[{timestamp}] [{level}] {message} | {context}")
    return record

log_buffer = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_fhir_resource(resource_type, base_url, page_size, lookback_days,
                         max_retries, backoff_seconds):
    """
    Calls the FHIR search endpoint for a given resource type,
    follows pagination links, and returns a list of raw JSON
    Bundle pages (each page = one API response, unmodified).
    """
    from datetime import timedelta

    since_date = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    url = f"{base_url}/{resource_type}"
    params = {
        "_lastUpdated": f"ge{since_date}",
        "_count": page_size
    }

    pages = []
    page_num = 0
    next_url = url
    next_params = params

    while next_url:
        page_num += 1
        attempt = 0
        response = None

        while attempt < max_retries:
            try:
                response = requests.get(
                    next_url,
                    params=next_params,
                    headers={"Accept": "application/fhir+json"},
                    timeout=30
                )
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                attempt += 1
                log_buffer.append(log_event(
                    "WARN",
                    f"Request failed (attempt {attempt}/{max_retries})",
                    resource_type=resource_type, page=page_num, error=str(e)
                ))
                if attempt >= max_retries:
                    log_buffer.append(log_event(
                        "ERROR",
                        f"Max retries exceeded for page {page_num}",
                        resource_type=resource_type, error=str(e)
                    ))
                    raise
                time.sleep(backoff_seconds * (2 ** (attempt - 1)))

        bundle = response.json()
        pages.append(bundle)

        log_buffer.append(log_event(
            "INFO", f"Fetched page {page_num}",
            resource_type=resource_type,
            record_count=len(bundle.get("entry", []))
        ))

        next_link = next(
            (link["url"] for link in bundle.get("link", []) if link.get("relation") == "next"),
            None
        )

        if next_link:
            next_url = next_link
            next_params = None
        else:
            next_url = None

    return pages

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_raw_batch(pages, resource_type):
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    batch_id = str(uuid.uuid4())[:8]
    extraction_timestamp = datetime.now(timezone.utc).isoformat()

    base_path = f"Files/raw/fhir/{resource_type.lower()}/ingestion_date={ingestion_date}/batch_id={batch_id}"
    mssparkutils.fs.mkdirs(base_path)

    total_records = 0
    file_names = []

    for i, page in enumerate(pages, start=1):
        file_name = f"page_{i:04d}.json"
        file_path = f"{base_path}/{file_name}"

        wrapped = {
            "_extraction_metadata": {
                "extraction_timestamp": extraction_timestamp,
                "api_url": fhir_base_url,
                "resource_type": resource_type,
                "batch_id": batch_id,
                "page_number": i
            },
            "fhir_bundle": page
        }

        mssparkutils.fs.put(file_path, json.dumps(wrapped), overwrite=True)
        file_names.append(file_name)
        total_records += len(page.get("entry", []))

    manifest = {
        "batch_id": batch_id,
        "resource_type": resource_type,
        "ingestion_date": ingestion_date,
        "extraction_timestamp": extraction_timestamp,
        "page_count": len(pages),
        "total_records": total_records,
        "files": file_names
    }

    mssparkutils.fs.put(
        f"{base_path}/_manifest.json",
        json.dumps(manifest),
        overwrite=True
    )

    log_buffer.append(log_event(
        "INFO",
        "Batch write complete",
        resource_type=resource_type,
        batch_id=batch_id,
        pages=len(pages),
        total_records=total_records,
        path=base_path
    ))

    return manifest

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

log_buffer.append(log_event("INFO", "Starting ingestion", resource_type=resource_type))

try:
    pages = fetch_fhir_resource(
        resource_type=resource_type,
        base_url=fhir_base_url,
        page_size=page_size,
        lookback_days=lookback_days,
        max_retries=max_retries,
        backoff_seconds=retry_backoff_seconds
    )

    manifest = write_raw_batch(pages, resource_type)

    log_buffer.append(log_event(
        "INFO",
        "Ingestion completed successfully",
        resource_type=resource_type,
        batch_id=manifest["batch_id"]
    ))

except Exception as e:
    log_buffer.append(log_event(
        "ERROR",
        "Ingestion failed",
        resource_type=resource_type,
        error=str(e)
    ))
    raise

print(json.dumps(manifest, indent=2))

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
