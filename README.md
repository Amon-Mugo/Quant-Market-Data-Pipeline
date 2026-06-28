# Quant Market Indicators Pipeline

A GCP data engineering pipeline that ingests daily OHLCV market data for a
15-ticker universe (technology, financials, energy, healthcare, consumer,
industrials, and benchmark ETFs), loads it into BigQuery, and orchestrates
the whole flow with self-hosted Airflow.

## Architecture

Twelve Data API
      |
      v
ingestion/ingest.py  --(NDJSON)-->  GCS (quant-pipeline-raw)
      |
      v
ingestion/bq_loader.py  -->  BigQuery (quant_pipeline_raw.ohlcv_raw)
      |
      v
   dbt (Week 3, not yet built)

Orchestrated by self-hosted Apache Airflow (docker-compose, LocalExecutor) —
not Cloud Composer, not Cloud Functions/Cloud Scheduler. This was a
deliberate choice: a lighter Terraform footprint, and Airflow's own
scheduler replaces the need for Cloud Scheduler entirely.

## Stack

- Data source: Twelve Data API (800 calls/day free tier)
- Storage: Google Cloud Storage (raw NDJSON, partitioned by ticker)
- Warehouse: BigQuery (quant_pipeline_raw.ohlcv_raw, partitioned by
  trading_date, clustered on symbol + sector)
- Orchestration: Apache Airflow 2.10.3, self-hosted via docker-compose,
  LocalExecutor
- IaC: Terraform (terraform/main.tf, variables.tf, storage.tf, bigquery.tf, iam.tf)
- Auth: Application Default Credentials (see note below — GCP key
  creation is org-blocked on this project)

## Project status

| Week | Scope | Status |
|------|-------|--------|
| 1 | Ingestion + raw layer (GCS, BigQuery, Terraform) | Complete |
| 2 | Orchestration (self-hosted Airflow) | Complete |
| 3 | dbt transformation layer (Core 4 indicators) | Next |

15 tickers, ~18,800 rows, 5 years of daily history backfilled and
incrementally maintained as of this writing.

## Running locally

### Prerequisites

- Docker, with network_mode: host (this project's Pop!_OS dev environment
  has broken bridge networking — br_netfilter not loaded, docker0
  shows NO-CARRIER. Every service in docker-compose.yaml uses
  network_mode: host to work around this. Irrelevant on a normal Docker
  setup, but the compose file is written this way regardless.)
- A GCP project with a BigQuery dataset and GCS bucket already provisioned
  via terraform/ (see that directory's own setup).
- A .env file at the project root with TWELVE_DATA_API_KEY=your_key_here.

### GCP authentication note

This project's GCP org has constraints/iam.disableServiceAccountKeyCreation
enforced, which blocks creating a downloadable JSON key for the
quant-pipeline-airflow service account. Local development therefore uses
Application Default Credentials (ADC) under the developer's own user
identity instead of a service account key:

gcloud auth application-default login
cp ~/.config/gcloud/application_default_credentials.json airflow/secrets/adc.json

This runs Airflow's GCP calls as the developer's user, not the
least-privilege-scoped service account defined in terraform/iam.tf. A
production deployment would use Workload Identity Federation instead to
exercise that scoped identity properly.

### Start Airflow

docker compose build
docker compose up -d

Airflow UI: http://localhost:8080 (default login admin/admin, set via .env).

The DAG quant_pipeline_ohlcv runs the ingestion script (ingest_and_load
task, which itself handles fetch, GCS upload, and BigQuery load in one
pass) followed by a dbt_run task (placeholder until Week 3). It's
scheduled @daily and defaults to mode=incremental; trigger manually with
{"mode": "backfill"} in the run config to re-seed a ticker's full history
(safe to rerun — backfill mode skips any ticker that already has rows).

## Repo layout

config/tickers.yaml          15-ticker universe with sector tags
ingestion/
  ingest.py                  fetch -> GCS -> BigQuery, per ticker
  gcs_loader.py               GCS upload helpers (NDJSON, retry-with-backoff)
  bq_loader.py                BigQuery load helpers (incremental + backfill)
terraform/                   GCS bucket, BQ dataset/table, least-privilege IAM
airflow/
  dags/quant_pipeline_ohlcv.py
  Dockerfile                 base Airflow image + ingestion/'s requirements.txt
  secrets/                   adc.json (gitignored)
docker-compose.yaml          Airflow services: postgres, webserver, scheduler
