resource "google_service_account" "airflow_pipeline_sa" {
  account_id   = "quant-pipeline-airflow"
  display_name = "Quant Pipeline Airflow Service Account"
  description  = "Used by self-hosted Airflow to run ingest.py (GCS writes) and bq_loader.py (BigQuery loads)."
  project      = var.project_id
}

resource "google_storage_bucket_iam_member" "airflow_sa_storage_access" {
  bucket = google_storage_bucket.quant_pipeline_raw.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.airflow_pipeline_sa.email}"
}

resource "google_bigquery_dataset_iam_member" "airflow_sa_bq_data_editor" {
  dataset_id = google_bigquery_dataset.quant_pipeline_raw.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.airflow_pipeline_sa.email}"
}

resource "google_project_iam_member" "airflow_sa_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.airflow_pipeline_sa.email}"
}