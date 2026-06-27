resource "google_bigquery_dataset" "quant_pipeline_raw" {
  dataset_id  = "quant_pipeline_raw"
  project     = var.project_id
  location    = var.region
  description = "Raw OHLCV market data loaded from GCS NDJSON, ticker-partitioned."

  labels = {
    project = "quant-market-indicators-pipeline"
    layer   = "raw"
  }
}

resource "google_bigquery_table" "ohlcv_raw" {
  dataset_id = google_bigquery_dataset.quant_pipeline_raw.dataset_id
  table_id   = "ohlcv_raw"
  project    = var.project_id

  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "trading_date"
  }

  clustering = ["symbol", "sector"]

  schema = jsonencode([
    {
      name = "symbol"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "trading_date"
      type = "DATE"
      mode = "REQUIRED"
    },
    {
      name = "open"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "high"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "low"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "close"
      type = "FLOAT"
      mode = "NULLABLE"
    },
    {
      name = "volume"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "sector"
      type = "STRING"
      mode = "REQUIRED"
    }
  ])

  labels = {
    project = "quant-market-indicators-pipeline"
    layer   = "raw"
  }
}