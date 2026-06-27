
resource "google_storage_bucket" "quant_pipeline_raw" {
  name     = "quant-pipeline-raw"
  project  = var.project_id
  location = var.region

  force_destroy               = false
  uniform_bucket_level_access = true

  versioning {
    enabled = false
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    project = "quant-market-indicators-pipeline"
    layer   = "raw"
  }
}