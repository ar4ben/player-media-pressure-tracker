output "generated_env" {
  description = "Environment values for .env."
  value       = <<EOT
GCP_PROJECT_ID=${var.project_id}
GDELT_GCP_BUCKET=${google_storage_bucket.lake.name}
GDELT_GCP_STG_BUCKET=${google_storage_bucket.dataproc_meta.name}
GDELT_DATAPROC_REGION=${var.region}
GDELT_DATAPROC_SERVICE_ACCOUNT=${google_service_account.pipeline_manager.email}
GOOGLE_APPLICATION_CREDENTIALS=./gcp_creds.json
EOT
}
