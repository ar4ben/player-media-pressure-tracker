variable "project_id" {
  description = "GCP project where the GDELT backfill infrastructure will be created."
  type        = string
}

variable "region" {
  description = "Default Dataproc Serverless region."
  type        = string
  default     = "europe-west1"
}
