provider "google" {
  project = var.project_id
  region  = var.region
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

locals {
  lake_bucket_name          = "${var.project_id}-gdelt-lake-${random_id.bucket_suffix.hex}"
  dataproc_meta_bucket_name = "${var.project_id}-dataproc-meta-${random_id.bucket_suffix.hex}"
  service_account_id        = "pipeline-manager"
  service_account_key_path  = "../gcp_creds.json"

  required_services = toset([
    "compute.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "dataproc.googleapis.com",
    "iam.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "serviceusage.googleapis.com",
    "storage.googleapis.com",
  ])
}

resource "google_project_service" "required" {
  for_each = local.required_services

  project = var.project_id
  service = each.value

  disable_on_destroy = false
}

resource "google_storage_bucket" "lake" {
  name     = local.lake_bucket_name
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false

  depends_on = [google_project_service.required]
}

resource "google_storage_bucket" "dataproc_meta" {
  name     = local.dataproc_meta_bucket_name
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false

  depends_on = [google_project_service.required]
}

resource "google_service_account" "pipeline_manager" {
  project      = var.project_id
  account_id   = local.service_account_id
  display_name = "Media Pressure pipeline manager"

  depends_on = [google_project_service.required]
}

resource "google_project_iam_member" "pipeline_project_roles" {
  for_each = toset([
    "roles/dataproc.editor",
    "roles/dataproc.worker",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
  ])

  project = var.project_id
  role    = each.value
  member  = google_service_account.pipeline_manager.member
}

resource "google_storage_bucket_iam_member" "pipeline_lake_object_admin" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectAdmin"
  member = google_service_account.pipeline_manager.member
}

resource "google_storage_bucket_iam_member" "pipeline_dataproc_meta_object_admin" {
  bucket = google_storage_bucket.dataproc_meta.name
  role   = "roles/storage.objectAdmin"
  member = google_service_account.pipeline_manager.member
}

resource "google_service_account_iam_member" "pipeline_self_act_as" {
  service_account_id = google_service_account.pipeline_manager.name
  role               = "roles/iam.serviceAccountUser"
  member             = google_service_account.pipeline_manager.member
}

resource "google_service_account_key" "pipeline_manager" {
  service_account_id = google_service_account.pipeline_manager.name
}

resource "local_sensitive_file" "pipeline_manager_key" {
  filename        = local.service_account_key_path
  content         = base64decode(google_service_account_key.pipeline_manager.private_key)
  file_permission = "0600"
}
