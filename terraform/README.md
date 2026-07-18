# GCP Infrastructure

Before the pipeline can run in `gcp` mode, this Terraform config will set up:

- required Google APIs, so Storage, IAM, and Dataproc Serverless can be used;
- GCS lake bucket (`GDELT_GCP_BUCKET`) for GDELT input files, parquet outputs,
  and pipeline logs;
- GCS Dataproc staging bucket (`GDELT_GCP_STG_BUCKET`) for Dataproc Serverless
  staging and dependencies;
- pipeline service account used by Docker/Airflow and Dataproc jobs;
- IAM roles required to upload files, submit Dataproc Serverless batches, and
  run Spark jobs;
- local `gcp_creds.json` key used by Docker Compose as pipeline credentials.

Dataproc Serverless uses the project's default network configuration.

## Prerequisites

Create or choose a GCP project, enable billing, and make sure your Google
account has enough project permissions to manage APIs, IAM, service accounts,
and buckets. 

Install the Google Cloud CLI, then authenticate:

```bash
gcloud auth login
gcloud config set project <your-project-id>
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform
```

This creates Application Default Credentials for Terraform.

## Apply

Prepare variables:

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars`:

```hcl
project_id = "<project-id>"
region     = "europe-west1"
```

Run Terraform:

```bash
terraform init
terraform validate
terraform plan
terraform apply
```

Terraform generates globally unique bucket names using the project id and a
random suffix. The suffix is stored in Terraform state, so bucket names stay
stable across later plans.

Print values for the project `.env`:

```bash
terraform output -raw generated_env
```

Use these values to update the root `.env` config.

Terraform also writes the pipeline service account key to the project root:

```text
../gcp_creds.json
```

Because Terraform is run from `terraform/`, `../gcp_creds.json` places the key
in the project root. This matches `GOOGLE_APPLICATION_CREDENTIALS=./gcp_creds.json`
used by `.env` config and Docker Compose.

## Security Note

The generated private key is written both to `gcp_creds.json` and to Terraform
state. Keep these files private. The project `.gitignore` excludes local
credential files and Terraform state files.
