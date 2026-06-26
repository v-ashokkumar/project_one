project_one
Move data from a source Google Cloud Storage (GCS) bucket to a destination GCS bucket using Apache Airflow and Google Cloud Functions.

Overview
This project moves or copies objects from a source GCS bucket to a destination GCS bucket on a schedule (via Airflow) and optionally triggers Cloud Functions for additional processing. Typical uses: backups, ETL staging, or bucket migration.

High-level architecture:

Airflow DAG schedules and orchestrates the transfer.
Airflow can trigger Cloud Functions (or call GCP APIs) to perform object-level processing or validation.
Service account with least-privileged IAM roles performs GCS operations.
Features
Scheduled, repeatable transfers using Airflow.
Configurable source and destination buckets.
Optional GCS -> Cloud Function trigger for post-processing.
Logging and failure handling via Airflow.
Prerequisites
Google Cloud Project with billing enabled.
gcloud CLI installed and authenticated.
A Composer (Managed Airflow) environment or an Airflow instance reachable by you.
Python 3.8+ (for local testing / function packaging).
Permissions: service account with roles/storage.objectViewer on source and roles/storage.objectAdmin (or storage.objectCreator + storage.objectViewer) on destination. If invoking Cloud Functions, add roles/cloudfunctions.invoker as needed.
Repository layout (recommended)
dags/ — Airflow DAG(s)
cloud_functions/ — Cloud Function source code
scripts/ — helper/deploy scripts
requirements.txt — Python dependencies
README.md — this file
Configuration
Set the following variables in Airflow (via Variables / Connections) or environment variables for local deployment:

SOURCE_BUCKET: name of the source GCS bucket
DESTINATION_BUCKET: name of the destination GCS bucket
MOVE_MODE: "copy" or "move" (copy keeps original; move deletes source object after successful copy)
GCS_PREFIX (optional): only process objects under this prefix
SERVICE_ACCOUNT_KEY (if not using Workload Identity / default credentials) — path to JSON key (use secrets manager where possible)
Recommended Airflow Variables:

project_id
location (GCP region)
cloud_function_name (if using CF)
dag_schedule: cron or '@daily', etc.
Deploying the Cloud Function
Example (replace placeholders):

Package and deploy: gcloud functions deploy <FUNCTION_NAME>
--runtime python39
--trigger-http
--entry-point <ENTRY_FN>
--region
--project <PROJECT_ID>
--set-env-vars DESTINATION_BUCKET=

Grant invoke permission to Airflow service account: gcloud functions add-iam-policy-binding <FUNCTION_NAME>
--member serviceAccount:<AIRFLOW_SA>@<PROJECT_ID>.iam.gserviceaccount.com
--role roles/cloudfunctions.invoker

Airflow DAG (high level)
The DAG should:
list objects in SOURCE_BUCKET (optionally filtered by GCS_PREFIX)
for each object (or in batches) copy to DESTINATION_BUCKET
verify the copied object (size/CRC/MD5)
if MOVE_MODE==move, delete the object in source after successful copy
optionally trigger a Cloud Function or Pub/Sub message for downstream processing
Use Google-provided operators (e.g., GoogleCloudStorageToGoogleCloudStorageOperator) or implement with GCSHook.
Example snippet (conceptual):

from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSToGCSOperator

transfer = GCSToGCSOperator(
    task_id="copy_objects",
    source_bucket="{{ var.value.SOURCE_BUCKET }}",
    source_object="{{ var.value.GCS_PREFIX }}*",
    destination_bucket="{{ var.value.DESTINATION_BUCKET }}",
    move_object=False  # True to move
)
Testing locally
Use gsutil to create test objects: gsutil cp sample.txt gs://<SOURCE_BUCKET>/
Run DAG in sandboxed Airflow, or test Cloud Function locally via functions framework: python -m functions_framework --target <ENTRY_FN> --debug
Logging & Monitoring
Airflow UI will show DAG run logs and task failures.
Enable GCS access logs if you need storage-level auditing.
Use Stackdriver (Cloud Logging) for Cloud Function logs.
Security
Prefer Workload Identity or Application Default Credentials (ADC) over long-lived service account keys.
Give minimal IAM roles to the service account used by Airflow.
Store secrets in Secret Manager and avoid plaintext credentials in the repo.
Troubleshooting
Permission errors: verify IAM roles for service account.
Partial copies: check DAG retries, network issues, and verify object checksums.
Large files: consider streaming copies or increasing task resources.
Next steps / TODO
Add an example DAG in dags/ implementing the full flow.
Add automated deployment scripts (gcloud / Terraform).
Add tests and CI to validate deployments.
