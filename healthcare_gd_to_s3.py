import os
import io
import json
import boto3
import re
from datetime import datetime, timezone

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account


# ===============================
# Environment Variables
# ===============================
GOOGLE_SERVICE_ACCOUNT_SECRET_NAME = os.environ.get("GOOGLE_SERVICE_ACCOUNT_SECRET_NAME")
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID")  # optional (root if not set)
TARGET_S3_BUCKET = os.environ.get("TARGET_S3_BUCKET")

# Glue trigger (Option B)
glue_client = boto3.client("glue")
GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME")  # REQUIRED for Step 3

# If you set S3_PREFIX="raw", you’ll get: raw/<dataset>/<file>
# If you set S3_PREFIX="" you’ll get: <dataset>/<file>
S3_PREFIX = os.environ.get("S3_PREFIX", "").strip("/")

DDB_TABLE_NAME = os.environ.get("DDB_TABLE_NAME")


# ===============================
# AWS Clients
# ===============================
s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")
dynamodb = boto3.resource("dynamodb")


# ===============================
# Dataset routing rules (your 21-table plan)
# ===============================
ROUTE_RULES = [
    # SNF VBP (2)
    (re.compile(r"^FY_\d{4}_SNF_VBP_Aggregate_Performance\.csv$", re.IGNORECASE), "snf_vbp_aggregate"),
    (re.compile(r"^FY_\d{4}_SNF_VBP_Facility_Performance\.csv$", re.IGNORECASE), "snf_vbp_facility"),

    # NH datasets (15)
    (re.compile(r"^NH_CitationDescriptions_.*\.csv$", re.IGNORECASE), "nh_citation_descriptions"),
    (re.compile(r"^NH_CovidVaxAverages_.*\.csv$", re.IGNORECASE), "nh_covid_vax_averages"),
    (re.compile(r"^NH_CovidVaxProvider_.*\.csv$", re.IGNORECASE), "nh_covid_vax_provider"),
    (re.compile(r"^NH_DataCollectionIntervals_.*\.csv$", re.IGNORECASE), "nh_data_collection_intervals"),
    (re.compile(r"^NH_FireSafetyCitations_.*\.csv$", re.IGNORECASE), "nh_fire_safety_citations"),
    (re.compile(r"^NH_HealthCitations_.*\.csv$", re.IGNORECASE), "nh_health_citations"),
    (re.compile(r"^NH_HlthInspecCutpointsState_.*\.csv$", re.IGNORECASE), "nh_health_inspection_cutpoints_state"),
    (re.compile(r"^NH_Ownership_.*\.csv$", re.IGNORECASE), "nh_ownership"),
    (re.compile(r"^NH_Penalties_.*\.csv$", re.IGNORECASE), "nh_penalties"),
    (re.compile(r"^NH_ProviderInfo_.*\.csv$", re.IGNORECASE), "nh_provider_info"),
    (re.compile(r"^NH_QualityMsr_Claims_.*\.csv$", re.IGNORECASE), "nh_quality_measure_claims"),
    (re.compile(r"^NH_QualityMsr_MDS_.*\.csv$", re.IGNORECASE), "nh_quality_measure_mds"),
    (re.compile(r"^NH_StateUSAverages_.*\.csv$", re.IGNORECASE), "nh_state_us_averages"),
    (re.compile(r"^NH_SurveyDates_.*\.csv$", re.IGNORECASE), "nh_survey_dates"),
    (re.compile(r"^NH_SurveySummary_.*\.csv$", re.IGNORECASE), "nh_survey_summary"),

    # PBJ staffing (1)
    (re.compile(r"^PBJ_Daily_Nurse_Staffing_.*\.csv$", re.IGNORECASE), "pbj_daily_nurse_staffing"),

    # SNF QRP (2)
    (re.compile(r"^Skilled_Nursing_Facility_Quality_Reporting_Program_National_Data_.*\.csv$", re.IGNORECASE), "snf_qrp_national"),
    (re.compile(r"^Skilled_Nursing_Facility_Quality_Reporting_Program_Provider_Data_.*\.csv$", re.IGNORECASE), "snf_qrp_provider"),

    # Swing Bed (1)
    (re.compile(r"^Swing_Bed_SNF_data_.*\.csv$", re.IGNORECASE), "swing_bed_snf"),
]


def dataset_folder_for(file_name: str) -> str:
    for pattern, folder in ROUTE_RULES:
        if pattern.match(file_name):
            return folder
    return "_unmapped"


# ===============================
# Helpers
# ===============================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def require_env(name: str, value: str):
    if not value:
        raise ValueError(f"{name} env var is not set")


def get_service_account_info_from_secret():
    require_env("GOOGLE_SERVICE_ACCOUNT_SECRET_NAME", GOOGLE_SERVICE_ACCOUNT_SECRET_NAME)
    resp = secrets_client.get_secret_value(SecretId=GOOGLE_SERVICE_ACCOUNT_SECRET_NAME)
    secret_string = resp.get("SecretString")
    if not secret_string:
        raise ValueError("SecretString is empty in Secrets Manager response")
    return json.loads(secret_string)


def get_drive_service():
    sa_info = get_service_account_info_from_secret()
    scopes = ["https://www.googleapis.com/auth/drive.readonly"]
    credentials = service_account.Credentials.from_service_account_info(sa_info, scopes=scopes)
    return build("drive", "v3", credentials=credentials)


def build_s3_key(file_name: str) -> str:
    """
    Writes to: <S3_PREFIX>/<dataset_folder>/<file_name>
    Example: raw/nh_provider_info/NH_ProviderInfo_Oct2024.csv

    If S3_PREFIX is empty, writes to: <dataset_folder>/<file_name>
    """
    dataset = dataset_folder_for(file_name)
    if S3_PREFIX:
        return f"{S3_PREFIX}/{dataset}/{file_name}"
    return f"{dataset}/{file_name}"


def list_files_in_folder(service):
    """
    Lists files in the folder. Includes modifiedTime for incremental checks.
    """
    if DRIVE_FOLDER_ID:
        query = f"'{DRIVE_FOLDER_ID}' in parents and trashed = false"
    else:
        query = "'root' in parents and trashed = false"

    files = []
    page_token = None

    while True:
        resp = service.files().list(
            q=query,
            fields="nextPageToken, files(id, name, mimeType, modifiedTime, size)",
            pageToken=page_token
        ).execute()

        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return files


def download_to_tmp(service, file_id: str, tmp_path: str):
    """
    Stream download to /tmp to avoid large in-memory buffers.
    """
    request = service.files().get_media(fileId=file_id)
    with open(tmp_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                print(f"Download progress: {int(status.progress() * 100)}%")


def upload_file_to_s3(tmp_path: str, bucket: str, key: str):
    with open(tmp_path, "rb") as f:
        s3_client.upload_fileobj(f, bucket, key)


def get_ddb_table():
    require_env("DDB_TABLE_NAME", DDB_TABLE_NAME)
    return dynamodb.Table(DDB_TABLE_NAME)


def should_process_file(table, file_id: str, drive_modified_time: str) -> bool:
    """
    Return True if:
      - file_id not in DDB (new)
      - or drive_modified_time is newer than stored modifiedTime
    """
    resp = table.get_item(Key={"file_id": file_id})
    item = resp.get("Item")

    if not item:
        return True  # new file

    last_modified = item.get("modifiedTime")
    if not last_modified:
        return True  # missing state, be safe and process

    # ISO-8601 timestamps compare correctly as strings when both are in same format
    return drive_modified_time > last_modified


def update_state(table, file_id: str, file_name: str, drive_modified_time: str, s3_bucket: str, s3_key: str):
    table.put_item(
        Item={
            "file_id": file_id,
            "file_name": file_name,
            "modifiedTime": drive_modified_time,
            "s3_bucket": s3_bucket,
            "s3_key": s3_key,
            "dataset_folder": dataset_folder_for(file_name),  # helpful for debugging
            "last_processed_at": utc_now_iso()
        }
    )


# ===============================
# Step 3 Helper: Start Glue job once per ingest run (Option B)
# ===============================
def start_glue_job_if_needed(processed_files: int, skipped_files: int, unmapped_files: int) -> dict:
    """
    Start the Glue job only if processed_files > 0.
    Also avoid starting a new run if one is already STARTING/RUNNING/STOPPING.
    """
    if processed_files <= 0:
        return {"started": False, "reason": "No new/changed files to process"}

    require_env("GLUE_JOB_NAME", GLUE_JOB_NAME)

    # Guard against duplicate concurrent runs
    try:
        runs = glue_client.get_job_runs(JobName=GLUE_JOB_NAME, MaxResults=5).get("JobRuns", [])
        for r in runs:
            state = r.get("JobRunState")
            if state in ("STARTING", "RUNNING", "STOPPING"):
                return {
                    "started": False,
                    "reason": f"Glue job already {state}",
                    "existing_job_run_id": r.get("Id")
                }
    except Exception as e:
        # If this guard fails for any reason, we can still attempt to start the job.
        print(f"WARNING: Could not check existing Glue runs: {e}")

    # Pass a few helpful args (optional) for logging/debugging inside Glue
    arguments = {
        "--triggered_by": "drive_to_s3_lambda",
        "--s3_bucket": TARGET_S3_BUCKET,
        "--s3_prefix": S3_PREFIX or "",
        "--processed_files": str(processed_files),
        "--skipped_files": str(skipped_files),
        "--unmapped_files": str(unmapped_files),
        "--run_ts_utc": utc_now_iso(),
    }

    resp = glue_client.start_job_run(JobName=GLUE_JOB_NAME, Arguments=arguments)
    return {"started": True, "job_run_id": resp.get("JobRunId")}


# ===============================
# Lambda Entry Point
# ===============================
def lambda_handler(event, context):
    require_env("TARGET_S3_BUCKET", TARGET_S3_BUCKET)

    print("Starting Google Drive → S3 ingestion (incremental mode)")
    print(f"S3 target: s3://{TARGET_S3_BUCKET}/{S3_PREFIX + '/' if S3_PREFIX else ''}<dataset>/<file>")

    drive = get_drive_service()
    table = get_ddb_table()

    files = list_files_in_folder(drive)
    print(f"Drive returned {len(files)} item(s)")

    processed = 0
    skipped = 0
    unmapped = 0

    for f in files:
        file_id = f["id"]
        file_name = f["name"]
        mime_type = f.get("mimeType", "unknown")
        modified_time = f.get("modifiedTime")

        # CSV-only filter
        if not file_name.lower().endswith(".csv"):
            print(f"Skipping non-CSV file: {file_name} ({mime_type})")
            continue

        if not modified_time:
            print(f"No modifiedTime for {file_name}; processing to be safe.")
        else:
            if not should_process_file(table, file_id, modified_time):
                print(f"Skipping unchanged file: {file_name} (modifiedTime={modified_time})")
                skipped += 1
                continue

        s3_key = build_s3_key(file_name)
        dataset = dataset_folder_for(file_name)
        if dataset == "_unmapped":
            unmapped += 1
            print(f"WARNING: Unmapped filename pattern for {file_name}. Uploading under {S3_PREFIX}/_unmapped/")

        tmp_path = f"/tmp/{file_id}.csv"  # use file_id to avoid weird filename issues

        print(f"Processing file: {file_name} ({mime_type}) modifiedTime={modified_time}")
        print(f"Routing to dataset folder: {dataset}")
        print(f"Downloading → {tmp_path}")
        download_to_tmp(drive, file_id, tmp_path)

        print(f"Uploading → s3://{TARGET_S3_BUCKET}/{s3_key}")
        upload_file_to_s3(tmp_path, TARGET_S3_BUCKET, s3_key)

        try:
            os.remove(tmp_path)
        except OSError:
            pass

        # Update state AFTER successful upload
        update_state(table, file_id, file_name, modified_time or "", TARGET_S3_BUCKET, s3_key)

        processed += 1
        print(f"Done: {file_name}")

    # ===============================
    # Step 2 + Step 3: Trigger Glue after ingest (Option B)
    # ===============================
    glue_result = start_glue_job_if_needed(processed, skipped, unmapped)
    print(f"Glue trigger result: {glue_result}")

    msg = (
        f"Processed {processed} CSV file(s), skipped {skipped} unchanged CSV file(s). "
        f"Unmapped processed: {unmapped}. Glue: {glue_result}"
    )
    print(msg)
    return {"statusCode": 200, "body": msg}
