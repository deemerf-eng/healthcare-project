import sys
import re
import logging
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql import types as T


# -----------------------------
# Logging (CloudWatch-friendly)
# -----------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# -----------------------------
# Defaults: your 21 dataset folders under raw/
# -----------------------------
DEFAULT_DATASETS = [
    "snf_vbp_aggregate",
    "snf_vbp_facility",
    "nh_citation_descriptions",
    "nh_covid_vax_averages",
    "nh_covid_vax_provider",
    "nh_data_collection_intervals",
    "nh_fire_safety_citations",
    "nh_health_citations",
    "nh_health_inspection_cutpoints_state",
    "nh_ownership",
    "nh_penalties",
    "nh_provider_info",
    "nh_quality_measure_claims",
    "nh_quality_measure_mds",
    "nh_state_us_averages",
    "nh_survey_dates",
    "nh_survey_summary",
    "pbj_daily_nurse_staffing",
    "snf_qrp_national",
    "snf_qrp_provider",
    "swing_bed_snf",
]


# -----------------------------
# Parse required args
# -----------------------------
required = ["JOB_NAME", "RAW_BUCKET", "RAW_PREFIX", "REFINED_BUCKET", "REFINED_PREFIX"]
args = getResolvedOptions(sys.argv, required)

RAW_BUCKET = args["RAW_BUCKET"]
RAW_PREFIX = args["RAW_PREFIX"].strip("/")
REFINED_BUCKET = args["REFINED_BUCKET"]
REFINED_PREFIX = args["REFINED_PREFIX"].strip("/")


# -----------------------------
# Optional args (parse safely)
# -----------------------------
def get_opt(name: str, default: str) -> str:
    if f"--{name}" in sys.argv:
        return getResolvedOptions(sys.argv, [name]).get(name, default)
    return default

WRITE_MODE = get_opt("WRITE_MODE", "overwrite").lower()
HEADER = get_opt("HEADER", "true").lower()
DELIMITER = get_opt("DELIMITER", ",")
SKIP_MISSING = get_opt("SKIP_MISSING", "true").lower() == "true"

# ✅ If true, job will FAIL at the end if any dataset failed
FAIL_JOB_ON_DATASET_FAILURE = get_opt("FAIL_JOB_ON_DATASET_FAILURE", "true").lower() == "true"

datasets_raw = get_opt("DATASETS", "")
if datasets_raw.strip():
    DATASETS = [d.strip().strip("/") for d in datasets_raw.split(",") if d.strip()]
else:
    DATASETS = DEFAULT_DATASETS


# -----------------------------
# Spark / Glue setup
# -----------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


# -----------------------------
# Helpers
# -----------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def normalize_col(col: str) -> str:
    c = col.strip().lower()
    c = re.sub(r"[ \t\-\/]+", "_", c)
    c = re.sub(r"[^a-z0-9_]", "", c)
    c = re.sub(r"_+", "_", c)
    c = c.strip("_")
    return c if c else "col"

def uniqueify(cols):
    seen = {}
    out = []
    for c in cols:
        if c not in seen:
            seen[c] = 1
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}_{seen[c]}")
    return out

def s3_path(bucket: str, prefix: str, dataset: str) -> str:
    return f"s3://{bucket}/{prefix}/{dataset}/"

MISSING_TOKENS = {"", "na", "n/a", "null", "none", ".", "nan"}

def standardize_missing(df_in):
    exprs = []
    for field in df_in.schema.fields:
        c = field.name
        if c in ("_source_file", "_ingested_at_utc"):
            exprs.append(F.col(c))
            continue

        if isinstance(field.dataType, T.StringType):
            exprs.append(
                F.when(
                    F.col(c).isNull()
                    | (F.trim(F.col(c)) == "")
                    | (F.lower(F.trim(F.col(c))).isin(list(MISSING_TOKENS))),
                    F.lit(None)
                ).otherwise(F.col(c)).alias(c)
            )
        else:
            exprs.append(F.col(c))
    return df_in.select(*exprs)

def is_missing_path_error(e: Exception) -> bool:
    """
    Spark/S3 missing path errors often bubble up as Java exceptions.
    Check common substrings rather than exact matches.
    """
    msg = (str(e) or "").lower()
    patterns = [
        "path does not exist",
        "no such file or directory",
        "not found",
        "nosuchkey",
        "does not exist",
    ]
    return any(p in msg for p in patterns)


# -----------------------------
# Main job runner with exception handling
# -----------------------------
def run():
    logger.info("Starting Glue job")
    logger.info("RAW_BUCKET=%s RAW_PREFIX=%s", RAW_BUCKET, RAW_PREFIX)
    logger.info("REFINED_BUCKET=%s REFINED_PREFIX=%s", REFINED_BUCKET, REFINED_PREFIX)
    logger.info("WRITE_MODE=%s HEADER=%s DELIMITER=%s", WRITE_MODE, HEADER, DELIMITER)
    logger.info("SKIP_MISSING=%s FAIL_JOB_ON_DATASET_FAILURE=%s", SKIP_MISSING, FAIL_JOB_ON_DATASET_FAILURE)
    logger.info("DATASETS=%s", DATASETS)

    results = {"processed": [], "skipped_missing": [], "failed": []}

    for dataset in DATASETS:
        raw_path = s3_path(RAW_BUCKET, RAW_PREFIX, dataset)
        refined_path = s3_path(REFINED_BUCKET, REFINED_PREFIX, dataset)

        logger.info("---- DATASET=%s RAW=%s REFINED=%s", dataset, raw_path, refined_path)

        try:
            df = (
                spark.read.format("csv")
                .option("header", HEADER)
                .option("inferSchema", "true")   # OK for class project
                .option("sep", DELIMITER)
                .option("multiLine", "true")
                .option("quote", "\"")
                .option("escape", "\"")
                .load(raw_path)
            )

            # Very defensive: empty schema / empty columns
            if SKIP_MISSING and len(df.columns) == 0:
                logger.warning("SKIP: %s has 0 columns (missing/empty).", dataset)
                results["skipped_missing"].append({"dataset": dataset, "reason": "no columns"})
                continue

            # Lineage
            df = (
                df.withColumn("_source_file", F.input_file_name())
                  .withColumn("_ingested_at_utc", F.lit(utc_now_iso()))
            )

            # Normalize column names
            normalized_cols = uniqueify([normalize_col(c) for c in df.columns])
            df = df.toDF(*normalized_cols)

            # Missing tokens -> nulls
            df = standardize_missing(df)

            # Dedup (note: count() is expensive, but fine for a class project)
            before = df.count()
            df = df.dropDuplicates()
            after = df.count()
            logger.info("DEDUP: %s dropped %s exact duplicates (kept %s).", dataset, before - after, after)

            # Write Parquet
            (
                df.write.mode(WRITE_MODE)
                  .format("parquet")
                  .save(refined_path)
            )

            logger.info("OK: wrote parquet for %s to %s", dataset, refined_path)
            results["processed"].append({"dataset": dataset, "refined_path": refined_path})

        except Exception as e:
            if SKIP_MISSING and is_missing_path_error(e):
                logger.warning("SKIP (missing path): %s — %s", dataset, str(e), exc_info=True)
                results["skipped_missing"].append({"dataset": dataset, "reason": str(e)})
                continue

            logger.error("FAILED: %s — %s", dataset, str(e), exc_info=True)
            results["failed"].append({"dataset": dataset, "error": str(e)})
            # continue to next dataset

    logger.info("SUMMARY processed=%s skipped_missing=%s failed=%s",
                len(results["processed"]), len(results["skipped_missing"]), len(results["failed"]))
    logger.info("DETAILS: %s", results)

    # ✅ This is what “production” usually does: fail the job if anything failed
    if FAIL_JOB_ON_DATASET_FAILURE and len(results["failed"]) > 0:
        raise RuntimeError(f"One or more datasets failed: {results['failed']}")

    return results


# -----------------------------
# Entry point with job-level exception handling
# -----------------------------
try:
    job.init(args["JOB_NAME"], args)
    run()
    job.commit()
    logger.info("Glue job committed successfully")
except Exception as e:
    logger.error("Glue job failed fatally", exc_info=True)
    # job.commit() is generally safe to call once; we avoid double commit.
    raise