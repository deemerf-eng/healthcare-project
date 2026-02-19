import os
import time
import boto3

s3 = boto3.client("s3")
athena = boto3.client("athena")

SQL_BUCKET = os.environ["SQL_BUCKET"]
SQL_PREFIX = os.environ["SQL_PREFIX"].strip("/") + "/"

ATHENA_DATABASE = os.environ["ATHENA_DATABASE"]
ATHENA_OUTPUT_S3 = os.environ["ATHENA_OUTPUT_S3"].rstrip("/") + "/"
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")

# Polling settings
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "3"))
MAX_POLLS = int(os.environ.get("MAX_POLLS", "600"))  # ~30 min at 3s


def list_sql_keys(bucket: str, prefix: str) -> list[str]:
    keys = []
    token = None
    while True:
        args = {"Bucket": bucket, "Prefix": prefix}
        if token:
            args["ContinuationToken"] = token

        resp = s3.list_objects_v2(**args)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".sql"):
                keys.append(key)

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    keys.sort()  # relies on your 001_, 002_ ordering
    return keys


def get_sql(bucket: str, key: str) -> str:
    resp = s3.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read().decode("utf-8")


def start_query(sql: str) -> str:
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_S3},
        WorkGroup=ATHENA_WORKGROUP,
    )
    return resp["QueryExecutionId"]


def wait_for_query(qid: str) -> dict:
    for _ in range(MAX_POLLS):
        r = athena.get_query_execution(QueryExecutionId=qid)
        status = r["QueryExecution"]["Status"]["State"]
        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return r
        time.sleep(POLL_SECONDS)

    raise TimeoutError(f"Athena query {qid} did not finish in time")


def lambda_handler(event, context):
    keys = list_sql_keys(SQL_BUCKET, SQL_PREFIX)
    if not keys:
        return {"statusCode": 200, "body": f"No .sql files found under s3://{SQL_BUCKET}/{SQL_PREFIX}"}

    results = []
    for key in keys:
        sql = get_sql(SQL_BUCKET, key).strip()
        if not sql:
            print(f"Skipping empty SQL file: {key}")
            continue

        print(f"Running: s3://{SQL_BUCKET}/{key}")
        qid = start_query(sql)
        final = wait_for_query(qid)

        state = final["QueryExecution"]["Status"]["State"]
        reason = final["QueryExecution"]["Status"].get("StateChangeReason", "")

        results.append({"sql_key": key, "query_execution_id": qid, "state": state})

        if state != "SUCCEEDED":
            raise RuntimeError(f"Athena query failed for {key}. State={state}. Reason={reason}")

    return {
        "statusCode": 200,
        "body": f"Ran {len(results)} Athena queries successfully from s3://{SQL_BUCKET}/{SQL_PREFIX}",
        "results": results
    }
