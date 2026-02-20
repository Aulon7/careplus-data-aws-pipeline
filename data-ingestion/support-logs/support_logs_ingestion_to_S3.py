import os
import boto3
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
SCRIPT_DIR = os.path.dirname(__file__)
SHARED_ENV_PATH = os.path.join(BASE_DIR, ".env")
if os.path.exists(SHARED_ENV_PATH):
    load_dotenv(SHARED_ENV_PATH)
else:
    load_dotenv()

# ---------- CONFIG ----------
LOGS_FOLDER = os.path.join(SCRIPT_DIR, "day-wise-logs-data")

S3_BUCKET = "careplus-dataplus-store"
S3_PREFIX = "support-logs/raw-data/"

AWS_CONFIG = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY"),
    "aws_secret_access_key": os.getenv("SECRET_KEY"),
    "region_name": os.getenv("REGION"),
}

def upload_log_file_to_s3(file_path, bucket, key):
    s3 = boto3.client("s3", **AWS_CONFIG)
    with open(file_path, "r", encoding="utf-8") as file:
        content = file.read()
    s3.put_object(Bucket=bucket, Key=key, Body=content)
    print(f"Uploaded log file to s3://{bucket}/{key}")


def run_log_ingestion():
    try:
        log_files = sorted(
            [
                file_name
                for file_name in os.listdir(LOGS_FOLDER)
                if file_name.startswith("support_logs_") and file_name.endswith(".log")
            ]
        )

        if not log_files:
            print("No log files found to ingest.")
            return

        for file_name in log_files:
            log_file_full_path = os.path.join(LOGS_FOLDER, file_name)
            s3_key = f"{S3_PREFIX}{file_name}"
            upload_log_file_to_s3(log_file_full_path, S3_BUCKET, s3_key)

        print(f"Ingestion complete. Uploaded {len(log_files)} file(s).")
    except Exception as error:
        print(f"Log ingestion failed: {error}")


if __name__ == "__main__":
    run_log_ingestion()