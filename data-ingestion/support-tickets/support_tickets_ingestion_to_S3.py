import os
from datetime import datetime, timedelta
from io import StringIO

import boto3
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
SHARED_ENV_PATH = os.path.join(BASE_DIR, ".env")
if os.path.exists(SHARED_ENV_PATH):
    load_dotenv(SHARED_ENV_PATH)
else:
    load_dotenv()

# ---------- CONFIG ----------
db_config = {
    "host": "localhost",
    "port": "3306",
    "user": "root",
    "password": "root",  
    "database": "careplus_support_db",
}

S3_BUCKET = "careplus-dataplus-store"
S3_PREFIX = "support-tickets/raw-data/"
INGESTION_DATE = "2025-07-01"
END_DATE = "2025-07-31"

AWS_CONFIG = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY"),
    "aws_secret_access_key": os.getenv("SECRET_KEY"),
    "region_name": os.getenv("REGION"),
}


# ---------- UTILITY FUNCTIONS ----------
def get_engine(config):
    return create_engine(
        f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    )


def upload_to_s3(dataframe, bucket, key):
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, index=False)

    s3 = boto3.client("s3", **AWS_CONFIG)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    print(f"✅ Uploaded to s3://{bucket}/{key}")

# ---------- MAIN INGESTION LOGIC ----------
def run_ingestion():
    try:
        engine = get_engine(db_config)
        current_date = datetime.strptime(INGESTION_DATE, "%Y-%m-%d")
        end_date = datetime.strptime(END_DATE, "%Y-%m-%d")

        while current_date <= end_date:
            current_date_str = current_date.strftime("%Y-%m-%d")
            query = f"""
                SELECT * FROM support_tickets
                WHERE DATE(created_at) = '{current_date_str}';
            """
            dataframe = pd.read_sql(query, engine)
            print(f"📅 Processing {current_date_str} -> {dataframe.shape}")

            if dataframe.empty:
                print(f"⚠️ No data found for {current_date_str}. Skipping.")
                current_date = current_date + timedelta(days=1)
                continue

            s3_key = f"{S3_PREFIX}support_tickets_{current_date_str}.csv"
            upload_to_s3(dataframe, S3_BUCKET, s3_key)

            current_date = current_date + timedelta(days=1)
    except Exception as error:
        print(f"❌ Tickets ingestion failed: {error}")


# Run
if __name__ == "__main__":
    run_ingestion()