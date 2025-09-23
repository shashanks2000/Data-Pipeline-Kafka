import pandas as pd
import boto3
from io import BytesIO
from datetime import datetime, timezone
from typing import Any
from src.pipeline.config.config import config

def load_data_to_s3(records) -> Any:
    if not records:
        return None

    df = pd.DataFrame(records)

    # Generate S3 file key
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H-%M-%SZ')
    s3_key = f"arxiv_{timestamp}.parquet"

    # Convert DataFrame to Parquet in-memory
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)  # Reset buffer pointer to beginning

    # Initialize boto3 client
    s3_client = boto3.client(
        "s3",
        region_name=config.aws_s3.region,
        aws_access_key_id=config.aws_s3.access_key,
        aws_secret_access_key=config.aws_s3.secret_key,
    )

    try:
        s3_client.upload_fileobj(buffer, config.aws_s3.bucket, s3_key)
        print(f"✅ Uploaded to s3://{config.aws_s3.bucket}/{s3_key}")
    except Exception as e:
        print(f"❌ Failed to upload: {e}")
