import pandas as pd
# import boto3
# from io import BytesIO
from datetime import datetime, timezone
from typing import Annotated, Any
from src.pipeline.config.config import config

s3_path = f"s3://{config.aws_s3.bucket}"

def load_data_to_s3(records, prefix: str = s3_path) -> Any:
    if not records:
        return None
    
    df = pd.DataFrame(records)
    # buffer = BytesIO
    # df.write_parquet(buffer)

    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H-%M-%SZ')
    # unique_id = str(uuid.uuid4())
    file_path = f'{prefix}/arxiv_{timestamp}.parquet'

    try:
        df.to_parquet(
            file_path,
            index=False,
            engine='pyarrow',       # Required to support S3
            # storage_options={},     # Optional: add custom options like credentials if needed
        )
        print(f"✅ Uploaded to {s3_path}")
    except Exception as e:
        print(f"❌ Failed to upload: {e}")
