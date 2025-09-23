import os
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv

# Load .env file
load_dotenv()  # Will load from .env by default

# === Pydantic Schemas ===
class kafka_schema(BaseModel):
    bootstrap_servers: str
    topic_name: str
    consumer_group: str
    auto_offset_reset: str

class api_schema(BaseModel):
    base_url: str
    api_key: Optional[str] = None

class settings_schema(BaseModel):
    fetch_interval: int

class aws_s3_schema(BaseModel):
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    region : Optional[str] = None
    bucket: str

# === Config Loader ===
class Config:
    @property
    def kafka_config(self) -> kafka_schema:
        return kafka_schema(
            bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS"),
            topic_name=os.getenv("TOPIC_NAME"),
            consumer_group=os.getenv("CONSUMER_GROUP"),
            auto_offset_reset=os.getenv("AUTO_OFFSET_RESET")
        )
    
    @property
    def api_config(self) -> api_schema:
        return api_schema(
            base_url=os.getenv("BASE_URL"),
            api_key=os.getenv("API_KEY")
        )
    
    @property
    def settings(self) -> settings_schema:
        return settings_schema(
            fetch_interval=int(os.getenv("FETCH_INTERVAL", 2))
        )
    
    @property
    def aws_s3(self) -> aws_s3_schema:
        return aws_s3_schema(
            access_key=os.getenv("AWS_ACCESS_KEY"),
            secret_key=os.getenv("AWS_SECRET_KEY"),
            region= os.getenv("AWS_REGION"),
            bucket=os.getenv("AWS_BUCKET")
        )

# Initialize config
config = Config()
