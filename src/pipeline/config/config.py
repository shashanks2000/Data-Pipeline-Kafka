import toml
import os
from pydantic import BaseModel
from typing import Optional

class kafka_schema(BaseModel):
    bootstrap_servers : str
    topic_name : str
    consumer_group : str
    auto_offset_reset : str

class api_schema(BaseModel):
    base_url : str
    api_key : Optional[str] = None

class settings_schema(BaseModel):
    fetch_interval : int

class aws_s3_schema(BaseModel):
    access_key : Optional[str] = None
    secret_key : Optional[str] = None
    bucket : str

class Config:
    def __init__(self, config_file=r'/Users/shashankachar/Desktop/Python-Projects/Data-Pipeline/config.toml'):
        # If the config_file is an absolute path, just use it directly
        if os.path.isabs(config_file):
            config_path = config_file
        else:
            # Otherwise, calculate relative path based on this file's directory
            current_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(current_dir, config_file)

        self.config = toml.load(config_path)

    @property
    def kafka_config(self) -> kafka_schema:
        return kafka_schema(**self.config['kafka'])  # Use ** to unpack dict
    
    @property
    def api_config(self) -> api_schema:
        return api_schema(**self.config['api'])
    
    @property
    def settings(self) -> settings_schema:
        return settings_schema(**self.config['settings'])
    
    @property
    def aws_s3(self) -> aws_s3_schema:
        return aws_s3_schema(**self.config['aws-s3'])

config = Config()
