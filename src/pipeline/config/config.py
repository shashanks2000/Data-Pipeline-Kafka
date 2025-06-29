import toml
import os

class Config:
    def __init__(self, config_file= 'config.toml'):
        current_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        config_path = os.path.join(current_dir, config_file)

        self.config = toml.load(config_path)

    @property
    def kafka_config(self):
        return self.config['kafka']
    
    @property
    def api_config(self):
        return self.config['api']
    
    @property
    def settings(self):
        return self.config['settings']
    
config = Config()