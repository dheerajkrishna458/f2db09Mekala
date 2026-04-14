import yaml
import os
import logging
from typing import Dict, Any
from pathlib import Path

class ConfigManager:
    """Load and parse YAML configuration files"""
    logger = logging.getLogger(f"{__name__}.{__qualname__}")
    
    @classmethod
    def read_config(cls, file_path: str) -> Dict[str, Any]:
        """Read and parse a YAML file."""
        with open(file_path, 'r') as f:
            config = yaml.safe_load(f)
            
        return config

    @classmethod
    def load_config(cls, args) -> Dict[str, Any]:
        """Load YAML config file"""
        # Construct the config file path
        config_file_path = os.path.join(args.config_directory_path, 
                                        args.layer, 
                                        args.config_file_name)
                                        
        if not os.path.exists(config_file_path):
            raise Exception(
                f"Provided config path doesn't exists, Please validate the path -> {config_file_path}"
            )
            
        cls.logger.info(f"Loading config from: {config_file_path}")
        
        config = cls.read_config(config_file_path)
        
        cls.logger.info("Config loaded successfully")
        return config
        
    @classmethod
    def validate_config(cls, required_keys: list, config: dict):
        """
        Validates that the configuration dictionary contains all required keys.
        
        Args:
            required_keys (list): A list of string keys that must be present in self.config.
            
        Raises:
            ValueError: If any key in required_keys is missing from self.config.
        """
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required key: {key}")
