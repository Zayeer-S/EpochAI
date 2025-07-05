import os
import sys
import yaml

from predictai.common.config_validator import ConfigValidator

if sys.version_info >= (3, 0):
    import locale
    
    try:
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    except locale.Error:
        try:
            locale.setlocale(locale.LC_ALL, 'C.UTF-8')
        except locale.Error:
            pass

class ConfigLoader:
    @staticmethod
    def load_the_config():  
        """Load the config from config.yaml"""
        current_dir = os.path.dirname(__file__)
        project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
        config_path = os.path.join(project_root, 'config.yml')
        
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
            if config is None:
                raise ValueError(f"Config file kinda sus yo: {config}")
            return config
        
        except FileNotFoundError:
            raise FileNotFoundError((f"Config file not found at location: {config_path}"))
        
        except yaml.YAMLError as e:
            raise ValueError(f"Error in parsing config.yml: {e}")
        
        except UnicodeDecodeError as e:
            raise ValueError(f"UTF-8 encoding error in {config_path}: {e}")

    @staticmethod
    def override_default_config_values(defaults, overrides):
        """Recursively merges two dictionaries (default and specific) from config.yml"""
        
        if not isinstance(defaults, dict) or not isinstance(overrides, dict):
            return overrides
        
        result = defaults.copy()
        
        for key, value in overrides.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = ConfigLoader.override_default_config_values(result[key], value)
            else:
                result[key] = value
                
        return result
        
    @staticmethod
    def get_wikipedia_collector_config():
        """Get Wikipedia collector configuration with defaults applied and validate it"""
        config = ConfigLoader.load_the_config()
        
        defaults = config.get('defaults', {}).get('wikipedia', {})
        main_config = config.get('wikipedia', {})
        
        merged_config = ConfigLoader.override_default_config_values(defaults, main_config)
        
        ConfigValidator.validate_wikipedia_config(merged_config)

        return merged_config
        
    @staticmethod
    def get_logging_config():
        """Get logging configuration and validate it"""
        config = ConfigLoader.load_the_config()
        logging_config = config.get('logging', {
            'level': 'INFO',
            'log_to_file': True,
            'log_directory': 'logs'
        })
        
        ConfigValidator.validate_logging_config(logging_config)
        
        return logging_config
    
    @staticmethod
    def get_all_collector_configs():
        """Gets all collector configs"""
        
        all_configs = {}
        
        try:
            all_configs['wikipedia'] = ConfigLoader.get_wikipedia_collector_config()
        except Exception as e:
            print(f"Could not load wikipedia collector: '{e}'")
            all_configs['wikipedia'] = None
            
        return all_configs