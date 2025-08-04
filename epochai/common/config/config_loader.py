import os
import sys
import yaml
from typing import Any, Dict
from epochai.common.config.config_validator import ValidateWholeConfig

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
    def _get_config_path(filename: str) -> str:
        """Gets config path of config.yml and constraints.yml"""
        current_dir = os.path.dirname(__file__)
        project_root = os.path.abspath(os.path.join(current_dir, '..', '..', '..'))
        config_path = os.path.join(project_root, f'{filename}')
        return config_path
    
    @staticmethod
    def load_the_config() -> Dict[str, Any]:  
        """
        Loads the config from config.yaml
        
        Returns:
            Validated config (validated via helper function and config_validator)
        """
        config_path = ConfigLoader._get_config_path('config.yml')
        
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
            if config is None:
                raise ValueError(f"Config file returning as None: {config}")
            
            ConfigLoader.validate_whole_config(config)
            
            return config
        
        except FileNotFoundError:
            raise FileNotFoundError((f"Config file not found at location: {config_path}"))
        
        except yaml.YAMLError as e:
            raise ValueError(f"Error in parsing config.yml: {e}")
        
        except UnicodeDecodeError as e:
            raise ValueError(f"UTF-8 encoding error in {config_path}: {e}")
    
    @staticmethod  
    def validate_whole_config(
        config: Dict[str, Any]
        ) -> Dict[str, Any]:
        """
        Validates parts of config that need validation by using ConfigValidator
        
        Returns:
            The config (dict) originally sent to this (if validation fails, config_validator will throw a ValueError)
        """
        merged_wikipedia_config = ConfigLoader.get_merged_config(config, 'wikipedia')
        
        config_parts_to_validate = {
            'data_settings': config.get('data_settings'),
            'logging': config.get('logging'),
            'wikipedia': merged_wikipedia_config
        }
        
        ValidateWholeConfig.validate_config(config_parts_to_validate)
        
        return config
        
    @staticmethod
    def load_constraints_config() -> Dict[str, Any]:
        """Loads the constraints config"""
        config_path = ConfigLoader._get_config_path("constraints.yml")
        
        try:
            with open(config_path, 'r') as file:
                constraints_config = yaml.safe_load(file)
            if constraints_config is None:
                raise ValueError(f"File returning as None: {constraints_config}")
            return constraints_config
        
        except FileNotFoundError:
            raise FileNotFoundError((f"Config file not found at location: {config_path}"))
        
        except yaml.YAMLError as e:
            raise ValueError(f"Error in parsing config.yml: {e}")
        
    @staticmethod
    def get_merged_config(
        config: Dict[str, Any],
        config_name: str
        ) -> Dict[str, Any]:
        """
        Takes the default config and the override config and merges them via a helper function
        
        Returns:
            Merged config (default config settings overriden by override config)
        """
        defaults = config.get('defaults').get(config_name)
        main_config = config.get(config_name)
        merged_config = ConfigLoader.override_default_config_values(defaults, main_config)
        return merged_config

    @staticmethod
    def override_default_config_values(defaults, overrides) -> Dict[str, Any]:
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
    def get_data_config() -> Dict[str, Any]:
        """Gets just the YAML data_settings portion of the config"""
        whole_config = ConfigLoader.load_the_config()
        
        data_settings_config = whole_config.get('data_settings', {})
        
        return data_settings_config
    
    @staticmethod
    def get_wikipedia_yaml_config() -> Dict[str, Any]:
        """Gets Wikipedia collector (YAML only) configuration with defaults applied and validates it"""
        config = ConfigLoader.load_the_config()
        
        merged_config = ConfigLoader.get_merged_config(config, 'wikipedia')
        
        return merged_config
        
    @staticmethod
    def get_logging_config() -> Dict[str, Any]:
        """Get logging configuration and validate it"""
        config = ConfigLoader.load_the_config()
        logging_config = config.get('logging', {
            'level': 'INFO',
            'log_to_file': True,
            'log_directory': 'logs'
        })
        
        return logging_config
    
    @staticmethod
    def get_all_collector_configs() -> Dict[str, Any]:
        """Gets all collector configs"""
        
        all_configs = {}
        
        try:
            wikipedia_yaml_config = ConfigLoader.get_wikipedia_yaml_config()
            collector_name = wikipedia_yaml_config['collector_name']
            
            from epochai.common.database.collection_config_manager import CollectionConfigManager
            all_configs['wikipedia'] = CollectionConfigManager.get_combined_wikipedia_config(collector_name= collector_name)
        except Exception as e:
            print(f"Could not load wikipedia collector: '{e}'")
            all_configs['wikipedia'] = None
            
        return all_configs
    
    @staticmethod
    def get_wikipedia_config() -> Dict[str, Any]:
        """Gets whole Wikipedia Config (combination of YAML + DB) and returns it"""
        wikipedia_yaml_config = ConfigLoader.get_wikipedia_yaml_config()
        collector_name = wikipedia_yaml_config['api']['collector_name']
        
        from epochai.common.database.collection_config_manager import CollectionConfigManager
        return CollectionConfigManager.get_combined_wikipedia_config(collector_name = collector_name)
    
    @staticmethod   
    def get_collection_status_summary() -> Dict[str, Any]:
        """Gets collection status summary"""
        from epochai.common.database.collection_config_manager import CollectionConfigManager
        return CollectionConfigManager.get_collection_configs_from_database()