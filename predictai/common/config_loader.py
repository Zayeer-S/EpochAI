import os
import yaml

from predictai.common.config_validator import ConfigValidator

class ConfigLoader:
    @staticmethod
    def load_the_config():  
        """Load the config from config.yaml"""
        current_dir = os.path.dirname(__file__)
        project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
        config_path = os.path.join(project_root, 'config.yml')
        
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            return config
        except FileNotFoundError:
            raise FileNotFoundError((f"Config file not found at location: {config_path}"))
        except yaml.YAMLError as e:
            raise ValueError(f"Error in parsing config.yml: {e}")
        
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
        
        years = merged_config.get('collection_years', [])
        events_template = merged_config.get('political_events_template', [])
        
        political_events = []
        for year in years:
            for template in events_template:    
                political_events.append(template.format(year=year))
        
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
    def get_events_for_years(years):
        """Get political events for non-default years"""
        config = ConfigLoader.load_the_config()
        
        defaults = config.get('defaults', {}).get('wikipedia', {})
        main_config = config.get('wikipedia', {})
        merged_config = ConfigLoader.override_default_config_values(defaults, main_config)
        
        events_template = merged_config.get('political_events_template', [])
        
        political_events = []
        
        for year in years:
            for template in events_template:
                political_events.append(template.format(year=year))
                
        return political_events
    
    @staticmethod
    def get_test_pages_for_debug():
        """Get all pages for testing in the debug script"""
        wikipedia_config = ConfigLoader.get_wikipedia_collector_config()
        return (wikipedia_config['politicians'] + 
                wikipedia_config['political_topics'] + 
                wikipedia_config['political_events'])