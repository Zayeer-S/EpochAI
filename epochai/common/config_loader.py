import os
import sys
import yaml
from typing import Any, Dict, List, Optional

from epochai.common.config_validator import ValidateWholeConfig
from epochai.common.database.database import get_database
from epochai.common.database.dao.collection_configs_dao import CollectionConfigsDAO
from epochai.common.database.dao.collector_names_dao import CollectorNamesDAO
from epochai.common.database.dao.collection_types_dao import CollectionTypesDAO
from epochai.common.logging_config import get_logger

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
    
    def __init__(self):
        try:
            self.logger = get_logger(__name__)
            self.db_connection = get_database()
            self.collection_configs_dao = CollectionConfigsDAO()
            self.collector_names_dao = CollectorNamesDAO()
            self.collection_types_dao = CollectionTypesDAO()
            
        except ImportError as import_error:
            raise ImportError(f"Error importing modules: {import_error}")
        except Exception as general_error:
            raise Exception(f"General error while running __init__: {general_error}")
    
    @staticmethod
    def load_the_config() -> Dict[str, Any]:  
        """
        Loads the config from config.yaml
        
        Returns:
            Validated config (validated via helper function and config_validator)
        """
        current_dir = os.path.dirname(__file__)
        project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
        config_path = os.path.join(project_root, 'config.yml')
        
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
    def load_constraints_config():
        """Loads the constraints config"""
        current_dir = os.path.dirname(__file__)
        project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
        config_path = os.path.join(project_root, 'constraints.yml')
        
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
    def get_data_config():
        whole_config = ConfigLoader.load_the_config()
        
        data_settings_config = whole_config.get('data_settings', {})
        
        return data_settings_config
    
    @staticmethod
    def get_wikipedia_collector_yaml_config():
        """Gets Wikipedia collector (YAML only) configuration with defaults applied and validates it"""
        config = ConfigLoader.load_the_config()
        
        merged_config = ConfigLoader.get_merged_config(config, 'wikipedia')
        
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
        
        return logging_config
    
    @staticmethod
    def get_all_collector_configs():
        """Gets all collector configs"""
        
        all_configs = {}
        
        try:
            all_configs['wikipedia'] = ConfigLoader.get_wikipedia_collector_yaml_config()
        except Exception as e:
            print(f"Could not load wikipedia collector: '{e}'")
            all_configs['wikipedia'] = None
            
        return all_configs
    
    def get_collection_configs_from_database(
        self,
        collector_name: str = "wikipedia_collector",
        collection_type: Optional[str] = None,
        language_code: Optional[str] = None,
        is_collected: Optional[bool] = None
    ) -> Dict[str, Any]:
        """Gets collection configurations from database"""
        
        try:
            # Directly get uncollected if we have collection_type and language_code
            if collection_type and language_code:
                collection_configs = self.collection_configs_dao.get_uncollected_by_type_and_language(
                    collection_type, language_code
                )
                
            elif collection_type:
                if is_collected is not None:
                    all_configs = self.collection_configs_dao.get_by_collector_and_type(
                        collector_name, collection_type
                    )
                else:
                    collection_configs = self.collection_configs_dao.get_uncollected_by_type(collection_type)
            
            # Get all configs and filter so that we only have uncollected    
            else:
                all_configs = self.collection_configs_dao.get_all()
                collection_configs = all_configs
                
                if is_collected is not None:
                    collection_configs = [c for c in collection_configs if c.is_collected == is_collected]
                    
            grouped_configs = {}
            
            for each_config in collection_configs:
                collection_type_obj = self.collection_types_dao.get_by_id(each_config.collection_type_id)
                type_name = collection_type_obj.collection_type if collection_type_obj else 'unknown'
                
                if type_name not in grouped_configs:
                    grouped_configs[type_name] = {}
                    
                if each_config.language_code not in grouped_configs[type_name]:
                    grouped_configs[type_name][each_config.language_code] = []
                    
                grouped_configs[type_name][each_config.language_code].append({
                    'id': each_config.id,
                    'name': each_config.collection_name,
                    'is_collected': each_config.is_collected,
                    'created_at': each_config.created_at,
                    'updated_at': each_config.updated_at
                })
                
            self.logger.info(f"Retrieved {len(collection_configs)} collection configs from database")
            return grouped_configs
        
        except Exception as general_error:
            self.logger.error(f"Error retrieving collection configs from database: {general_error}")
            return {}
        
    def get_uncollected_configs_by_type(
        self,
        collection_type: str,
        collector_name: str = "wikipedia_collector"
    ) ->  Dict[str, List[str, Any]]:
        """Gets uncollected configs of a specific type  for a specific collector, grouped by language"""
        
        try:
            collection_configs = self.collection_configs_dao.get_uncollected_grouped_by_language(collection_type)
            
            result = {}
            for language_code, config_list in collection_configs.items():
                result[language_code] = [
                    {
                        'id': each_config.id,
                        'name': each_config.collection_name,
                        'is_collected': each_config.is_collected
                    }
                    for each_config in config_list
                ]
                
            self.logger.info(f"Retrieved uncollected {collection_type} configs for {len(result)}")
            return result
        
        except Exception as general_error:
            self.logger.error(f"Error retrieving uncollected {collection_type} configs: {general_error}")
            return {}
        
    def get_collected_status_summary(self) -> Dict[str, Any]:
        """Gets summary of collection status across all types and languages"""
        
        try:
            status = self.collection_configs_dao.get_collection_status()
            self.logger.info(f"Retrieved collection status summary from database")
            return status
        
        except Exception as general_error:
            self.logger.error(f"Error retrieving collection status summary: {general_error}")
            return {'by_type_and_language': [], 'summary': []}
        
    def mark_config_as_collected(
        self,
        config_id: int
    ) -> bool:
        """Marks a collection config as collected"""
        
        try:
            success = self.collection_configs_dao.mark_as_collected(config_id)
            if success:
                self.logger.info(f"Marked config {config_id} as collected")
            return True
        
        except Exception as general_error:
            self.logger.error(f"Error marking config {config_id} as collected")
            return False
        
    def mark_config_as_uncollected(
        self,
        config_id: int
    ) -> bool:
        """Marks a collection config as uncollected"""
        
        try:
            success = self.collection_configs_dao.mark_as_uncollected(config_id)
            if success:
                self.logger.info(f"Marked config {config_id} as uncollected")
            return True
        
        except Exception as general_error:
            self.logger.error(f"Error marking config {config_id} as uncollected")
            return False
        
    def search_collection_configs(
        self,
        search_term: str
    ) -> List[Dict[str, Any]]:
        """Search collection configs by their names"""
        
        try:
            collection_configs = self.collection_configs_dao.search_by_name(search_term)
            
            result = []
            for each_config in collection_configs:
                collection_type_obj = self.collection_types_dao.get_by_id(each_config.collection_type_id)
                type_name = collection_type_obj.collection_type if collection_type_obj else "unknown"
                
                result.append({
                    'id': each_config.id,
                    'name': each_config.collection_name,
                    'type': type_name,
                    'language_code': each_config.language_code,
                    'is_collected': each_config.is_collected,
                    'created_at': each_config.created_at
                })
                
            self.logger.info(f"Found {len(result)} configs matching search term '{search_term}'")
            return result
        
        except Exception as general_error:
            self.logger.error(f"Error searching collection configs for '{search_term}': {general_error}")
            return []
        
    def get_combined_wikipedia_config(self) -> Dict[str, Any]:
        """Gets combined wikipedia config from database and config.yml"""
        
        try:
            yaml_config = self.get_wikipedia_collector_yaml_config()
            
            db_configs = self.get_collection_configs_from_database(
                collector_name="wikipedia_collector"
            )
            
            combined_config = yaml_config.copy()
            
            if 'politicians' in db_configs:
                combined_config['politicians'] = db_configs['politicians']
                
            if 'political_topics' in db_configs:
                combined_config['political_topics'] = db_configs['political_topics']
                
            combined_config['_database_info'] = {
                'total_topics': len(db_configs),
                'last_updated': 'from_database'
            }
            
            self.logger.info(f"Successfully combined YAML and Database configurations")
            return combined_config
        
        except Exception as general_error:
            self.logger.error(f"Error getting combined Wikipedia config: {general_error} - falling back to YAML only")
            return self.get_wikipedia_collector_yaml_config()
        
    def test_database_connection(self) -> bool:
        """Tests if database connection is working"""
        
        try:
            return self.db_connection.test_connection()
        except Exception as general_error:
            self.logger.error(f"Database connection test failed: {general_error}")
            return False