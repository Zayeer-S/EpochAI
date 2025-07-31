from typing import Any, Dict, List, Optional

from epochai.common.logging_config import get_logger
from epochai.common.database.dao.collection_attempts_dao import CollectionAttemptsDAO
from epochai.common.database.dao.collected_contents_dao import CollectedContentsDAO
from epochai.common.database.dao.collection_configs_dao import CollectionConfigsDAO
from epochai.common.data_utils import DataUtils
from epochai.common.config_loader import ConfigLoader

class WikipediaSaver:
    
    def __init__(self):
        try:
            self.logger = get_logger(__name__)
                        
            self.data_config = ConfigLoader.get_data_config()
            self.data_utils = DataUtils(self.data_config)
            
            self.save_to_database = self.data_config.get('data_output').get('database').get('save_to_database')
            if self.save_to_database:
                self.collection_attempts_dao = CollectionAttemptsDAO()
                self.collected_contents_dao = CollectedContentsDAO()
                self.collection_configs_dao = CollectionConfigsDAO()
                
                self.CONTENT_TYPE_ID = 1
                self.ATTEMPT_STATUS_ID = 1
                self.VALIDATION_STATUS_ID = 1
                self.METADATA_SCHEMA_ID = 1
            
        except ImportError as import_error:
            raise ImportError(f"Error importing modules: {import_error}")
        
        except Exception as general_error:
            if self.logger:
                self.logger.error(f"General error in __init__: {general_error}")
            else:
                print(f"General error in __init__ and self.logger returning false: {general_error}")
    
    def save_locally_at_end(
        self,
        collected_data: List[Dict[str, Any]],
        data_type: str
    ) -> Optional[str]:
        return self.data_utils.save_at_end(
            collected_data=collected_data,
            data_type=data_type
        )
        
    def save_incrementally_to_database(
        self,
        collected_data: List[Dict[str, Any]],
        collection_config_id: int,
        language_code: str
    ) -> Optional[int]:
       
        self.logger.info(f"Saving {len(collected_data)} Wikipedia articles to database for config {collection_config_id}...")
        
        success_count = 0
        
        for item in collected_data:
            title = item.get("title")
            content = item.get("content")
            url = item.get("url")
            
            if not title or not content:
                self.logger.warning(f"Skipping item due to missing title or content: {item}")
                continue
            
            try:
                attempt_id = self.collection_attempts_dao.create_attempt(
                    collection_config_id=collection_config_id,
                    language_code_used=language_code,
                    search_term_used=title,
                    attempt_status_id=self.ATTEMPT_STATUS_ID,
                    error_type_id=None,
                    error_message=""
                )
                
                if not attempt_id:
                    self.logger.error(f"Failed to create attempt for '{title}' - Not saving metadata for this")
                    continue
                else:
                    content_id = self.collected_contents_dao.create_content(
                        collection_attempt_id=attempt_id,
                        content_type_id=self.CONTENT_TYPE_ID,
                        content_metadata_schema_id=self.METADATA_SCHEMA_ID,
                        title=title,
                        main_content=content,
                        url=url,
                        validation_status_id=self.VALIDATION_STATUS_ID,
                        validation_error=None,
                        filepath_of_save=""
                    )
                    
                    if content_id:
                        success_count += 1
                    else:
                        self.logger.error(f"Failed  to insert content for '{title}'")
                    
            except Exception as general_error:
                self.logger.error(f"Database error while saving '{title}': {general_error}")
                    
        self.logger.info(f"Successfully saved {success_count} of length {len(collected_data)} Wikipedia articles to the database")
        return success_count
    
    def log_data_summary(
        self,
        collected_data: List[Dict[str, Any]]
    ) -> None:
        self.data_utils.log_data_summary(collected_data)
        
    def get_collection_config_id(
        self,
        collection_type: str,
        language_code: str,
        collection_name: str
    ) -> Optional[int]:
        """Gets the collection_config_id for the current collection"""
        try:
            configs = self.collection_configs_dao.get_uncollected_by_type_and_language(
                collection_type, language_code
            )
            
            for config in configs:
                if config.collection_name == collection_name:
                    self.logger.debug(f"Found exact config match id {config.id}")
                    return config.id
                    
            self.logger.warning(f"No existing config found for '{collection_name}'")
            return None
        
        except Exception as e:
            self.logger.error(f"Error getting collection config id: {e}")
            return None