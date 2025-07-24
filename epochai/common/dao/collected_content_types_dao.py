from datetime import datetime
from typing import List, Optional

from epochai.common.database.database import get_database
from epochai.common.database.models import CollectedContentTypes
from epochai.common.logging_config import get_logger

class CollectedContentTypesDAO:
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)
    
    def create_content_type(
        self,
        collected_content_type_name: str
    ) -> Optional[int]:
        """Creates a new collected content type"""
        
        query = """
            INSERT INTO collected_content_types (collected_content_type_name, created_at, updated_at)
            VALUES (%s, %s, %s)
            RETURNING id
        """
        
        try:
            current_timestamp = datetime.now()
            params = (collected_content_type_name, current_timestamp, current_timestamp)
            result = self.db.execute_insert_query(query, params)
            
            if result:
                self.logger.info(f"Created collected content type: '{collected_content_type_name}'")
                return result
            else:
                self.logger.error(f"Failed to create collected content type: '{collected_content_type_name}'")
                return None
                
        except Exception as general_error:
            self.logger.error(f"Error creating collected content type '{collected_content_type_name}': {general_error}")
            return None
    
    def get_by_id(
        self,
        type_id: int
    ) -> Optional[CollectedContentTypes]:
        """Gets collected content type by ID"""
        
        query = """
            SELECT * FROM collected_content_types WHERE id = %s
        """
        
        try:
            results = self.db.execute_select_query(query, (type_id,))
            if results:
                return CollectedContentTypes.from_dict(results[0])
            return None
            
        except Exception as general_error:
            self.logger.error(f"Error getting collected content type by ID {type_id}: {general_error}")
            return None
    
    def get_by_name(
        self,
        collected_content_type_name: str
    ) -> Optional[CollectedContentTypes]:
        """Get collected content type by name"""
        
        query = """
        SELECT * FROM collected_content_types WHERE collected_content_type_name = %s
        """
        
        try:
            results = self.db.execute_select_query(query, (collected_content_type_name,))
            if results:
                return CollectedContentTypes.from_dict(results[0])
            return None
            
        except Exception as general_error:
            self.logger.error(f"Error getting collected content type by name '{collected_content_type_name}': {general_error}")
            return None
    
    def get_all(self) -> List[CollectedContentTypes]:
        """Get all collected content types"""
        
        query = """
            SELECT * FROM collected_content_types ORDER BY collected_content_type_name
        """
        
        try:
            results = self.db.execute_select_query(query)
            return [CollectedContentTypes.from_dict(row) for row in results]
            
        except Exception as general_error:
            self.logger.error(f"Error getting all collected content types: {general_error}")
            return []
    
    def get_or_create_content_type(
        self,
        collected_content_type_name: str
    ) -> Optional[CollectedContentTypes]:
        """Get existing content type or create new one if doesn't exist"""
        existing = self.get_by_name(collected_content_type_name)
        if existing:
            return existing
            
        new_id = self.create_content_type(collected_content_type_name)
        if new_id:
            return self.get_by_id(new_id)
        return None