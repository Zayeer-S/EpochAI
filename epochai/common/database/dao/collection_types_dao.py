from datetime import datetime
from typing import List, Optional

from epochai.common.database.database import get_database
from epochai.common.database.models import CollectionTypes
from epochai.common.logging_config import get_logger

class CollectionTypesDAO:
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)
    
    def create_collection_type(
        self,
        collection_type: str
    ) -> Optional[int]:
        """Creates a new collection type"""
        
        query = """
            INSERT INTO collection_types (collection_type, created_at, updated_at)
            VALUES (%s, %s, %s)
            RETURNING id
        """
        
        try:
            current_timestamp = datetime.now()
            params = (collection_type, current_timestamp, current_timestamp)
            result = self.db.execute_insert_query(query, params)
            
            if result:
                self.logger.info(f"Created collection type: '{collection_type}'")
                return result
            else:
                self.logger.error(f"Failed to create collection type: '{collection_type}'")
                return None
                
        except Exception as general_error:
            self.logger.error(f"Error creating collection type '{collection_type}': {general_error}")
            return None
    
    def get_by_id(
        self,
        type_id: int
    ) -> Optional[CollectionTypes]:
        """Gets collection type by ID"""
        query = """
            SELECT * FROM collection_types WHERE id = %s
        """
        
        try:
            results = self.db.execute_select_query(query, (type_id,))
            if results:
                return CollectionTypes.from_dict(results[0])
            return None
            
        except Exception as general_error:
            self.logger.error(f"Error getting collection type by ID {type_id}: {general_error}")
            return None
    
    def get_by_type(
        self,
        collection_type: str
    ) -> Optional[CollectionTypes]:
        """Gets collection type by name"""
        
        query = """
            SELECT * FROM collection_types WHERE collection_type = %s
        """
        
        try:
            results = self.db.execute_select_query(query, (collection_type,))
            if results:
                return CollectionTypes.from_dict(results[0])
            return None
            
        except Exception as general_error:
            self.logger.error(f"Error getting collection type by name '{collection_type}': {general_error}")
            return None
    
    def get_all(self) -> List[CollectionTypes]:
        """Gets all collection types"""
        
        query = """
            SELECT * FROM collection_types ORDER BY collection_type
        """
        
        try:
            results = self.db.execute_select_query(query)
            return [CollectionTypes.from_dict(row) for row in results]
            
        except Exception as general_error:
            self.logger.error(f"Error getting all collection types: {general_error}")
            return []
    
    def get_or_create_type(
        self,
        collection_type: str
    ) -> Optional[CollectionTypes]:
        """Gets existing type or create new one if doesn't exist"""
        existing = self.get_by_type(collection_type)
        if existing:
            return existing
            
        new_id = self.create_collection_type(collection_type)
        if new_id:
            return self.get_by_id(new_id)
        return None
