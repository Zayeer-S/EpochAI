from datetime import datetime
from typing import Any, Dict, List, Optional
import json

from epochai.common.database.database import get_database
from epochai.common.database.models import ContentMetadataSchemas
from epochai.common.logging_config import get_logger

class ContentMetadataSchemasDAO:
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)
    
    def create_schema(
        self,
        content_metadata_schema: Dict[str, Any]
    ) -> Optional[int]:
        """Creates a new content metadata schema"""
        
        query = """
            INSERT INTO content_metadata_schemas (content_metadata_schema, created_at, updated_at)
            VALUES (%s, %s, %s)
            RETURNING id
        """
        
        try:
            current_timestamp = datetime.now()
            schema_json = json.dumps(content_metadata_schema)
            params = (schema_json, current_timestamp, current_timestamp)
            result = self.db.execute_insert_query(query, params)
            
            if result:
                self.logger.info(f"Created content metadata schema with ID: {result}")
                return result
            else:
                self.logger.error(f"Failed to create content metadata schema")
                return None
                
        except Exception as general_error:
            self.logger.error(f"Error creating content metadata schema: {general_error}")
            return None
    
    def get_by_id(
        self,
        schema_id: int
    ) -> Optional[ContentMetadataSchemas]:
        """Gets content metadata schema by ID"""
        
        query = """
            SELECT * FROM content_metadata_schemas WHERE id = %s
        """
        
        try:
            results = self.db.execute_select_query(query, (schema_id,))
            if results:
                return ContentMetadataSchemas.from_dict(results[0])
            return None
            
        except Exception as general_error:
            self.logger.error(f"Error getting content metadata schema by ID {schema_id}: {general_error}")
            return None
    
    def get_all(self) -> List[ContentMetadataSchemas]:
        """Gets all content metadata schemas"""
        
        query = """
            SELECT * FROM content_metadata_schemas ORDER BY created_at DESC
        """
        
        try:
            results = self.db.execute_select_query(query)
            return [ContentMetadataSchemas.from_dict(row) for row in results]
            
        except Exception as general_error:
            self.logger.error(f"Error getting all content metadata schemas: {general_error}")
            return []
    
    def update_schema(
        self,
        schema_id: int,
        content_metadata_schema: Dict[str, Any]
    ) -> bool:
        """Updates an existing content metadata schema"""
        
        query = """
            UPDATE content_metadata_schemas 
            SET content_metadata_schema = %s, updated_at = %s 
            WHERE id = %s
        """
        
        try:
            schema_json = json.dumps(content_metadata_schema)
            params = (schema_json, datetime.now(), schema_id)
            affected_rows = self.db.execute_update_delete_query(query, params)
            
            if affected_rows > 0:
                self.logger.info(f"Updated content metadata schema {schema_id}")
                return True
            else:
                self.logger.warning(f"No content metadata schema found with ID {schema_id}")
                return False
                
        except Exception as general_error:
            self.logger.error(f"Error updating content metadata schema {schema_id}: {general_error}")
            return False
    
    def delete_schema(
        self,
        schema_id: int
    ) -> bool:
        """Deletes a content metadata schema"""
        
        query = """
            DELETE FROM content_metadata_schemas WHERE id = %s
        """
        
        try:
            affected_rows = self.db.execute_update_delete_query(query, (schema_id,))
            
            if affected_rows > 0:
                self.logger.info(f"Deleted content metadata schema {schema_id}")
                return True
            else:
                self.logger.warning(f"No content metadata schema found with ID {schema_id}")
                return False
                
        except Exception as general_error:
            self.logger.error(f"Error deleting content metadata schema {schema_id}: {general_error}")
            return False
    
    def find_schema_by_content(
        self,
        schema_content: Dict[str, Any]
    ) -> Optional[ContentMetadataSchemas]:
        """Finds a schema that matches the given content structure"""
        
        query = """
            SELECT * FROM content_metadata_schemas WHERE content_metadata_schema = %s
        """
        
        try:
            schema_json = json.dumps(schema_content, sort_keys=True)
            results = self.db.execute_select_query(query, (schema_json,))
            if results:
                return ContentMetadataSchemas.from_dict(results[0])
            return None
            
        except Exception as general_error:
            self.logger.error(f"Error finding schema by content: {general_error}")
            return None
    
    def get_or_create_schema(self, content_metadata_schema: Dict[str, Any]) -> Optional[ContentMetadataSchemas]:
        """Get existing schema or create new one if doesn't exist"""
        existing = self.find_schema_by_content(content_metadata_schema)
        if existing:
            return existing
            
        new_id = self.create_schema(content_metadata_schema)
        if new_id:
            return self.get_by_id(new_id)
        return None
