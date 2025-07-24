from datetime import datetime
from typing import List, Optional

from epochai.common.database.database import get_database
from epochai.common.database.models import RunTypes
from epochai.common.logging_config import get_logger

class RunTypesDAO:
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)
    
    def create_run_type(self, run_type_name: str) -> Optional[int]:
        """Create a new run type"""
        
        query = """
            INSERT INTO run_types (run_type_name, created_at, updated_at)
            VALUES (%s, %s, %s)
            RETURNING id
        """
        
        try:
            current_timestamp = datetime.now()
            params = (run_type_name, current_timestamp, current_timestamp)
            result = self.db.execute_insert_query(query, params)
            
            if result:
                self.logger.info(f"Created run type: '{run_type_name}'")
                return result
            else:
                self.logger.error(f"Failed to create run type: '{run_type_name}'")
                return None
                
        except Exception as general_error:
            self.logger.error(f"Error creating run type '{run_type_name}': {general_error}")
            return None
    
    def get_by_id(self, run_type_id: int) -> Optional[RunTypes]:
        """Get run type by ID"""
        
        query = """
            SELECT * FROM run_types WHERE id = %s
        """
        
        try:
            results = self.db.execute_select_query(query, (run_type_id,))
            if results:
                return RunTypes.from_dict(results[0])
            return None
            
        except Exception as general_error:
            self.logger.error(f"Error getting run type by ID {run_type_id}: {general_error}")
            return None
    
    def get_by_name(self, run_type_name: str) -> Optional[RunTypes]:
        """Get run type by name"""
        
        query = """
            SELECT * FROM run_types WHERE run_type_name = %s
        """
        
        try:
            results = self.db.execute_select_query(query, (run_type_name,))
            if results:
                return RunTypes.from_dict(results[0])
            return None
            
        except Exception as general_error:
            self.logger.error(f"Error getting run type by name '{run_type_name}': {general_error}")
            return None
    
    def get_all(self) -> List[RunTypes]:
        """Get all run types"""
        
        query = """
            SELECT * FROM run_types ORDER BY run_type_name
        """
        
        try:
            results = self.db.execute_select_query(query)
            return [RunTypes.from_dict(row) for row in results]
            
        except Exception as general_error:
            self.logger.error(f"Error getting all run types: {general_error}")
            return []