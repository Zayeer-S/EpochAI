from datetime import datetime
from typing import List, Optional

from epochai.common.database.database import get_database
from epochai.common.database.models import RunStatuses
from epochai.common.logging_config import get_logger

class RunStatusesDAO:
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)
    
    def create_run_status(
        self,
        run_status_name: str
    ) -> Optional[int]:
        """Creates a new run status"""
        
        query = """
            INSERT INTO run_statuses (run_status_name, created_at, updated_at)
            VALUES (%s, %s, %s)
            RETURNING id
        """
        
        try:
            current_timestamp = datetime.now()
            params = (run_status_name, current_timestamp, current_timestamp)
            result = self.db.execute_insert_query(query, params)
            
            if result:
                self.logger.info(f"Created run status: '{run_status_name}'")
                return result
            else:
                self.logger.error(f"Failed to create run status: '{run_status_name}'")
                return None
                
        except Exception as general_error:
            self.logger.error(f"Error creating run status '{run_status_name}': {general_error}")
            return None
    
    def get_by_id(
        self,
        status_id: int
    ) -> Optional[RunStatuses]:
        """Gets run status by ID"""
        
        query = """
            SELECT * FROM run_statuses WHERE id = %s
        """
        
        try:
            results = self.db.execute_select_query(query, (status_id,))
            if results:
                return RunStatuses.from_dict(results[0])
            return None
            
        except Exception as general_error:
            self.logger.error(f"Error getting run status by ID {status_id}: {general_error}")
            return None
    
    def get_by_name(
        self,
        run_status_name: str
    ) -> Optional[RunStatuses]:
        """Gets run status by name"""
        
        query = """
            SELECT * FROM run_status WHERE run_status_name = %s
        """
        
        try:
            results = self.db.execute_select_query(query, (run_status_name,))
            if results:
                return RunStatuses.from_dict(results[0])
            return None
            
        except Exception as general_error:
            self.logger.error(f"Error getting run status by name '{run_status_name}': {general_error}")
            return None
    
    def get_all(self) -> List[RunStatuses]:
        """Gets all run statuses"""
        
        query = """
            SELECT * FROM run_statuses ORDER BY run_status_name
        """
        
        try:
            results = self.db.execute_select_query(query)
            return [RunStatuses.from_dict(row) for row in results]
            
        except Exception as general_error:
            self.logger.error(f"Error getting all run statuses: {general_error}")
            return []
