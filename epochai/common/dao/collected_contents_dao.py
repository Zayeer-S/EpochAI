from datetime import datetime
from typing import Any, Dict, List, Optional
import json

from epochai.common.database.database import get_database
from epochai.common.database.models import CollectedContents
from epochai.common.logging_config import get_logger

class CollectedContentsDAO:
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)
    
    def create_content(
        self,
        collection_attempt_id: int,
        content_type_id: int,
        content_metadata_schema_id: int,
        title: str,
        main_content: str,
        url: Optional[str] = None,
        validation_status_id: int = 0,
        validation_error: Optional[Dict[str, Any]] = None,
        filepath_of_save: str = ""
    ) -> Optional[int]:
        """
        Creates a new collected content record
        
        Returns:
            The id of created content or None if it fails
        """
        
        query = """
            INSERT INTO collected_contents 
            (collection_attempt_id, content_type_id, content_metadata_schema_id, title, main_content, 
             url, validation_status_id, validation_error, filepath_of_save, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        
        try:
            current_timestamp = datetime.now()
            validation_error_json = json.dumps(validation_error) if validation_error else None
            
            params = (
                collection_attempt_id, content_type_id, content_metadata_schema_id, 
                title, main_content, url, validation_status_id, validation_error_json, 
                filepath_of_save, current_timestamp
            )
            
            result = self.db.execute_insert_query(query, params)
            
            if result:
                self.logger.info(f"Created collected content: '{title}' (attempt_id: {collection_attempt_id})")
                return result
            else:
                self.logger.error(f"Failed to create collected content: '{title}'")
                return None
                
        except Exception as general_error:
            self.logger.error(f"Error creating collected content '{title}': {general_error}")
            return None
    
    def get_by_id(
        self,
        content_id: int
    ) -> Optional[CollectedContents]:
        """Gets collected content by id"""
        
        query = """
            SELECT * FROM collected_contents WHERE id = %s
        """
        
        try:
            results = self.db.execute_select_query(query, (content_id,))
            if results:
                return CollectedContents.from_dict(results[0])
            return None
            
        except Exception as general_error:
            self.logger.error(f"Error getting collected content by id {content_id}: {general_error}")
            return None
    
    def get_all(
        self,
        limit: Optional[int] = None
    ) -> List[CollectedContents]:
        """Gets all collected contents with an optional limit"""
        
        query = """
            SELECT * FROM collected_contents ORDER BY created_at DESC
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            results = self.db.execute_select_query(query)
            return [CollectedContents.from_dict(row) for row in results]
            
        except Exception as general_error:
            self.logger.error(f"Error getting all collected contents: {general_error}")
            return []
    
    def get_by_attempt_id(
        self,
        collection_attempt_id: int
    ) -> List[CollectedContents]:
        """Gets all contents for a specific collection attempt"""
        
        query = """
            SELECT * FROM collected_contents WHERE collection_attempt_id = %s ORDER BY created_at DESC
        """
        
        try:
            results = self.db.execute_select_query(query, (collection_attempt_id,))
            return [CollectedContents.from_dict(row) for row in results]
            
        except Exception as general_error:
            self.logger.error(f"Error getting contents for attempt {collection_attempt_id}: {general_error}")
            return []
    
    def get_by_content_type(
        self,
        content_type_name: str
    ) -> List[CollectedContents]:
        """Gets contents by content type"""
        
        query = """
            SELECT cc.*
            FROM collected_contents cc
            JOIN collected_content_types cct ON cc.content_type_id = cct.id
            WHERE cct.collected_content_type_name = %s
            ORDER BY cc.created_at DESC
        """
        
        try:
            results = self.db.execute_select_query(query, (content_type_name,))
            contents = [CollectedContents.from_dict(row) for row in results]
            
            self.logger.info(f"Found {len(contents)} contents of type '{content_type_name}'")
            return contents
            
        except Exception as general_error:
            self.logger.error(f"Error getting contents by type '{content_type_name}': {general_error}")
            return []
    
    def get_by_validation_status(
        self,
        validation_status_name: str
    ) -> List[CollectedContents]:
        """Gets contents by validation status"""
        
        query = """
            SELECT cc.*
            FROM collected_contents cc
            JOIN validation_statuses vs ON cc.validation_status_id = vs.id
            WHERE vs.validation_status_name = %s
            ORDER BY cc.created_at DESC
        """
        
        try:
            results = self.db.execute_select_query(query, (validation_status_name,))
            contents = [CollectedContents.from_dict(row) for row in results]
            
            self.logger.info(f"Found {len(contents)} contents with validation status '{validation_status_name}'")
            return contents
            
        except Exception as general_error:
            self.logger.error(f"Error getting contents by validation status '{validation_status_name}': {general_error}")
            return []
    
    def get_invalid_contents(self) -> List[CollectedContents]:
        """Gets all contents that failed validation"""
        return self.get_by_validation_status("invalid")
    
    def get_valid_contents(self) -> List[CollectedContents]:
        """Gets all contents that passed validation"""
        return self.get_by_validation_status("valid")
    
    def get_pending_validation(self) -> List[CollectedContents]:
        """Gets all contents pending validation"""
        return self.get_by_validation_status("pending")
    
    def update_validation_status(
        self, 
        content_id: int, 
        validation_status_id: int,
        validation_error: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Update validation status and error for a content"""
        
        query = """
            UPDATE collected_contents 
            SET validation_status_id = %s, validation_error = %s
            WHERE id = %s
        """
        
        try:
            validation_error_json = json.dumps(validation_error) if validation_error else None
            params = (validation_status_id, validation_error_json, content_id)
            
            affected_rows = self.db.execute_update_delete_query(query, params)
            
            if affected_rows > 0:
                self.logger.info(f"Updated validation status for content {content_id}")
                return True
            else:
                self.logger.warning(f"No content found with id {content_id} to update")
                return False
                
        except Exception as general_error:
            self.logger.error(f"Error updating validation status for content {content_id}: {general_error}")
            return False
    
    def search_by_title(
        self,
        search_term: str
    ) -> List[CollectedContents]:
        """Search contents by title (partial match)"""
        
        query = """
            SELECT * FROM collected_contents WHERE title ILIKE %s ORDER BY created_at DESC
        """
        
        try:
            search_pattern = f"%{search_term}%"
            results = self.db.execute_select_query(query, (search_pattern,))
            return [CollectedContents.from_dict(row) for row in results]
            
        except Exception as general_error:
            self.logger.error(f"Error searching contents by title '{search_term}': {general_error}")
            return []
    
    def search_by_content(
        self,
        search_term: str,
        limit: int = 50
    ) -> List[CollectedContents]:
        """Search contents by main content (partial match)"""
        
        query = """
            SELECT * FROM collected_contents 
            WHERE main_content ILIKE %s 
            ORDER BY created_at DESC 
            LIMIT %s
        """
        
        try:
            search_pattern = f"%{search_term}%"
            results = self.db.execute_select_query(query, (search_pattern, limit))
            return [CollectedContents.from_dict(row) for row in results]
            
        except Exception as general_error:
            self.logger.error(f"Error searching contents by content '{search_term}': {general_error}")
            return []
    
    def get_contents_with_details(
        self,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Gets contents with all relevant details
        """
        
        query = """
            SELECT 
                cc.*,
                vs.validation_status_name,
                cct.collected_content_type_name,
                ca.search_term_used,
                ca.language_code_used,
                cfg.collection_name,
                ct.collection_type
            FROM collected_contents cc
            LEFT JOIN validation_statuses vs ON cc.validation_status_id = vs.id
            LEFT JOIN collected_content_types cct ON cc.content_type_id = cct.id
            LEFT JOIN collection_attempts ca ON cc.collection_attempt_id = ca.id
            LEFT JOIN collection_configs cfg ON ca.collection_config_id = cfg.id
            LEFT JOIN collection_types ct ON cfg.collection_type_id = ct.id
            ORDER BY cc.created_at DESC
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            results = self.db.execute_select_query(query)
            self.logger.info(f"Retrieved {len(results)} contents with details")
            return results
            
        except Exception as general_error:
            self.logger.error(f"Error getting contents with details: {general_error}")
            return []
    
    def get_content_statistics(self) -> Dict[str, Any]:
        """Gets comprehensive statistics about the collected contents"""
        
        basic_stats_query = """
            SELECT 
                COUNT(*) as total_contents,
                AVG(LENGTH(main_content)) as avg_content_length,
                MIN(LENGTH(main_content)) as min_content_length,
                MAX(LENGTH(main_content)) as max_content_length
            FROM collected_contents
        """
        
        validation_stats_query = """
            SELECT 
                vs.validation_status_name,
                COUNT(*) as content_count
            FROM collected_contents cc
            JOIN validation_statuses vs ON cc.validation_status_id = vs.id
            GROUP BY vs.validation_status_name
            ORDER BY content_count DESC
        """
        
        type_stats_query = """
            SELECT 
                cct.collected_content_type_name,
                COUNT(*) as content_count
            FROM collected_contents cc
            JOIN collected_content_types cct ON cc.content_type_id = cct.id
            GROUP BY cct.collected_content_type_name
            ORDER BY content_count DESC
        """
        
        try:
            basic_stats = self.db.execute_select_query(basic_stats_query)
            validation_stats = self.db.execute_select_query(validation_stats_query)
            type_stats = self.db.execute_select_query(type_stats_query)
            
            stats = {
                'basic_stats': basic_stats[0] if basic_stats else {},
                'by_validation_status': validation_stats,
                'by_content_type': type_stats,
                'summary': {}
            }
            
            if basic_stats and basic_stats[0]['total_contents']:
                total_contents = basic_stats[0]['total_contents']
                
                for status_row in validation_stats:
                    status_name = status_row['validation_status_name']
                    count_of_contents = status_row['content_count']
                    percentage = round((count_of_contents / total_contents * 100), 2)
                    
                    stats['summary'][status_name] = {
                        'count': count_of_contents,
                        'percentage': percentage
                    }
            
            return stats
            
        except Exception as general_error:
            self.logger.error(f"Error getting content statistics: {general_error}")
            return {'basic_stats': {}, 'by_validation_status': [], 'by_content_type': [], 'summary': {}}
    
    def get_recent_contents(
        self,
        hours: int = 24
    ) -> List[CollectedContents]:
        """Gets contents collected in the last X hours"""
        
        query = """
            SELECT * FROM collected_contents 
            WHERE created_at >= %s 
            ORDER BY created_at DESC
        """
        
        try:
            from datetime import timedelta
            cutoff_time = datetime.now() - timedelta(hours=hours)
            results = self.db.execute_select_query(query, (cutoff_time,))
            return [CollectedContents.from_dict(row) for row in results]
            
        except Exception as general_error:
            self.logger.error(f"Error getting recent contents from last {hours} hours: {general_error}")
            return []
    
    def get_duplicate_titles(self) -> List[Dict[str, Any]]:
        """Find contents with duplicate titles for cleanup"""
        
        query = """
            SELECT title, COUNT(*) as duplicate_count, 
                   ARRAY_AGG(id) as content_ids,
                   MIN(created_at) as first_created,
                   MAX(created_at) as last_created
            FROM collected_contents 
            GROUP BY title 
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC
        """
        
        try:
            results = self.db.execute_select_query(query)
            self.logger.info(f"Found {len(results)} titles with duplicates")
            return results
            
        except Exception as general_error:
            self.logger.error(f"Error finding duplicate titles: {general_error}")
            return []
    
    def delete_content(
        self,
        content_id: int
    ) -> bool:
        """Delete a collected content record"""
        
        query = """
            DELETE FROM collected_contents WHERE id = %s
        """
        
        try:
            affected_rows = self.db.execute_update_delete_query(query, (content_id,))
            
            if affected_rows > 0:
                self.logger.info(f"Deleted collected content {content_id}")
                return True
            else:
                self.logger.warning(f"No content found with id {content_id} to delete")
                return False
                
        except Exception as general_error:
            self.logger.error(f"Error deleting content {content_id}: {general_error}")
            return False
    
    def delete_old_contents(
        self,
        days_old: int
    ) -> int:
        """Delete contents older than specified days"""
        
        query = """
            DELETE FROM collected_contents 
            WHERE created_at < %s
        """
        
        try:
            from datetime import timedelta
            cutoff_date = datetime.now() - timedelta(days=days_old)
            affected_rows = self.db.execute_update_delete_query(query, (cutoff_date,))
            
            if affected_rows > 0:
                self.logger.info(f"Deleted {affected_rows} contents older than {days_old} days")
            else:
                self.logger.info(f"No contents older than {days_old} days found to delete")
                
            return affected_rows
            
        except Exception as general_error:
            self.logger.error(f"Error deleting old contents: {general_error}")
            return 0
    
    def bulk_update_validation_status(
        self, 
        content_ids: List[int], 
        validation_status_id: int
    ) -> int:
        """Bulk update validation status for multiple contents"""
        
        if not content_ids:
            return 0
            
        placeholders = ','.join(['%s'] * len(content_ids))
        
        query = f"""
            UPDATE collected_contents 
            SET validation_status_id = %s
            WHERE id IN ({placeholders})
        """
        
        try:
            params = [validation_status_id] + content_ids
            affected_rows = self.db.execute_update_delete_query(query, tuple(params))
            
            if affected_rows > 0:
                self.logger.info(f"Bulk updated validation status for {affected_rows} contents")
            
            return affected_rows
            
        except Exception as general_error:
            self.logger.error(f"Error bulk updating validation status: {general_error}")
            return 0
    
    def get_by_filepath(
        self,
        filepath: str
    ) -> List[CollectedContents]:
        """Gets contents by their save filepath"""
        
        query = """
            SELECT * FROM collected_contents WHERE filepath_of_save = %s ORDER BY created_at DESC
        """
        
        try:
            results = self.db.execute_select_query(query, (filepath,))
            return [CollectedContents.from_dict(row) for row in results]
            
        except Exception as general_error:
            self.logger.error(f"Error getting contents by filepath '{filepath}': {general_error}")
            return []