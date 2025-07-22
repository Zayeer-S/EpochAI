from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from epochai.common.database.database import get_database
from epochai.common.database.models import CollectionConfigs
from epochai.common.logging_config import get_logger

class CollectionConfigsDAO:
    """DAO for collection_configs table"""
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)
        
    def create_collection_config(
        self,
        collector_name_id: int,
        collection_type_id: int,
        language_code: str,
        collection_name: str,
        is_collected: bool = False
    ) -> Optional[int]:
        """
        Creates a new collection config entry
        
        Returns:
            ID of created config or None if failure
        """
        
        query = """
            INSERT INTO collection_configs
            (collector_name_id, collection_type_id, language_code, collection_name, is_collected, updated_at, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        
        try:
            current_timestamp = datetime.now()
            params = (collector_name_id, collection_type_id, language_code, collection_name, is_collected, current_timestamp, current_timestamp)
            result = self.db.execute_insert_query(query, params)
            
            if result:
                self.logger.info(f"Created collection config: '{collection_name}' ({language_code})")
                return True
            else:
                self.logger.error(f"Failed to create collection config: '{collection_name}' ({language_code})")
                return None
            
        except Exception as general_error:
            self.logger.error(f"Error creating collection config '{collection_name}' ({language_code}): {general_error}")
            return None
        
    def get_by_id(
        self,
        config_id: int
        ) -> Optional[CollectionConfigs]:
        """Gets collection config by id"""
        
        query = """
            SELECT * FROM collection_configs WHERE id = %s
        """
        
        try:
            results = self.db.execute_select_query(query, (config_id,))
            if results:
                return CollectionConfigs.from_dict(results[0])
            return None
        
        except Exception as general_error:
            self.logger.error(f"Error getting collection config by id {config_id}: {general_error}")
            return None
        
    def get_all(self) -> List[CollectionConfigs]:
        """Gets all collection configs"""
        query = """
            Select * FROM collection_configs ORDER BY created_at DESC
        """
        
        try:
            results = self.db.execute_select_query(query)
            return [CollectionConfigs.from_dict(row) for row in results]
        
        except Exception as general_error:
            self.logger.error(f"Error getting all collection configs: {general_error}")
            return []
        
    def get_by_collection_status(
        self,
        is_collected: bool
    ) -> List[CollectionConfigs]:
        """Gets configs by collection status"""
        
        query = """
            SELECT * FROM collection_configs WHERE is_collected = %s ORDER BY created_at ASC    
        """
        
        try:
            results = self.db.execute_select_query(query, (is_collected,))
            return [CollectionConfigs.from_dict(row) for row in results]
        
        except Exception as general_error:
            self.logger.error(f"Error getting configs by collection status '{is_collected}': {general_error}")
            return []
        
    def get_uncollected_by_type_and_language(
        self,
        collection_types: str,
        language_code: str
    ) -> List[CollectionConfigs]:
        """
        Gets uncollected configs by their collection type and language
        
        Note:
            I'm sorry to whoever sees this in the future for making the function name so long
        """
        
        query = """
            SELECT cc.*
            FROM collection_configs cc
            JOIN collection_types ct ON cc.collection_type_id = ct.id
            WHERE ct.collection_types = %s
            AND cc.language_code = %s
            AND cc.is_collected = FALSE
            ORDER BY cc.created_at ASC
        """
        
        try:
            results = self.db.execute_select_query(query, (collection_types, language_code))
            configs = [CollectionConfigs.from_dict(row) for row in results]
            
            self.logger.info(f"Found {len(configs)} uncollected in type '{collection_types}' for language code '{language_code}'")
            return configs
        
        except Exception as general_error:
            self.logger.error(f"Error getting uncollected {collection_types} in type '{collection_types}' configs for language code '{language_code}': {general_error}")
            return []
        
    def get_uncollected_by_type(
        self,
        collection_type: str
    ) -> List[CollectionConfigs]:
        """Gets all uncollected configs by type across all languages"""
        
        query = """
            SELECT cc.*
            FROM collection_configs cc
            JOIN collection_types ct ON cc.collection_type_id = ct.id
            WHERE ct.collection_type = %s
            AND cc.is_collected = false
            ORDER BY cc.language_code, cc.created_at ASC
        """
        
        try:
            results = self.db.execute_select_query(query, (collection_type,))
            configs = [CollectionConfigs.from_dict(row) for row in results]
            
            self.logger.info(f"Found {len(configs)} uncollected {collection_type} configs across all languages")
            return configs
        
        except Exception as general_error:
            self.logger.error(f"Error getting uncollected {collection_type} configs: {general_error}")
            return []
        
    def get_uncollected_grouped_by_language(
        self,
        collection_type: str
    ) -> Dict[str, List[CollectionConfigs]]:
        """
        Gets all uncolected configs grouped by language for easier processing
        """
        configs = self.get_uncollected_by_type(collection_type)
        grouped = {}
        
        for config in configs:
            if config.language_code not in grouped:
                grouped[config.language_code] = []
            grouped[config.language_code].append(config)
            
        return grouped
    
    def mark_as_collected(
        self,
        config_id: int
    ) -> bool:
        """Marks a row as collected"""
        
        query = """
            UPDATE collection_configs
            SET is_collected = true, updated_at = %s
            WHERE id = %s
        """
        
        try:
            affected_rows = self.db.execute_update_delete_query(query, (datetime.now(), config_id))
            
            if affected_rows > 0:
                self.logger.info(f"Marked config {config_id} as collected")
                return True
            else:
                self.logger.warning(f"No config found with id '{config_id}' to mark as collected")
                return False
            
        except Exception as general_error:
            self.logger.error(f"Error marking config with id '{config_id} as collected: {general_error}'")
            return False
        
    def mark_as_uncollected(
        self,
        config_id: int
    ) -> bool:
        """Marks a row as uncollected"""
        
        query = """
            UPDATE collection_configs
            SET is_collected = false, updated_at = %s
            WHERE id = %s
        """
        
        try:
            affected_rows = self.db.execute_update_delete_query(query, (datetime.now(), config_id))
            
            if affected_rows > 0:
                self.logger.info(f"Marked config {config_id} as uncollected")
                return True
            else:
                self.logger.warning(f"No config found with id '{config_id}' to mark as uncollected")
                return False
            
        except Exception as general_error:
            self.logger.error(f"Error marking config with id '{config_id} as uncollected: {general_error}'")
            return False
        
    def bulk_create_configs(
        self,
        collection_configs: List[Tuple[int, int, str, str, bool]]
    ) -> int:
        """
        Bulk creates multiple configs
        
        Returns:
            Number of successfully created configs
        """
        
        if not collection_configs:
            return 0
        
        query = """
            INSERT INTO collection_configs
            (collector_name_id, collection_type_id, language_code, collection_name, is_collected, updated_at, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        try:
            operations = []
            now = datetime.now()
            
            for config_data in collection_configs:
                collector_name_id, collection_type_id, language_code, collection_name, is_collected = config_data
                params = (collector_name_id, collection_type_id, language_code, collection_name, is_collected, now, now)
                operations.append((query, params))
                
            success = self.db.execute_transaction(operations)
            
            if success:
                self.logger.info(f"Successfully bulk created {len(collection_configs)} collection configs")
                return len(collection_configs)
            else:
                self.logger.error(f"Failed to bulk create collection configs")
                return 0
            
        except Exception as general_error:
            self.logger.error(f"Error bulk creating configs: {general_error}")
            return 0
        
    def get_collection_status(self) -> Dict[str, Any]:
        """Gets statistics about collection status"""
        query ="""
            SELECT
                ct.collection_type,
                cc.language_code,
                COUNT(*) as total_configs,
                SUM(CASE WHEN cc.is_collected THEN 1 ELSE 0 END) as collected_count,
                SUM(CASE WHEN NOT cc.is_collected THEN 1 ELSE 0 END) as uncollected_count
            FROM collection_configs cc
            JOIN collection_type ct ON cc.collection_type_id = ct.id
            GROUP BY ct.collection_type, cc.language_code
            ORDER BY ct.collection_type, cc.language_code
        """
        
        try:
            results = self.db.execute_select_query(query)
            
            stats = {
                'by_type_and_language': results,
                'summary': {}
            }
            
            total_configs = sum(row['total_configs'] for row in results)
            total_collected = sum(row['total_collected'] for row in results)
            total_uncollected = sum(row['total_uncollected'] for row in results)
        
            stats['summary'] = {
                'total_configs': total_configs,
                'total_collected': total_collected,
                'total_uncollected': total_uncollected,
                'collection_percentage': round((total_collected / total_configs * 100), 2) if total_configs > 0 else 0
            }
            
            return stats
        
        except Exception as general_error:
            self.logger.error(f"Error getting collection stats: {general_error}")
            return {'by_type_and_language', 'summary'}
        
    def delete_config(
        self,
        config_id: int
    ) -> bool:
        """Deletes a collection config"""
        
        query = """
            DELETE FROM collection_configs WHERE id = %s
        """

        try:
            affected_rows = self.db.execute_update_delete_query(query, (config_id,))
            
            if affected_rows > 0:
                self.logger.info(f"Deleted collection config {config_id}")
                return True
            else:
                self.logger.warning(f"No config found with id {config_id} to delete")
                return False
            
        except Exception as general_error:
            self.logger.error(f"Error deleting config {config_id}: {general_error}")
            return False
        
    def search_by_name(
        self,
        search_term: str
        ) -> List[CollectionConfigs]:
        """Search configs by collection name"""
        
        query = """
            SELECT * collection_configs WHERE collection_name ILIKE %s ORDER BY collection_name
        """
        
        try:
            search_pattern = f"%{search_term}%"
            results = self.db.execute_select_query(query, search_pattern)
            return [CollectionConfigs.from_dict(row) for row in results]
        
        except Exception as general_error:
            self.logger.error(f"Error searching configs by name '{search_term}': {general_error}")
            return []
        
    def get_by_collector_and_type(
        self,
        collector_name: str,
        collection_type: str
    ) -> List[CollectionConfigs]:
        """Gets configs by collector name and collection type"""
        
        query = """
            SELECT cc.*
            FROM collection_configs cc
            JOIN collection_names cc ON cc.collector_name_id = cn.id
            JOIN collection_types ct ON cc.collection_type_id = ct.id
            WHERE cn.collector_name = %s AND ct.collection_type = %s
            ORDER BY cc.language_code, cc.collection_name
        """
        
        try:
            results = self.db.execute_select_query(query, (collector_name, collection_type))
            return [CollectionConfigs.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting configs for collector '{collector_name}' and type '{collection_type}': {general_error}")
            return []