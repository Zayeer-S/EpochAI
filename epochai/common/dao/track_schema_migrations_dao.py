from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from epochai.common.database.database import get_database
from epochai.common.database.models import TrackSchemaMigrations
from epochai.common.logging_config import get_logger


class TrackSchemaMigrationsDAO:
    
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)
    
    def create_migration_record(
        self,
        version: str,
        filename: str,
        executed_at: datetime,
        execution_time_seconds: float,
        status: str = 'completed',
        checksum: Optional[str] = None,
        error_message: Optional[str] = None
    ) -> Optional[int]:
        """
        Creates a new migration record.
            
        Returns:
            ID of created record or None if failed
        """
        query = """
            INSERT INTO schema_migrations 
            (version, filename, checksum, executed_at, execution_time_seconds, status, error_message, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """
        
        try:
            current_timestamp = datetime.now()
            params = (
                version, filename, checksum, executed_at, execution_time_seconds, 
                status, error_message, current_timestamp
            )
            
            result = self.db.execute_insert_query(query, params)
            
            if result:
                self.logger.info(f"Created migration record for version {version}: {status}")
                return result
            else:
                self.logger.error(f"Failed to create migration record for version {version}")
                return None
                
        except Exception as general_error:
            self.logger.error(f"Error creating migration record for version {version}: {general_error}")
            return None
    
    def get_by_version(self, migration_record_version: str) -> Optional[TrackSchemaMigrations]:
        """
        Get migration record by version.
            
        Returns:
            TrackSchemaMigrations object or None if not found
        """
        query = """
            SELECT * FROM schema_migrations WHERE version = %s
        """
        
        try:
            results = self.db.execute_select_query(query, (migration_record_version,))
            if results:
                return TrackSchemaMigrations.from_dict(results[0])
            return None
            
        except Exception as general_error:
            self.logger.error(f"Error getting migration by version {migration_record_version}: {general_error}")
            return None
    
    def get_all_migrations(self) -> List[TrackSchemaMigrations]:
        """
        Gets all migration records ordered by version.
        
        Returns:
            TrackSchemaMigrations objects (list)
        """
        
        query = """
            SELECT * FROM schema_migrations ORDER BY version ASC
        """
        
        try:
            results = self.db.execute_select_query(query)
            return [TrackSchemaMigrations.from_dict(row) for row in results]
            
        except Exception as general_error:
            self.logger.error(f"Error getting all migrations: {general_error}")
            return []
    
    def get_applied_migration_versions(self) -> Set[str]:
        """
        Gets set of all applied migration versions.
        
        Returns:
                Version strings for applied migrations (set)
        """
        query = """
            SELECT version FROM schema_migrations 
            WHERE status = 'completed' AND rolled_back_at IS NULL
            ORDER BY version ASC
        """
        
        try:
            results = self.db.execute_select_query(query)
            return {row['version'] for row in results}
            
        except Exception as general_error:
            self.logger.error(f"Error getting applied migration versions: {general_error}")
            return set()
    
    def get_by_status(self, status: str) -> List[TrackSchemaMigrations]:
        """Gets migrations by status"""
        
        query = """
            SELECT * FROM schema_migrations WHERE status = %s ORDER BY executed_at DESC
        """
        
        try:
            results = self.db.execute_select_query(query, (status,))
            migrations = [TrackSchemaMigrations.from_dict(row) for row in results]
            
            self.logger.info(f"Found {len(migrations)} migrations with status '{status}'")
            return migrations
            
        except Exception as general_error:
            self.logger.error(f"Error getting migrations by status '{status}': {general_error}")
            return []
    
    def get_failed_migrations(self) -> List[TrackSchemaMigrations]:
        """Gets all failed migrations."""
        return self.get_by_status('failed')
    
    def get_completed_migrations(self) -> List[TrackSchemaMigrations]:
        """Gets all completed migrations."""
        return self.get_by_status('completed')
    
    def get_rolled_back_migrations(self) -> List[TrackSchemaMigrations]:
        """Gets all rolled back migrations."""
        return self.get_by_status('rolled_back')
    
    def mark_migration_rolled_back(self, version_marked_rolled_back: str) -> bool:
        """
        Marks a migration as rolled backe
        
        Returns:
            True if successful and vice versa
        """
        
        query = """
            UPDATE schema_migrations 
            SET status = 'rolled_back', rolled_back_at = %s
            WHERE version = %s AND status = 'completed'
        """
        
        try:
            rollback_time = datetime.now()
            affected_rows = self.db.execute_update_delete_query(query, (rollback_time, version_marked_rolled_back))
            
            if affected_rows > 0:
                self.logger.info(f"Marked migration {version_marked_rolled_back} as rolled back")
                return True
            else:
                self.logger.warning(f"No completed migration found with version {version_marked_rolled_back} to rollback")
                return False
                
        except Exception as general_error:
            self.logger.error(f"Error marking migration {version_marked_rolled_back} as rolledback: {general_error}")
            return False
    
    def get_latest_migration(self) -> Optional[TrackSchemaMigrations]:
        """
        Gets the most recently applied migration.
        
        Returns:
            Latest TrackSchemaMigrations object or None if no migrations
        """
        
        query = """
            SELECT * FROM schema_migrations 
            WHERE status = 'completed' AND rolled_back_at IS NULL
            ORDER BY executed_at DESC, version DESC
            LIMIT 1
        """
        
        try:
            results = self.db.execute_select_query(query)
            if results:
                return TrackSchemaMigrations.from_dict(results[0])
            return None
            
        except Exception as general_error:
            self.logger.error(f"Error getting the latest migration: {general_error}")
            return None
    
    def get_migration_statistics(self) -> Dict[str, Any]:
        """
        Gets statistics about migrations.
        
        Returns:
            Dictionary containing stats
        """
        
        stats_query = """
            SELECT 
                status,
                COUNT(*) as migration_count,
                AVG(execution_time_seconds) as avg_execution_time,
                MIN(execution_time_seconds) as min_execution_time,
                MAX(execution_time_seconds) as max_execution_time,
                SUM(execution_time_seconds) as total_execution_time
            FROM schema_migrations
            GROUP BY status
            ORDER BY migration_count DESC
        """
        
        timeline_query = """
            SELECT 
                DATE(executed_at) as execution_date,
                COUNT(*) as migrations_per_day
            FROM schema_migrations
            WHERE executed_at >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY DATE(executed_at)
            ORDER BY execution_date DESC
        """
        
        try:
            status_stats = self.db.execute_select_query(stats_query)
            timeline_stats = self.db.execute_select_query(timeline_query)
            
            total_migrations = sum(row['migration_count'] for row in status_stats)
            
            stats = {
                'total_migrations': total_migrations,
                'by_status': status_stats,
                'recent_timeline': timeline_stats,
                'summary': {}
            }
            
            for status_row in status_stats:
                status_name = status_row['status']
                count = status_row['migration_count']
                percentage = round((count / total_migrations * 100), 2) if total_migrations > 0 else 0
                
                stats['summary'][status_name] = {
                    'count': count,
                    'percentage': percentage,
                    'avg_execution_time': float(status_row['avg_execution_time']) if status_row['avg_execution_time'] else 0
                }
            
            return stats
            
        except Exception as general_error:
            self.logger.error(f"Error getting migration statistics: {general_error}")
            return {'total_migrations': 0, 'by_status': [], 'recent_timeline': [], 'summary': {}}
    
    def check_migration_exists(self, version_to_check: str) -> bool:
        """
        Checks if a migration version has been applied.
            
        Returns:
            True if migration exists and is completed, False if otherwise
        """
        query = """
            SELECT 1 FROM schema_migrations 
            WHERE version = %s AND status = 'completed' AND rolled_back_at IS NULL
        """
        
        try:
            results = self.db.execute_select_query(query, (version_to_check,))
            return len(results) > 0
            
        except Exception as general_error:
            self.logger.error(f"Error checking if migration {version_to_check} exists: {general_error}")
            return False
    
    def delete_migration_record(self, version_to_delete: str) -> bool:
        """
        Deletes a migration record
        
        Returns:
            True if successful and vice versa
        """
        
        query = """
            DELETE FROM schema_migrations WHERE version = %s
        """
        
        try:
            affected_rows = self.db.execute_update_delete_query(query, (version_to_delete,))
            
            if affected_rows > 0:
                self.logger.info(f"Deleted migration record for version {version_to_delete}")
                return True
            else:
                self.logger.warning(f"No migration record found with version {version_to_delete} to delete")
                return False
                
        except Exception as general_error:
            self.logger.error(f"Error deleting migration record for version {version_to_delete}: {general_error}")
            return False
    
    def get_migrations_by_date_range(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> List[TrackSchemaMigrations]:
        """
        Gets migrations executed within a date range.
        """
        
        query = """
            SELECT * FROM schema_migrations 
            WHERE executed_at >= %s AND executed_at <= %s
            ORDER BY executed_at ASC
        """
        
        try:
            results = self.db.execute_select_query(query, (start_date, end_date))
            return [TrackSchemaMigrations.from_dict(row) for row in results]
            
        except Exception as general_error:
            self.logger.error(f"Error getting migrations by date range: {general_error}")
            return []
    
    def get_slowest_migrations(self, limit: int = 10) -> List[TrackSchemaMigrations]:
        """
        Gets the slowest migrations by execution time.
        """
        
        query = """
            SELECT * FROM schema_migrations 
            WHERE status = 'completed'
            ORDER BY execution_time_seconds DESC
            LIMIT %s
        """
        
        try:
            results = self.db.execute_select_query(query, (limit,))
            return [TrackSchemaMigrations.from_dict(row) for row in results]
            
        except Exception as general_error:
            self.logger.error(f"Error getting slowest migrations: {general_error}")
            return []