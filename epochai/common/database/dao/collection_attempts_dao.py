from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from epochai.common.database.database import get_database
from epochai.common.database.models import CollectionAttempts
from epochai.common.logging_config import get_logger


class CollectionAttemptsDAO:
    """DAO for collection_attempts table"""

    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)

    def create_attempt(
        self,
        collection_target_id: int,
        language_code: str,
        search_term_used: str,
        attempt_status_id: int,
        error_type_id: Optional[int] = None,
        error_message: str = "",
    ) -> Optional[int]:
        """
        Creates a new collection attempt record

        Returns:
            ID of created attempt or None if failed
        """

        query = """
            INSERT INTO collection_attempts
            (collection_target_id, language_code, search_term_used, attempt_status_id, error_type_id, error_message, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """

        try:
            current_timestamp = datetime.now()
            params = (
                collection_target_id,
                language_code,
                search_term_used,
                attempt_status_id,
                error_type_id,
                error_message,
                current_timestamp,
            )
            result = self.db.execute_insert_query(query, params)

            if result:
                self.logger.info(
                    f"Created collection attempt for config '{collection_target_id}': {search_term_used} ({language_code})",
                )
                return result
            self.logger.error(
                f"Failed to create collection attempt for config '{collection_target_id}': {search_term_used} ({language_code})",
            )
            return None

        except Exception as general_error:
            self.logger.error(
                f"Error creatiing collection attempt for config '{collection_target_id}' (search term used: {search_term_used} ({language_code}): {general_error}",  # noqa
            )
            return None

    def get_by_id(
        self,
        attempt_id: int,
    ) -> Optional[CollectionAttempts]:
        """Gets collection attempt by ID"""

        query = """
            SELECT * FROM collection_attempts WHERE id = %s
        """

        try:
            results = self.db.execute_select_query(query, (attempt_id,))
            if results:
                return CollectionAttempts.from_dict(results[0])
            return None

        except Exception as general_error:
            self.logger.error(f"Error getting collection attempt by id '{attempt_id}': {general_error}")
            return None

    def get_all(self) -> List[CollectionAttempts]:
        """Gets all collection attempts"""

        query = """
            SELECT * FROM collection_attempts ORDER BY created_at DESC
        """

        try:
            results = self.db.execute_select_query(query)
            return [CollectionAttempts.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting all collection attempts: {general_error}")
            return []

    def get_by_target_id(
        self,
        collection_target_id: int,
    ) -> List[CollectionAttempts]:
        """Gets all attempts for a specific collection target"""

        query = """
            SELECT * FROM collection_attempts WHERE collection_target_id = %s ORDER BY created_at DESC
        """

        try:
            results = self.db.execute_select_query(query, (collection_target_id,))
            return [CollectionAttempts.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting attempts for target '{collection_target_id}': {general_error}")
            return []

    def get_by_status(
        self,
        attempt_status_name: str,
    ) -> List[CollectionAttempts]:
        """Gets attempts by status name"""

        query = """
            SELECT ca.*
            FROM collection_attempts ca
            JOIN attempt_statuses ast ON ca.attempt_status_id = ast.id
            WHERE ast.attempt_status_name = %s
            ORDER BY ca.created_at DESC
        """

        try:
            results = self.db.execute_select_query(query, (attempt_status_name,))
            attempts = [CollectionAttempts.from_dict(row) for row in results]

            self.logger.info(f"Found {len(attempts)} attempts with status '{attempt_status_name}'")
            return attempts

        except Exception as general_error:
            self.logger.error(f"Error getting attempts by status '{attempt_status_name}': {general_error}")
            return []

    def get_failed_attempts(self) -> List[CollectionAttempts]:
        """Gets all failed collection attempts by calling get_by_status"""
        return self.get_by_status("failed")

    def get_successful_attempts(self) -> List[CollectionAttempts]:
        """Gets all successful collection attempts by calling get_by_status"""
        return self.get_by_status("success")

    def get_latest_attempt_for_config(
        self,
        collection_target_id: int,
    ) -> Optional[CollectionAttempts]:
        """Gets the latest attempt for a specific config"""

        query = """
            SELECT * FROM collection_attempts
            WHERE collection_target_id = %s
            ORDER BY created_at DESC
            LIMIT 1
        """

        try:
            results = self.db.execute_select_query(query, (collection_target_id,))
            if results:
                return CollectionAttempts.from_dict(results[0])
            return None

        except Exception as general_error:
            self.logger.error(
                f"Error getting latest attempt for config '{collection_target_id}': {general_error}",
            )
            return None

    def get_attempts_by_error_type(
        self,
        error_type_name: str,
    ) -> List[CollectionAttempts]:
        """Gets attempts by error type name"""

        query = """
            SELECT ca.*
            FROM collection_attempts ca
            JOIN error_types et ON ca.error_type_id = et.id
            WHERE et.error_type_name = %s
            ORDER BY ca.created_at DESC
        """

        try:
            results = self.db.execute_select_query(query, (error_type_name,))
            attempts = [CollectionAttempts.from_dict(row) for row in results]

            self.logger.info(f"Found {len(attempts)} attempts with error type '{error_type_name}'")
            return attempts

        except Exception as general_error:
            self.logger.error(f"Error getting attempts by error type '{error_type_name}': {general_error}")
            return []

    def get_attempts_with_details(
        self,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Gets attempts with full details"""

        query = """
            SELECT
                ca.*,
                ast.attempt_status_name,
                et.error_type_name,
                cfg.collection_name,
                ct.collection_type
            FROM collection_attempts ca
            LEFT JOIN attempt_statuses ast ON ca.attempt_status_id = ast.id
            LEFT JOIN error_types et ON ca.error_type_id = et.id
            LEFT JOIN collection_targets cfg ON ca.collection_target_id = cfg.id
            LEFT JOIN collection_types ct ON cfg.collection_type_id = ct.id
            ORDER BY ca.created_at DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        try:
            results = self.db.execute_select_query(query)
            self.logger.info(f"Retrieved {len(results)} attempts with full details")
            return results

        except Exception as general_error:
            self.logger.error(f"Error getting attempts with full details: {general_error}")
            return []

    def get_failed_configs_for_retry(self) -> List[Dict[str, Any]]:
        """Gets collection targets that have only failed attempts and need retry"""

        query = """
            SELECT DISTINCT
                    cfg.id as config_id,
                    cfg.collection_name,
                    cfg.language_code,
                    ct.collection_type,
                    ca.error_message,
                    et.error_type_name,
                    ca.created_at as last_attempt_at
                FROM collection_targets cfg
                JOIN collection_types ct ON cfg.collection_type_id = ct.id
                JOIN collection_attempts ca ON cfg.id = ca.collection_target_id
                JOIN error_types et ON ca.error_type_id = et.id
                WHERE cfg.is_collected = false
                AND ca.id = (
                    SELECT MAX(ca2.id)
                    FROM collection_attempts ca2
                    WHERE ca2.collection_target_id = cfg.id
                )
                AND ca.attempt_status_id = (
                    SELECT id FROM attempt_statuses WHERE attempt_status_name = 'failed'
                )
        """

        try:
            results = self.db.execute_select_query(query)
            self.logger.info(f"Found {len(results)} configs that failed and need retry")
            return results

        except Exception as general_error:
            self.logger.error(f"Error getting failed configs for retry: {general_error}")
            return []

    def get_attempt_statistics(self) -> Dict[str, Any]:
        """Gets comprehensive stats about data collection attempts"""

        query = """
            SELECT
                ast.attempt_status_name,
                COUNT(*) as attempt_count,
                COUNT(DISTINCT ca.collection_target_id) as unique_configs
            FROM collection_attempts ca
            JOIN attempt_statuses ast ON ca.attempt_status_id = ast.id
            GROUP BY ast.attempt_status_name
            ORDER BY attempt_count DESC
        """

        error_state_query = """
            SELECT
                et.error_type_name,
                COUNT(*) as error_count
            FROM collection_attempts ca
            JOIN error_types et ON ca.error_type_id = et.id
            WHERE ca.attempt_status_id = (
                SELECT id FROM attempt_statuses WHERE attempt_status_name = 'failed'
            )
            GROUP BY et.error_type_name
            ORDER BY error_count DESC
        """

        try:
            status_stats = self.db.execute_select_query(query)
            error_stats = self.db.execute_select_query(error_state_query)

            total_attempts = sum(row["attempt_count"] for row in status_stats)

            stats = {
                "total_attempts": total_attempts,
                "by_status": status_stats,
                "by_error_type": error_stats,
                "summary": {},
            }

            for status_row in status_stats:
                status_name = status_row["attempt_status_name"]
                count = status_row["attempt_count"]
                percentage = round((count / total_attempts * 100), 2) if total_attempts > 0 else 0

                stats["summary"][status_name] = {
                    "count": count,
                    "percentage": percentage,
                }

            return stats

        except Exception as general_error:
            self.logger.error(f"Error getting attempt statistics: {general_error}")
            return {"total_attempts": 0, "by_status": [], "by_error_type": [], "summary": {}}

    def delete_attempts_for_config(
        self,
        collection_target_id: int,
    ) -> int:
        """Deletes all attempts for a specific config"""

        query = """
            DELETE FROM collection_attempts WHERE collection_target_id = %s
        """

        try:
            affected_rows = self.db.execute_update_delete_query(query, (collection_target_id,))

            if affected_rows > 0:
                self.logger.info(f"Deleted {affected_rows} attempts for config {collection_target_id}")
            else:
                self.logger.warning(f"No attempts found for config {collection_target_id} to delete")

            return affected_rows

        except Exception as general_error:
            self.logger.error(f"Error deleting attempts for config {collection_target_id}: {general_error}")
            return 0

    def delete_old_attempts(
        self,
        days_old: int,
    ) -> int:
        """Deletes attempts older than specified days"""

        query = """
            DELETE FROM collection_attempts
            WHERE created_at < %s
        """

        try:
            cutoff_date = datetime.now() - timedelta(days=days_old)
            affected_rows = self.db.execute_update_delete_query(query, (cutoff_date,))

            if affected_rows > 0:
                self.logger.info(f"Deleted {affected_rows} attempts older than {days_old} days")

            return affected_rows

        except Exception as general_error:
            self.logger.error(f"Error deleting old attempts: {general_error}")
            return 0

    def search_by_term(
        self,
        search_term_used: str,
    ) -> List[CollectionAttempts]:
        """Search attempts by search term used"""

        query = """
            SELECT * FROM collection_attempts WHERE search_term_used ILIKE %s ORDER BY created_at DESC
        """

        try:
            search_pattern = f"%{search_term_used}%"
            results = self.db.execute_select_query(query, (search_pattern,))
            return [CollectionAttempts.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error searching attempts by term '{search_term_used}': {general_error}")
            return []

    def get_recent_attempts(
        self,
        hours: int = 24,
    ) -> List[CollectionAttempts]:
        """Gets attempts from the last X hours"""

        query = """
            SELECT * FROM collection_attempts
            WHERE created_at >= %s
            ORDER BY created_at DESC
        """

        try:
            cutoff_time = datetime.now() - timedelta(hours=hours)
            results = self.db.execute_select_query(query, (cutoff_time,))
            return [CollectionAttempts.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting recent attempts from last {hours} hours: {general_error}")
            return []
