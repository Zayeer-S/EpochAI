from datetime import datetime
from typing import List, Optional

from epochai.common.database.database import get_database
from epochai.common.database.models import CollectionStatuses
from epochai.common.logging_config import get_logger


class CollectionStatusesDAO:
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)

    def create_collection_status(
        self,
        collection_status_name: str,
    ) -> Optional[int]:
        """Creates a new collection status"""

        query = """
            INSERT INTO collection_statuses (collection_status_name, created_at, updated_at)
            VALUES (%s, %s, %s)
            RETURNING id
        """

        try:
            current_timestamp = datetime.now()
            params = (collection_status_name, current_timestamp, current_timestamp)
            result = self.db.execute_insert_query(query, params)

            if result:
                self.logger.info(f"Created collection status: '{collection_status_name}'")
                return result
            self.logger.error(f"Failed to create collection status: '{collection_status_name}'")
            return None

        except Exception as general_error:
            self.logger.error(f"Error creating collection status '{collection_status_name}': {general_error}")
            return None

    def get_by_id(
        self,
        status_id: int,
    ) -> Optional[CollectionStatuses]:
        """Gets collection status by ID"""

        query = """
            SELECT * FROM collection_statuses WHERE id = %s
        """

        try:
            results = self.db.execute_select_query(query, (status_id,))
            if results:
                return CollectionStatuses.from_dict(results[0])
            return None

        except Exception as general_error:
            self.logger.error(f"Error getting collection status by ID {status_id}: {general_error}")
            return None

    def get_collection_status_by_name(
        self,
        collection_status_name: str,
    ) -> Optional[CollectionStatuses]:
        """Gets collection status by name"""

        query = """
            SELECT * FROM collection_statuses WHERE collection_status_name = %s
        """

        try:
            results = self.db.execute_select_query(query, (collection_status_name,))
            if results:
                return CollectionStatuses.from_dict(results[0])
            return None

        except Exception as general_error:
            self.logger.error(
                f"Error getting collection status by name '{collection_status_name}': {general_error}",
            )
            return None

    def get_id_by_name(
        self,
        collection_status_name: str,
    ) -> Optional[int]:
        """Gets collection status ID by name"""

        query = """
            SELECT id FROM collection_statuses WHERE collection_status_name = %s
        """

        try:
            results = self.db.execute_select_query(query, (collection_status_name,))
            if results:
                tmp: int = results[0]["id"]
                return tmp
            return None

        except Exception as general_error:
            self.logger.error(
                f"Error getting collection status ID for '{collection_status_name}': {general_error}",
            )
            return None

    def get_all(self) -> List[CollectionStatuses]:
        """Gets all collection statuses"""

        query = """
            SELECT * FROM collection_statuses ORDER BY collection_status_name ASC
        """

        try:
            results = self.db.execute_select_query(query)
            return [CollectionStatuses.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting all collection statuses: {general_error}")
            return []

    def update_collection_status(
        self,
        status_id: int,
        collection_status_name: str,
    ) -> bool:
        """Updates an existing collection status"""

        query = """
            UPDATE collection_statuses
            SET collection_status_name = %s, updated_at = %s
            WHERE id = %s
        """

        try:
            params = (collection_status_name, datetime.now(), status_id)
            affected_rows = self.db.execute_update_delete_query(query, params)

            if affected_rows > 0:
                self.logger.info(f"Updated collection status {status_id} to '{collection_status_name}'")
                return True
            self.logger.warning(f"No collection status found with ID {status_id}")
            return False

        except Exception as general_error:
            self.logger.error(f"Error updating collection status {status_id}: {general_error}")
            return False

    def delete_collection_status(
        self,
        status_id: int,
    ) -> bool:
        """Deletes a collection status"""

        query = """
            DELETE FROM collection_statuses WHERE id = %s
        """

        try:
            affected_rows = self.db.execute_update_delete_query(query, (status_id,))

            if affected_rows > 0:
                self.logger.info(f"Deleted collection status {status_id}")
                return True
            self.logger.warning(f"No collection status found with ID {status_id}")
            return False

        except Exception as general_error:
            self.logger.error(f"Error deleting collection status {status_id}: {general_error}")
            return False

    def status_exists(
        self,
        collection_status_name: str,
    ) -> bool:
        """Checks if a collection status exists by name"""

        query = """
            SELECT COUNT(*) as count FROM collection_statuses WHERE collection_status_name = %s
        """

        try:
            results = self.db.execute_select_query(query, (collection_status_name,))
            return int(results[0]["count"]) > 0 if results else False

        except Exception as general_error:
            self.logger.error(
                f"Error checking if collection status '{collection_status_name}' exists: {general_error}",
            )
            return False

    def get_or_create_status(
        self,
        collection_status_name: str,
    ) -> Optional[CollectionStatuses]:
        """Gets existing status or creates new one if it doesn't exist"""

        existing = self.get_collection_status_by_name(collection_status_name)
        if existing:
            return existing

        new_id = self.create_collection_status(collection_status_name)
        if new_id:
            return self.get_by_id(new_id)
        return None

    def get_status_usage_count(
        self,
        status_id: int,
    ) -> int:
        """Gets the number of collection targets using this status"""

        query = """
            SELECT COUNT(*) as count FROM collection_targets WHERE collection_status_id = %s
        """

        try:
            results = self.db.execute_select_query(query, (status_id,))
            return int(results[0]["count"]) if results else 0

        except Exception as general_error:
            self.logger.error(f"Error getting status usage count for status {status_id}: {general_error}")
            return 0
