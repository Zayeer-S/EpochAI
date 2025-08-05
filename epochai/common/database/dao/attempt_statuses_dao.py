from datetime import datetime
from typing import List, Optional

from epochai.common.database.database import get_database
from epochai.common.database.models import AttemptStatuses
from epochai.common.logging_config import get_logger


class AttemptStatusesDAO:
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)

    def create_attempt_status(self, attempt_status_name: str) -> Optional[int]:
        """Create a new attempt status"""

        query = """
            INSERT INTO attempt_statuses (attempt_status_name, created_at, updated_at)
            VALUES (%s, %s, %s)
            RETURNING id
        """

        try:
            current_timestamp = datetime.now()
            params = (attempt_status_name, current_timestamp, current_timestamp)
            result = self.db.execute_insert_query(query, params)

            if result:
                self.logger.info(f"Created attempt status: '{attempt_status_name}'")
                return result
            self.logger.error(f"Failed to create attempt status: '{attempt_status_name}'")
            return None

        except Exception as general_error:
            self.logger.error(f"Error creating attempt status '{attempt_status_name}': {general_error}")
            return None

    def get_by_id(self, status_id: int) -> Optional[AttemptStatuses]:
        """Gets attempt status by ID"""

        query = """
            SELECT * FROM attempt_statuses WHERE id = %s
        """

        try:
            results = self.db.execute_select_query(query, (status_id,))
            if results:
                return AttemptStatuses.from_dict(results[0])
            return None

        except Exception as general_error:
            self.logger.error(f"Error getting attempt status by ID {status_id}: {general_error}")
            return None

    def get_by_name(
        self,
        attempt_status_name: str,
    ) -> Optional[AttemptStatuses]:
        """Gets attempt status by name"""

        query = """
            SELECT * FROM attempt_statuses WHERE attempt_status_name = %s
        """

        try:
            results = self.db.execute_select_query(query, (attempt_status_name,))
            if results:
                return AttemptStatuses.from_dict(results[0])
            return None

        except Exception as general_error:
            self.logger.error(
                f"Error getting attempt status by name '{attempt_status_name}': {general_error}",
            )
            return None

    def get_all(self) -> List[AttemptStatuses]:
        """Gets all attempt statuses"""

        query = """
            SELECT * FROM attempt_statuses ORDER BY attempt_status_name
        """

        try:
            results = self.db.execute_select_query(query)
            return [AttemptStatuses.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting all attempt statuses: {general_error}")
            return []

    def update_status_name(
        self,
        status_id: int,
        new_name: str,
    ) -> bool:
        """Update attempt status name"""

        query = """
            UPDATE attempt_statuses SET attempt_status_name = %s, updated_at = %s WHERE id = %s
        """

        try:
            params = (new_name, datetime.now(), status_id)
            affected_rows = self.db.execute_update_delete_query(query, params)

            if affected_rows > 0:
                self.logger.info(f"Updated attempt status {status_id} to '{new_name}'")
                return True
            self.logger.warning(f"No attempt status found with ID {status_id}")
            return False

        except Exception as general_error:
            self.logger.error(f"Error upating attempt status {status_id}: {general_error}")
            return False
