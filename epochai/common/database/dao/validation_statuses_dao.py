from datetime import datetime
from typing import List, Optional

from epochai.common.database.database import get_database
from epochai.common.database.models import ValidationStatuses
from epochai.common.logging_config import get_logger


class ValidationStatusesDAO:
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)

    def create_validation_status(
        self,
        validation_status_name: str,
    ) -> Optional[int]:
        """Creates a new validation status"""

        query = """
            INSERT INTO validation_statuses (validation_status_name, created_at, updated_at)
            VALUES (%s, %s, %s)
            RETURNING id
        """

        try:
            current_timestamp = datetime.now()
            params = (validation_status_name, current_timestamp, current_timestamp)
            result = self.db.execute_insert_query(query, params)

            if result:
                self.logger.info(f"Created validation status: '{validation_status_name}'")
                return result
            self.logger.error(f"Failed to create validation status: '{validation_status_name}'")
            return None

        except Exception as general_error:
            self.logger.error(f"Error creating validation status '{validation_status_name}': {general_error}")
            return None

    def get_by_id(
        self,
        status_id: int,
    ) -> Optional[ValidationStatuses]:
        """Gets validation status by ID"""

        query = """
            SELECT * FROM validation_statuses WHERE id = %s
        """

        try:
            results = self.db.execute_select_query(query, (status_id,))
            if results:
                return ValidationStatuses.from_dict(results[0])
            return None

        except Exception as general_error:
            self.logger.error(f"Error getting validation status by ID {status_id}: {general_error}")
            return None

    def get_by_name(
        self,
        validation_status_name: str,
    ) -> Optional[ValidationStatuses]:
        """Gets validation status by name"""

        query = """
            SELECT * FROM validation_statuses WHERE validation_status_name = %s
        """

        try:
            results = self.db.execute_select_query(query, (validation_status_name,))
            if results:
                return ValidationStatuses.from_dict(results[0])
            return None

        except Exception as general_error:
            self.logger.error(
                f"Error getting validation status by name '{validation_status_name}': {general_error}",
            )
            return None

    def get_all(self) -> List[ValidationStatuses]:
        """Get all validation statuses"""

        query = """
            SELECT * FROM validation_statuses ORDER BY validation_status_name
        """

        try:
            results = self.db.execute_select_query(query)
            return [ValidationStatuses.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting all validation statuses: {general_error}")
            return []
