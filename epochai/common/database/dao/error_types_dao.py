from datetime import datetime
from typing import List, Optional

from epochai.common.database.database import get_database
from epochai.common.database.models import ErrorTypes
from epochai.common.logging_config import get_logger


class ErrorTypesDAO:
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)

    def create_error_type(
        self,
        error_type_name: str,
    ) -> Optional[int]:
        """Creates a new error type"""

        query = """
            INSERT INTO error_types (error_type_name, created_at, updated_at)
            VALUES (%s, %s, %s)
            RETURNING id
        """

        try:
            current_timestamp = datetime.now()
            params = (error_type_name, current_timestamp, current_timestamp)
            result = self.db.execute_insert_query(query, params)

            if result:
                self.logger.info(f"Created error type: '{error_type_name}'")
                return result
            self.logger.error(f"Failed to create error type: '{error_type_name}'")
            return None

        except Exception as general_error:
            self.logger.error(f"Error creating error type '{error_type_name}': {general_error}")
            return None

    def get_by_id(
        self,
        error_type_id: int,
    ) -> Optional[ErrorTypes]:
        """Gets error type by ID"""

        query = """
            SELECT * FROM error_types WHERE id = %s
        """

        try:
            results = self.db.execute_select_query(query, (error_type_id,))
            if results:
                return ErrorTypes.from_dict(results[0])
            return None

        except Exception as general_error:
            self.logger.error(f"Error getting error type by ID {error_type_id}: {general_error}")
            return None

    def get_by_name(self, error_type_name: str) -> Optional[ErrorTypes]:
        """Gets error type by name"""

        query = """
            SELECT * FROM error_types WHERE error_type_name = %s
        """

        try:
            results = self.db.execute_select_query(query, (error_type_name,))
            if results:
                return ErrorTypes.from_dict(results[0])
            return None

        except Exception as general_error:
            self.logger.error(f"Error getting error type by name '{error_type_name}': {general_error}")
            return None

    def get_all(self) -> List[ErrorTypes]:
        """Gets all error types"""

        query = """
            SELECT * FROM error_types ORDER BY error_type_name
        """

        try:
            results = self.db.execute_select_query(query)
            return [ErrorTypes.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting all error types: {general_error}")
            return []
