from datetime import datetime
from typing import List, Optional

from epochai.common.database.database import get_database
from epochai.common.database.models import CollectorNames
from epochai.common.logging_config import get_logger


class CollectorNamesDAO:
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)

    def create_collector_name(self, collector_name: str) -> Optional[int]:
        """Creates a new collector name"""

        query = """
            INSERT INTO collector_names (collector_name, created_at, updated_at)
            VALUES (%s, %s, %s)
            RETURNING id
        """

        try:
            current_timestamp = datetime.now()
            params = (collector_name, current_timestamp, current_timestamp)
            result = self.db.execute_insert_query(query, params)

            if result:
                self.logger.info(f"Created collector name: '{collector_name}'")
                return result
            self.logger.error(f"Failed to create collector name: '{collector_name}'")
            return None

        except Exception as general_error:
            self.logger.error(f"Error creating collector name '{collector_name}': {general_error}")
            return None

    def get_by_id(
        self,
        collector_id: int,
    ) -> Optional[CollectorNames]:
        """Gets collector name by ID"""
        query = """
            SELECT * FROM collector_names WHERE id = %s
        """

        try:
            results = self.db.execute_select_query(query, (collector_id,))
            if results:
                return CollectorNames.from_dict(results[0])
            return None

        except Exception as general_error:
            self.logger.error(f"Error getting collector name by ID {collector_id}: {general_error}")
            return None

    def get_by_name(
        self,
        collector_name: str,
    ) -> Optional[CollectorNames]:
        """Gets collector by name"""
        query = """
            SELECT * FROM collector_names WHERE collector_name = %s
        """

        try:
            results = self.db.execute_select_query(query, (collector_name,))
            if results:
                return CollectorNames.from_dict(results[0])
            return None

        except Exception as general_error:
            self.logger.error(f"Error getting collector by name '{collector_name}': {general_error}")
            return None

    def get_all(self) -> List[CollectorNames]:
        """Gets all collector names"""
        query = """
            SELECT * FROM collector_names ORDER BY collector_name
        """

        try:
            results = self.db.execute_select_query(query)
            return [CollectorNames.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting all collector names: {general_error}")
            return []

    def get_or_create_collector(
        self,
        collector_name: str,
    ) -> Optional[CollectorNames]:
        """Gets existing collector or create new one if doesn't exist"""
        existing = self.get_by_name(collector_name)
        if existing:
            return existing

        new_id = self.create_collector_name(collector_name)
        if new_id:
            return self.get_by_id(new_id)
        return None
