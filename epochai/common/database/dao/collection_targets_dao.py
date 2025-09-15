from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from epochai.common.database.database import get_database
from epochai.common.database.models import CollectionTargets
from epochai.common.logging_config import get_logger


class CollectionTargetsDAO:
    """DAO for collection_targets table"""

    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)

    def create_collection_target(
        self,
        collector_name_id: int,
        collection_type_id: int,
        language_code: str,
        collection_name: str,
        collection_status_id: int,
    ) -> Optional[int]:
        """
        Creates a new collection target entry

        Returns:
            ID of created config or None if failure
        """

        query = """
            INSERT INTO collection_targets
            (collector_name_id, collection_type_id, language_code, collection_name, collection_status_id, updated_at, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """

        try:
            current_timestamp = datetime.now()
            params = (
                collector_name_id,
                collection_type_id,
                language_code,
                collection_name,
                collection_status_id,
                current_timestamp,
                current_timestamp,
            )
            result = self.db.execute_insert_query(query, params)

            if result:
                self.logger.info(f"Created collection target: '{collection_name}' ({language_code})")
                return result
            self.logger.error(
                f"Failed to create collection target: '{collection_name}' ({language_code})",
            )
            return None

        except Exception as general_error:
            self.logger.error(
                f"Error creating collection target '{collection_name}' ({language_code}): {general_error}",
            )
            return None

    def get_by_id(
        self,
        id_list: List[int],
    ) -> Optional[List[CollectionTargets]]:
        """Gets collection target objects by their IDs"""
        query = """
            SELECT * FROM collection_targets WHERE id = ANY(%s)
        """
        if not id_list:
            self.logger.error(f"Passing in empty or None list: {id_list}")
            return None

        try:
            results = self.db.execute_select_query(query, (id_list,))
            return [CollectionTargets.from_dict(row) for row in results] if results else None

        except Exception as general_error:
            self.logger.error(f"Error getting collection target by id {id_list}: {general_error}")
            return None

    def get_all(self) -> List[CollectionTargets]:
        """Gets all collection targets"""
        query = """
            SELECT * FROM collection_targets ORDER BY created_at DESC
        """

        try:
            results = self.db.execute_select_query(query)

            if results is None:
                self.logger.warning(
                    "Database query return None in get_all() - table might be empty/not exist",
                )
                return []

            return [CollectionTargets.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting all collection targets: {general_error}")
            return []

    def get_by_collection_status_id(
        self,
        collection_status_id: int,
    ) -> List[CollectionTargets]:
        """Gets targets by collection status ID"""

        query = """
            SELECT * FROM collection_targets WHERE collection_status_id = %s ORDER BY created_at ASC
        """

        try:
            results = self.db.execute_select_query(query, (collection_status_id,))
            return [CollectionTargets.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(
                f"Error getting targets by collection status ID '{collection_status_id}': {general_error}",
            )
            return []

    def get_by_type_and_language(
        self,
        collection_type_id: int,
        language_code: str,
        collection_status_id: Optional[int] = None,
    ) -> List[CollectionTargets]:
        """
        Gets targets by collection type and language, optionally filtered by status
        """
        params: Any
        if collection_status_id is not None:
            query = """
                SELECT * FROM collection_targets
                WHERE collection_type_id = %s
                AND language_code = %s
                AND collection_status_id = %s
                ORDER BY created_at ASC
            """
            params = (collection_type_id, language_code, collection_status_id)
        else:
            query = """
                SELECT * FROM collection_targets
                WHERE collection_type_id = %s
                AND language_code = %s
                ORDER BY created_at ASC
            """
            params = (collection_type_id, language_code)

        try:
            results = self.db.execute_select_query(query, params)
            return [CollectionTargets.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(
                f"Error getting targets for type ID {collection_type_id} and language '{language_code}': {general_error}",
            )
            return []

    def get_by_collector_name_id(
        self,
        collector_name_id: int,
        collection_status_id: Optional[int] = None,
        unique_languages_only: bool = False,
    ) -> List[CollectionTargets]:
        """Gets targets by collector name ID, optionally filtered by status"""
        params: Any
        if unique_languages_only:
            if collection_status_id is not None:
                query = """
                    SELECT DISTINCT ON (language_code) *
                    FROM collection_targets
                    WHERE collector_name_id = %s
                    AND collection_status_id = %s
                    ORDER BY language_code, created_at ASC
                """
                params = (collector_name_id, collection_status_id)
            else:
                query = """
                    SELECT DISTINCT ON (language_code) *
                    FROM collection_targets
                    WHERE collector_name_id = %s
                    ORDER BY language_code, created_at ASC
                """
                params = (collector_name_id,)
        else:
            if collection_status_id is not None:
                query = """
                    SELECT * FROM collection_targets
                    WHERE collector_name_id = %s
                    AND collection_status_id = %s
                    ORDER BY language_code, created_at ASC
                """
                params = (collector_name_id, collection_status_id)
            else:
                query = """
                    SELECT * FROM collection_targets
                    WHERE collector_name_id = %s
                    ORDER BY language_code, created_at ASC
                """
                params = (collector_name_id,)

        try:
            results = self.db.execute_select_query(query, params)
            return [CollectionTargets.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(
                f"Error getting targets for collector name ID {collector_name_id}: {general_error}",
            )
            return []

    def get_by_collection_type_id(
        self,
        collection_type_id: int,
        collection_status_id: Optional[int] = None,
    ) -> List[CollectionTargets]:
        """Gets targets by collection type ID, optionally filtered by status"""
        params: Any
        if collection_status_id is not None:
            query = """
                SELECT * FROM collection_targets
                WHERE collection_type_id = %s
                AND collection_status_id = %s
                ORDER BY language_code, created_at ASC
            """
            params = (collection_type_id, collection_status_id)
        else:
            query = """
                SELECT * FROM collection_targets
                WHERE collection_type_id = %s
                ORDER BY language_code, created_at ASC
            """
            params = (collection_type_id,)

        try:
            results = self.db.execute_select_query(query, params)
            targets = [CollectionTargets.from_dict(row) for row in results]

            self.logger.info(
                f"Found {len(targets)} targets for collection type ID {collection_type_id}",
            )
            return targets

        except Exception as general_error:
            self.logger.error(
                f"Error getting targets for collection type ID {collection_type_id}: {general_error}",
            )
            return []

    def get_grouped_by_language(
        self,
        collection_type_id: int,
        collection_status_id: Optional[int] = None,
    ) -> Dict[str, List[CollectionTargets]]:
        """
        Gets targets grouped by language for easier processing
        """
        targets = self.get_by_collection_type_id(collection_type_id, collection_status_id)
        grouped: Dict[str, List[CollectionTargets]] = {}

        for config in targets:
            if config.language_code not in grouped:
                grouped[config.language_code] = []
            grouped[config.language_code].append(config)

        return grouped

    def update_collection_status_id(
        self,
        target_id: int,
        collection_status_id: int,
    ) -> bool:
        """Updates the collection status of a target by status ID"""

        query = """
            UPDATE collection_targets
            SET collection_status_id = %s,
            updated_at = %s
            WHERE id = %s
        """

        try:
            affected_rows = self.db.execute_update_delete_query(
                query,
                (collection_status_id, datetime.now(), target_id),
            )

            if affected_rows > 0:
                self.logger.info(f"Updated target {target_id} status to ID {collection_status_id}")
                return True
            self.logger.warning(f"No target found with id '{target_id}' to update status")
            return False

        except Exception as general_error:
            self.logger.error(
                f"Error updating target {target_id} status to ID {collection_status_id}: {general_error}",
            )
            return False

    def bulk_create_collection_targets(
        self,
        collection_targets: List[Tuple[int, int, str, str, int]],
    ) -> int:
        """
        Bulk creates multiple targets

        Returns:
            Number of successfully created targets
        """

        if not collection_targets:
            return 0

        query = """
            INSERT INTO collection_targets
            (collector_name_id, collection_type_id, language_code, collection_name, collection_status_id, updated_at, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        try:
            operations = []
            now = datetime.now()

            for config_data in collection_targets:
                (
                    collector_name_id,
                    collection_type_id,
                    language_code,
                    collection_name,
                    collection_status_id,
                ) = config_data
                params = (
                    collector_name_id,
                    collection_type_id,
                    language_code,
                    collection_name,
                    collection_status_id,
                    now,
                    now,
                )
                operations.append((query, params))

            success = self.db.execute_transaction(operations)

            if success:
                self.logger.info(f"Successfully bulk created {len(collection_targets)} collection targets")
                return len(collection_targets)
            self.logger.error("Failed to bulk create collection targets")
            return 0

        except Exception as general_error:
            self.logger.error(f"Error bulk creating target: {general_error}")
            return 0

    def delete_target(
        self,
        target_id: int,
    ) -> bool:
        """Deletes a collection target"""

        query = """
            DELETE FROM collection_targets WHERE id = %s
        """

        try:
            affected_rows = self.db.execute_update_delete_query(query, (target_id,))

            if affected_rows > 0:
                self.logger.info(f"Deleted collection target {target_id}")
                return True
            self.logger.warning(f"No target found with id {target_id} to delete")
            return False

        except Exception as general_error:
            self.logger.error(f"Error deleting target {target_id}: {general_error}")
            return False

    def search_by_name(
        self,
        search_term: str,
    ) -> List[CollectionTargets]:
        """Search targets by collection name"""

        query = """
            SELECT * FROM collection_targets WHERE collection_name ILIKE %s ORDER BY collection_name
        """

        try:
            search_pattern = f"%{search_term}%"
            results = self.db.execute_select_query(query, (search_pattern,))
            return [CollectionTargets.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error searching targets by name '{search_term}': {general_error}")
            return []

    def get_by_collector_and_type_ids(
        self,
        collector_name_id: int,
        collection_type_id: int,
    ) -> List[CollectionTargets]:
        """Gets targets by collector name ID and collection type ID"""

        query = """
            SELECT * FROM collection_targets
            WHERE collector_name_id = %s AND collection_type_id = %s
            ORDER BY language_code, collection_name
        """

        try:
            results = self.db.execute_select_query(query, (collector_name_id, collection_type_id))
            return [CollectionTargets.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(
                f"Error getting targets for collector ID {collector_name_id} and type ID {collection_type_id}: {general_error}",
            )
            return []
