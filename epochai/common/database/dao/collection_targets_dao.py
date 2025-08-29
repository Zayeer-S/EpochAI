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
        is_collected: bool = False,
    ) -> Optional[int]:
        """
        Creates a new collection target entry

        Returns:
            ID of created config or None if failure
        """

        query = """
            INSERT INTO collection_targets
            (collector_name_id, collection_type_id, language_code, collection_name, is_collected, updated_at, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """  # noqa

        try:
            current_timestamp = datetime.now()
            params = (
                collector_name_id,
                collection_type_id,
                language_code,
                collection_name,
                is_collected,
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
        target_id: int,
    ) -> Optional[CollectionTargets]:
        """Gets collection target by id"""

        query = """
            SELECT * FROM collection_targets WHERE id = %s
        """

        try:
            results = self.db.execute_select_query(query, (target_id,))
            if results:
                return CollectionTargets.from_dict(results[0])
            return None

        except Exception as general_error:
            self.logger.error(f"Error getting collection target by id {target_id}: {general_error}")
            return None

    def get_all(self) -> List[CollectionTargets]:
        """Gets all collection targets"""
        query = """
            Select * FROM collection_targets ORDER BY created_at DESC
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

    def get_by_collection_status(
        self,
        is_collected: bool,
    ) -> List[CollectionTargets]:
        """Gets targets by collection status"""

        query = """
            SELECT * FROM collection_targets WHERE is_collected = %s ORDER BY created_at ASC
        """

        try:
            results = self.db.execute_select_query(query, (is_collected,))
            return [CollectionTargets.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting targets by collection status '{is_collected}': {general_error}")
            return []

    def get_uncollected_by_type_and_language(
        self,
        collection_types: str,
        language_code: str,
    ) -> List[CollectionTargets]:
        """
        Gets uncollected targets by their collection type and language
        """

        query = """
            SELECT cc.*
            FROM collection_targets cc
            JOIN collection_types ct ON cc.collection_type_id = ct.id
            WHERE ct.collection_type = %s
            AND cc.language_code = %s
            AND cc.is_collected = FALSE
            ORDER BY cc.created_at ASC
        """

        try:
            results = self.db.execute_select_query(query, (collection_types, language_code))
            targets = [CollectionTargets.from_dict(row) for row in results]

            """self.logger.info(f"Found {len(targets)} uncollected in type '{collection_types}' for language code '{language_code}'")"""  # noqa
            return targets

        except Exception as general_error:
            self.logger.error(
                f"Error getting uncollected {collection_types} in type '{collection_types}' targets for language code '{language_code}': {general_error}",  # noqa
            )
            return []

    def get_uncollected_language_codes_by_collector_name(
        self,
        collector_name: str,
        unique_languages_only: bool,
    ) -> List[CollectionTargets]:
        """Gets uncollected targets by their language code"""

        if unique_languages_only:
            query = """
                SELECT DISTINCT ON (cc.language_code) cc.*
                FROM collection_targets
                JOIN collector_names cn ON cc.collector_name_id = cn.id
                WHERE cn.collector_name = %s
                AND cc.is_collected = FALSE
                ORDER BY cc.language_code, cc.created_at ASC
            """
        else:
            query = """
                SELECT cc.*
                FROM collection_targets cc
                JOIN collector_names cn ON cc.collector_name_id = cn.id
                WHERE cn.collector_name = %s
                AND cc.is_collected = FALSE
                ORDER BY cc.language_code, cc.created_at ASC
            """

        try:
            results = self.db.execute_select_query(query, (collector_name,))
            return [CollectionTargets.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(
                f"Error getting uncollected targets for collector '{collector_name}': {general_error}",
            )
            return []

    def get_uncollected_by_type(
        self,
        collection_type: str,
    ) -> List[CollectionTargets]:
        """Gets all uncollected targets by type across all languages"""

        query = """
            SELECT cc.*
            FROM collection_targets cc
            JOIN collection_types ct ON cc.collection_type_id = ct.id
            WHERE ct.collection_type = %s
            AND cc.is_collected = false
            ORDER BY cc.language_code, cc.created_at ASC
        """

        try:
            results = self.db.execute_select_query(query, (collection_type,))
            targets = [CollectionTargets.from_dict(row) for row in results]

            self.logger.info(
                f"Found {len(targets)} uncollected {collection_type} targets across all languages",
            )
            return targets

        except Exception as general_error:
            self.logger.error(f"Error getting uncollected {collection_type} targets: {general_error}")
            return []

    def get_uncollected_grouped_by_language(
        self,
        collection_type: str,
    ) -> Dict[str, List[CollectionTargets]]:
        """
        Gets all uncolected targets grouped by language for easier processing
        """
        targets = self.get_uncollected_by_type(collection_type)
        grouped: Dict[str, List[CollectionTargets]] = {}

        for config in targets:
            if config.language_code not in grouped:
                grouped[config.language_code] = []
            grouped[config.language_code].append(config)

        return grouped

    def mark_as_collected(
        self,
        target_id: int,
    ) -> bool:
        """Marks a row as collected"""

        query = """
            UPDATE collection_targets
            SET is_collected = true, updated_at = %s
            WHERE id = %s
        """

        try:
            affected_rows = self.db.execute_update_delete_query(query, (datetime.now(), target_id))

            if affected_rows > 0:
                self.logger.info(f"Marked target {target_id} as collected")
                return True
            self.logger.warning(f"No target found with id '{target_id}' to mark as collected")
            return False

        except Exception as general_error:
            self.logger.error(f"Error marking target with id '{target_id} as collected: {general_error}'")
            return False

    def mark_as_uncollected(
        self,
        target_id: int,
    ) -> bool:
        """Marks a row as uncollected"""

        query = """
            UPDATE collection_targets
            SET is_collected = false, updated_at = %s
            WHERE id = %s
        """

        try:
            affected_rows = self.db.execute_update_delete_query(query, (datetime.now(), target_id))

            if affected_rows > 0:
                self.logger.info(f"Marked target {target_id} as uncollected")
                return True
            self.logger.warning(f"No target found with id '{target_id}' to mark as uncollected")
            return False

        except Exception as general_error:
            self.logger.error(f"Error marking target with id '{target_id} as uncollected: {general_error}'")
            return False

    def bulk_create_collection_targets(
        self,
        collection_targets: List[Tuple[int, int, str, str, bool]],
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
            (collector_name_id, collection_type_id, language_code, collection_name, is_collected, updated_at, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """  # noqa

        try:
            operations = []
            now = datetime.now()

            for config_data in collection_targets:
                (
                    collector_name_id,
                    collection_type_id,
                    language_code,
                    collection_name,
                    is_collected,
                ) = config_data
                params = (
                    collector_name_id,
                    collection_type_id,
                    language_code,
                    collection_name,
                    is_collected,
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

    def get_collection_status(self) -> Dict[str, Any]:
        """Gets statistics about collection status"""
        query = """
            SELECT
                ct.collection_type,
                cc.language_code,
                COUNT(*) as total_targets,
                SUM(CASE WHEN cc.is_collected THEN 1 ELSE 0 END) as collected_count,
                SUM(CASE WHEN NOT cc.is_collected THEN 1 ELSE 0 END) as uncollected_count
            FROM collection_targets cc
            JOIN collection_types ct ON cc.collection_type_id = ct.id
            GROUP BY ct.collection_type, cc.language_code
            ORDER BY ct.collection_type, cc.language_code
        """

        try:
            results = self.db.execute_select_query(query)

            total_targets = sum(row["total_targets"] for row in results)
            total_collected = sum(row["collected_count"] for row in results)
            total_uncollected = sum(row["uncollected_count"] for row in results)

            summary = {
                "total_targets": total_targets,
                "total_collected": total_collected,
                "total_uncollected": total_uncollected,
                "collection_percentage": round((total_collected / total_targets * 100), 2)
                if total_targets > 0
                else 0,
            }

            stats = {
                "by_type_and_language": results,
                "summary": summary,
            }

            return stats

        except Exception as general_error:
            self.logger.error(f"Error getting collection stats: {general_error}")
            return {"by_type_and_language": [], "summary": {}}

    def delete_config(
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

    def get_by_collector_and_type(
        self,
        collector_name: str,
        collection_type: str,
    ) -> List[CollectionTargets]:
        """Gets targets by collector name and collection type"""

        query = """
            SELECT cc.*
            FROM collection_targets cc
            JOIN collector_names cn ON cc.collector_name_id = cn.id
            JOIN collection_types ct ON cc.collection_type_id = ct.id
            WHERE cn.collector_name = %s AND ct.collection_type = %s
            ORDER BY cc.language_code, cc.collection_name
        """

        try:
            results = self.db.execute_select_query(query, (collector_name, collection_type))
            return [CollectionTargets.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(
                f"Error getting targets for collector '{collector_name}' and type '{collection_type}': {general_error}",  # noqa
            )
            return []

    def get_uncollected_by_collector_name(
        self,
        collector_name: str,
        unique_types_only: bool,
    ) -> List[CollectionTargets]:
        """Gets uncollected targets by collector name"""

        if unique_types_only:
            query = """
                SELECT DISTINCT ON (ct.collection_type_id) ct.*
                FROM collection_targets cc
                JOIN collector_names cn ON cc.collector_name_id = cn.id
                JOIN collection_types ct ON cc.collection_type_id = ct.id
                WHERE cn.collector_name = %s
                AND cc.is_collected = FALSE
                ORDER BY ct.collection_type_id, cc.created_at ASC
            """
        else:
            query = """
                SELECT cc.*
                FROM collection_targets cc
                JOIN collector_names cn ON cc.collector_name_id = cn.id
                WHERE cn.collector_name = %s
                AND cc.is_collected = FALSE
                ORDER BY cc.language_code, cc.created_at ASC
            """

        try:
            results = self.db.execute_select_query(query, (collector_name,))
            return [CollectionTargets.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(
                f"Error getting uncollected targets for collector '{collector_name}': {general_error}",
            )
            return []
