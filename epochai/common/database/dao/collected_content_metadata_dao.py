from typing import Dict, List, Optional

from epochai.common.database.database import get_database
from epochai.common.database.models import CollectedContentMetadata
from epochai.common.logging_config import get_logger


class CollectedContentMetadataDAO:
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)

    def create_metadata(
        self,
        collected_content_id: int,
        metadata_key: str,
        metadata_value: str,
    ) -> Optional[int]:
        """Create a new metadata entry for collected content"""

        query = """
            INSERT INTO collected_content_metadata
            (collected_content_id, metadata_key, metadata_value)
            VALUES (%s, %s, %s)
            RETURNING id
        """

        try:
            params = (collected_content_id, metadata_key, metadata_value)
            result = self.db.execute_insert_query(query, params)

            if result:
                self.logger.info(f"Created metadata '{metadata_key}' for content {collected_content_id}")
                return result
            self.logger.error(
                f"Failed to create metadata '{metadata_key}' for content {collected_content_id}",
            )
            return None

        except Exception as general_error:
            self.logger.error(
                f"Error creating metadata '{metadata_key}' for content {collected_content_id}: {general_error}",  # noqa
            )
            return None

    def bulk_create_metadata(
        self,
        collected_content_id: int,
        metadata_dict: Dict[str, str],
    ) -> int:
        """Bulk create multiple metadata entries for one content"""

        if not metadata_dict:
            return 0

        query = """
            INSERT INTO collected_content_metadata
            (collected_content_id, metadata_key, metadata_value)
            VALUES (%s, %s, %s)
        """

        try:
            operations = []
            for key, value in metadata_dict.items():
                params = (collected_content_id, key, str(value))
                operations.append((query, params))

            success = self.db.execute_transaction(operations)

            if success:
                self.logger.info(
                    f"Bulk created {len(metadata_dict)} metadata entries for content {collected_content_id}",
                )
                return len(metadata_dict)
            self.logger.error(f"Failed to bulk create metadata for content '{collected_content_id}'")
            return 0

        except Exception as general_error:
            self.logger.error(
                f"Error bulk creating metadata for content {collected_content_id}: {general_error}",
            )
            return 0

    def get_by_content_id(
        self,
        collected_content_id: int,
    ) -> List[CollectedContentMetadata]:
        """Gets all metadata for a specific content"""

        query = """
            SELECT * FROM collected_content_metadata WHERE collected_content_id = %s ORDER BY metadata_key
        """

        try:
            results = self.db.execute_select_query(query, (collected_content_id,))
            return [CollectedContentMetadata.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting metadata for content {collected_content_id}: {general_error}")
            return []

    def get_metadata_as_dict(
        self,
        collected_content_id: int,
    ) -> Dict[str, str]:
        """Gets metadata for a content as a convenient dictionary"""
        metadata_list = self.get_by_content_id(collected_content_id)
        return {item.metadata_key: item.metadata_value for item in metadata_list}

    def search_by_key(
        self,
        metadata_key: str,
    ) -> List[CollectedContentMetadata]:
        """Find all metadata entries with a specific key"""

        query = """
            SELECT * FROM collected_content_metadata WHERE metadata_key = %s ORDER BY collected_content_id
        """

        try:
            results = self.db.execute_select_query(query, (metadata_key,))
            return [CollectedContentMetadata.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error searching metadata by key '{metadata_key}': {general_error}")
            return []

    def search_by_value(
        self,
        metadata_value: str,
    ) -> List[CollectedContentMetadata]:
        """Find all metadata entries with a specific value (partial match)"""

        query = """
            SELECT * FROM collected_content_metadata
            WHERE metadata_value ILIKE %s
            ORDER BY collected_content_id
        """

        try:
            search_pattern = f"%{metadata_value}%"
            results = self.db.execute_select_query(query, (search_pattern,))
            return [CollectedContentMetadata.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error searching metadata by value '{metadata_value}': {general_error}")
            return []

    def delete_by_content_id(
        self,
        collected_content_id: int,
    ) -> int:
        """Delete all metadata for a specific content"""

        query = """
            DELETE FROM collected_content_metadata WHERE collected_content_id = %s
        """

        try:
            affected_rows = self.db.execute_update_delete_query(query, (collected_content_id,))

            if affected_rows > 0:
                self.logger.info(
                    f"Deleted {affected_rows} metadata entries for content {collected_content_id}",
                )

            return affected_rows

        except Exception as general_error:
            self.logger.error(f"Error deleting metadata for content {collected_content_id}: {general_error}")
            return 0
