from datetime import datetime
import json
from typing import Any, Dict, List, Optional

from epochai.common.database.database import get_database
from epochai.common.database.models import CleanedDataMetadataSchemas
from epochai.common.logging_config import get_logger


class CleanedDataMetadataSchemasDAO:
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)

    def create_schema(
        self,
        metadata_schema: Dict[str, Any],
    ) -> Optional[int]:
        """Creates a new cleaned data metadata schema"""

        query = """
            INSERT INTO cleaned_data_metadata_schemas (metadata_schema, created_at, updated_at)
            VALUES (%s, %s, %s)
            RETURNING id
        """

        try:
            current_timestamp = datetime.now()
            schema_json = json.dumps(metadata_schema)
            params = (schema_json, current_timestamp, current_timestamp)
            result = self.db.execute_insert_query(query, params)

            if result:
                self.logger.info(f"Created cleaned data metadata schema with ID: {result}")
                return result
            self.logger.error("Failed to create cleaned data metadata schema")
            return None

        except Exception as general_error:
            self.logger.error(f"Error creating cleaned data metadata schema: {general_error}")
            return None

    def get_by_id(
        self,
        schema_id: int,
    ) -> Optional[CleanedDataMetadataSchemas]:
        """Gets cleaned data metadata schema by ID"""

        query = """
            SELECT * FROM cleaned_data_metadata_schemas WHERE id = %s
        """

        try:
            results = self.db.execute_select_query(query, (schema_id,))
            if results:
                return CleanedDataMetadataSchemas.from_dict(results[0])
            return None

        except Exception as general_error:
            self.logger.error(
                f"Error getting cleaned data metadata schema by ID {schema_id}: {general_error}",
            )
            return None

    def get_all(self) -> List[CleanedDataMetadataSchemas]:
        """Gets all cleaned data metadata schemas"""

        query = """
            SELECT * FROM cleaned_data_metadata_schemas ORDER BY created_at DESC
        """

        try:
            results = self.db.execute_select_query(query)
            return [CleanedDataMetadataSchemas.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting all cleaned data metadata schemas: {general_error}")
            return []

    def update_schema(
        self,
        schema_id: int,
        metadata_schema: Dict[str, Any],
    ) -> bool:
        """Updates an existing cleaned data metadata schema"""

        query = """
            UPDATE cleaned_data_metadata_schemas
            SET metadata_schema = %s, updated_at = %s
            WHERE id = %s
        """

        try:
            schema_json = json.dumps(metadata_schema)
            params = (schema_json, datetime.now(), schema_id)
            affected_rows = self.db.execute_update_delete_query(query, params)

            if affected_rows > 0:
                self.logger.info(f"Updated cleaned data metadata schema {schema_id}")
                return True
            self.logger.warning(f"No cleaned data metadata schema found with ID {schema_id}")
            return False

        except Exception as general_error:
            self.logger.error(f"Error updating cleaned data metadata schema {schema_id}: {general_error}")
            return False

    def delete_schema(
        self,
        schema_id: int,
    ) -> bool:
        """Deletes a cleaned data metadata schema"""

        query = """
            DELETE FROM cleaned_data_metadata_schemas WHERE id = %s
        """

        try:
            affected_rows = self.db.execute_update_delete_query(query, (schema_id,))

            if affected_rows > 0:
                self.logger.info(f"Deleted cleaned data metadata schema {schema_id}")
                return True
            self.logger.warning(f"No cleaned data metadata schema found with ID {schema_id}")
            return False

        except Exception as general_error:
            self.logger.error(f"Error deleting cleaned data metadata schema {schema_id}: {general_error}")
            return False

    def find_schema_by_content(
        self,
        schema_content: Dict[str, Any],
    ) -> Optional[CleanedDataMetadataSchemas]:
        """Finds a schema that matches the given content structure"""

        query = """
            SELECT * FROM cleaned_data_metadata_schemas WHERE metadata_schema = %s
        """

        try:
            schema_json = json.dumps(schema_content, sort_keys=True)
            results = self.db.execute_select_query(query, (schema_json,))
            if results:
                return CleanedDataMetadataSchemas.from_dict(results[0])
            return None

        except Exception as general_error:
            self.logger.error(f"Error finding schema by content: {general_error}")
            return None

    def get_or_create_schema(
        self,
        metadata_schema: Dict[str, Any],
    ) -> Optional[CleanedDataMetadataSchemas]:
        """Get existing schema or create new one if doesn't exist"""
        existing = self.find_schema_by_content(metadata_schema)
        if existing:
            return existing

        new_id = self.create_schema(metadata_schema)
        if new_id:
            return self.get_by_id(new_id)
        return None

    def get_unused_schemas(self) -> List[CleanedDataMetadataSchemas]:
        """Gets schemas that are not being used by any cleaned data"""

        query = """
            SELECT cdms.*
            FROM cleaned_data_metadata_schemas cdms
            LEFT JOIN cleaned_data cd ON cdms.id = cd.cleaned_data_metadata_schema_id
            WHERE cd.id IS NULL
            ORDER BY cdms.created_at DESC
        """

        try:
            results = self.db.execute_select_query(query)
            self.logger.info(f"Found {len(results)} unused schemas")
            return [CleanedDataMetadataSchemas.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting unused schemas: {general_error}")
            return []

    def search_schemas_by_property(
        self,
        property_name: str,
    ) -> List[CleanedDataMetadataSchemas]:
        """Search schemas that contain a specific property in their structure"""

        query = """
            SELECT * FROM cleaned_data_metadata_schemas
            WHERE metadata_schema::text ILIKE %s
            ORDER BY created_at DESC
        """

        try:
            search_pattern = f'%"{property_name}"%'
            results = self.db.execute_select_query(query, (search_pattern,))
            return [CleanedDataMetadataSchemas.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error searching schemas by property '{property_name}': {general_error}")
            return []
