from datetime import datetime
import json
from typing import Any, Dict, List, Optional

from epochai.common.database.database import get_database
from epochai.common.database.models import CleanedData
from epochai.common.logging_config import get_logger


class CleanedDataDAO:
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)

    def check_if_already_cleaned_for_version(
        self,
        raw_data_id: int,
        cleaner_used: str,
        cleaner_version: str,
    ) -> Optional[CleanedData]:
        """Checks if raw data was already cleaned for a specific version of a specific cleaner"""

        query = """
            SELECT * FROM cleaned_data
            WHERE raw_data_id = %s,
            AND cleaner_used = %s,
            AND cleaner_version = %s
            LIMIT 1
        """

        params = (raw_data_id, cleaner_used, cleaner_version)

        try:
            results = self.db.execute_select_query(query, params)
            if results:
                return CleanedData.from_dict(results[0])
            return None
        except Exception as general_error:
            self.logger.error(f"Error checking if already cleaned: {general_error}")
            return None

    def create_cleaned_data(
        self,
        raw_data_id: int,
        cleaned_data_metadata_schema_id: int,
        title: str,
        language_code: str,
        cleaner_used: str,
        cleaner_version: str,
        cleaning_time_ms: int,
        url: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        validation_status_id: int = 0,
        validation_error: Optional[Dict[str, Any]] = None,
        cleaned_at: Optional[datetime] = None,
    ) -> Optional[int]:
        """
        Creates a new cleaned data record

        Returns:
            The id of created cleaned data or None if it fails
        """

        query = """
            INSERT INTO cleaned_data
            (raw_data_id, cleaned_data_metadata_schema_id, title, language_code,
             url, metadata, validation_status_id, validation_error, cleaner_used,
             cleaner_version, cleaning_time_ms, cleaned_at, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """

        try:
            existing = self.check_if_already_cleaned_for_version(raw_data_id, cleaner_used, cleaner_version)
            if existing:
                self.logger.warning(
                    f"Raw data id {raw_data_id} already cleaned by {cleaner_used} v{cleaner_version} (existing cleaned data id: {existing.id})",  # noqa
                )
                return existing.id

            current_timestamp = datetime.now()
            cleaned_at_timestamp = cleaned_at if cleaned_at else current_timestamp
            validation_error_json = json.dumps(validation_error) if validation_error else None
            metadata_json = json.dumps(metadata) if metadata else None

            params = (
                raw_data_id,
                cleaned_data_metadata_schema_id,
                title,
                language_code,
                url,
                metadata_json,
                validation_status_id,
                validation_error_json,
                cleaner_used,
                cleaner_version,
                cleaning_time_ms,
                cleaned_at_timestamp,
                current_timestamp,
            )

            result = self.db.execute_insert_query(query, params)

            if result:
                self.logger.info(
                    f"Created cleaned data: '{title}' (raw_data_id: {raw_data_id})",
                )
                return result
            self.logger.error(f"Failed to create cleaned data: '{title}'")
            return None

        except Exception as general_error:
            self.logger.error(f"Error creating cleaned data '{title}': {general_error}")
            return None

    def get_by_id(
        self,
        cleaned_data_id: int,
    ) -> Optional[CleanedData]:
        """Gets cleaned data by id"""

        query = """
            SELECT * FROM cleaned_data WHERE id = %s
        """

        try:
            results = self.db.execute_select_query(query, (cleaned_data_id,))
            if results:
                return CleanedData.from_dict(results[0])
            return None

        except Exception as general_error:
            self.logger.error(f"Error getting cleaned data by id {cleaned_data_id}: {general_error}")
            return None

    def get_all(
        self,
        limit: Optional[int] = None,
    ) -> List[CleanedData]:
        """Gets all cleaned data with an optional limit"""

        query = """
            SELECT * FROM cleaned_data ORDER BY created_at DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        try:
            results = self.db.execute_select_query(query)
            return [CleanedData.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting all cleaned data: {general_error}")
            return []

    def get_by_raw_data_id(
        self,
        raw_data_id: int,
    ) -> List[CleanedData]:
        """Gets all cleaned data for a specific raw data record"""

        query = """
            SELECT * FROM cleaned_data WHERE raw_data_id = %s ORDER BY created_at DESC
        """

        try:
            results = self.db.execute_select_query(query, (raw_data_id,))
            return [CleanedData.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting cleaned data for raw_data_id {raw_data_id}: {general_error}")
            return []

    def get_by_validation_status(
        self,
        validation_status_name: str,
    ) -> List[CleanedData]:
        """Gets cleaned data by validation status"""

        query = """
            SELECT cd.*
            FROM cleaned_data cd
            JOIN validation_statuses vs ON cd.validation_status_id = vs.id
            WHERE vs.validation_status_name = %s
            ORDER BY cd.created_at DESC
        """

        try:
            results = self.db.execute_select_query(query, (validation_status_name,))
            cleaned_data = [CleanedData.from_dict(row) for row in results]

            self.logger.info(
                f"Found {len(cleaned_data)} cleaned data with validation status '{validation_status_name}'",
            )
            return cleaned_data

        except Exception as general_error:
            self.logger.error(
                f"Error getting cleaned data by validation status '{validation_status_name}': {general_error}",  # noqa
            )
            return []

    def get_invalid_rows(self) -> List[CleanedData]:
        """Gets all cleaned data rows that failed validation"""
        return self.get_by_validation_status("invalid")

    def get_valid_rows(self) -> List[CleanedData]:
        """Gets all cleaned data rows that passed validation"""
        return self.get_by_validation_status("valid")

    def get_pending_validation(self) -> List[CleanedData]:
        """Gets all cleaned data rows pending validation"""
        return self.get_by_validation_status("pending")

    def get_by_cleaner(
        self,
        cleaner_used: str,
        cleaner_version: Optional[str] = None,
    ) -> List[CleanedData]:
        """Gets cleaned data by cleaner used and optionally by version"""

        if cleaner_version:
            query = """
                SELECT * FROM cleaned_data
                WHERE cleaner_used = %s AND cleaner_version = %s
                ORDER BY created_at DESC
            """
            params = (cleaner_used, cleaner_version)
        else:
            query = """
                SELECT * FROM cleaned_data
                WHERE cleaner_used = %s
                ORDER BY created_at DESC
            """
            params = (cleaner_used,)  # type: ignore

        try:
            results = self.db.execute_select_query(query, params)
            return [CleanedData.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting cleaned data by cleaner '{cleaner_used}': {general_error}")
            return []

    def update_validation_status(
        self,
        cleaned_data_id: int,
        validation_status_id: int,
        validation_error: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Update validation status and error for a cleaned data row"""

        query = """
            UPDATE cleaned_data
            SET validation_status_id = %s, validation_error = %s
            WHERE id = %s
        """

        try:
            validation_error_json = json.dumps(validation_error) if validation_error else None
            params = (validation_status_id, validation_error_json, cleaned_data_id)

            affected_rows = self.db.execute_update_delete_query(query, params)

            if affected_rows > 0:
                self.logger.info(f"Updated validation status for cleaned data row {cleaned_data_id}")
                return True
            self.logger.warning(f"No cleaned data row found with id {cleaned_data_id} to update")
            return False

        except Exception as general_error:
            self.logger.error(
                f"Error updating validation status for cleaned data row {cleaned_data_id}: {general_error}",
            )
            return False

    def update_metadata(
        self,
        cleaned_data_id: int,
        metadata: Dict[str, Any],
    ) -> bool:
        """Update metadata for a cleaned data row"""

        query = """
            UPDATE cleaned_data
            SET metadata = %s
            WHERE id = %s
        """

        try:
            metadata_json = json.dumps(metadata)
            params = (metadata_json, cleaned_data_id)

            affected_rows = self.db.execute_update_delete_query(query, params)

            if affected_rows > 0:
                self.logger.info(f"Updated metadata for cleaned data row {cleaned_data_id}")
                return True
            self.logger.warning(f"No cleaned data row found with id {cleaned_data_id} to update")
            return False

        except Exception as general_error:
            self.logger.error(
                f"Error updating metadata for cleaned data row {cleaned_data_id}: {general_error}",
            )
            return False

    def search_by_title(
        self,
        search_term: str,
    ) -> List[CleanedData]:
        """Search cleaned data by title (partial match)"""

        query = """
            SELECT * FROM cleaned_data WHERE title ILIKE %s ORDER BY created_at DESC
        """

        try:
            search_pattern = f"%{search_term}%"
            results = self.db.execute_select_query(query, (search_pattern,))
            return [CleanedData.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error searching cleaned data by title '{search_term}': {general_error}")
            return []

    def search_by_metadata_content(
        self,
        search_term: str,
        metadata_field: Optional[str] = None,
    ) -> List[CleanedData]:
        """Search cleaned data by content within metadata JSON"""

        if metadata_field:
            query = """
                SELECT * FROM cleaned_data
                WHERE metadata ->> %s ILIKE %s
                ORDER BY created_at DESC
            """
            params = (metadata_field, f"%{search_term}%")
        else:
            query = """
                SELECT * FROM cleaned_data
                WHERE metadata::text ILIKE %s
                ORDER BY created_at DESC
            """
            params = (f"%{search_term}%",)  # type: ignore

        try:
            results = self.db.execute_select_query(query, params)
            return [CleanedData.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(
                f"Error searching cleaned data by metadata content '{search_term}': {general_error}",
            )
            return []

    def get_rows_with_details(
        self,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Gets cleaned data with all relevant details including raw data info
        """

        query = """
            SELECT
                cd.*,
                vs.validation_status_name,
                rd.title as raw_data_title,
                rd.url as raw_data_url,
                ca.search_term_used,
                ca.language_code_used,
                cfg.collection_name,
                ct.collection_type
            FROM cleaned_data cd
            LEFT JOIN validation_statuses vs ON cd.validation_status_id = vs.id
            LEFT JOIN raw_data rd ON cd.raw_data_id = rd.id
            LEFT JOIN collection_attempts ca ON rd.collection_attempt_id = ca.id
            LEFT JOIN collection_targets cfg ON ca.collection_target_id = cfg.id
            LEFT JOIN collection_types ct ON cfg.collection_type_id = ct.id
            ORDER BY cd.created_at DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        try:
            results = self.db.execute_select_query(query)
            self.logger.info(f"Retrieved {len(results)} cleaned data with details")
            return results

        except Exception as general_error:
            self.logger.error(f"Error getting cleaned data with details: {general_error}")
            return []

    def get_cleaned_data_statistics(self) -> Dict[str, Any]:
        """Gets comprehensive statistics about the cleaned data records"""

        basic_stats_query = """
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT raw_data_id) as unique_raw_data,
                COUNT(DISTINCT cleaner_used) as unique_cleaners,
                COUNT(DISTINCT language_code) as unique_languages,
                AVG(cleaning_time_ms) as avg_cleaning_time_ms,
                MIN(cleaning_time_ms) as min_cleaning_time_ms,
                MAX(cleaning_time_ms) as max_cleaning_time_ms
            FROM cleaned_data
        """

        validation_stats_query = """
            SELECT
                vs.validation_status_name,
                COUNT(*) as record_count
            FROM cleaned_data cd
            JOIN validation_statuses vs ON cd.validation_status_id = vs.id
            GROUP BY vs.validation_status_name
            ORDER BY record_count DESC
        """

        cleaner_stats_query = """
            SELECT
                cleaner_used,
                cleaner_version,
                COUNT(*) as record_count,
                AVG(cleaning_time_ms) as avg_cleaning_time_ms
            FROM cleaned_data
            GROUP BY cleaner_used, cleaner_version
            ORDER BY record_count DESC
        """

        try:
            basic_stats = self.db.execute_select_query(basic_stats_query)
            validation_stats = self.db.execute_select_query(validation_stats_query)
            cleaner_stats = self.db.execute_select_query(cleaner_stats_query)

            summary: Dict[str, Any] = {}

            stats = {
                "basic_stats": basic_stats[0] if basic_stats else {},
                "by_validation_status": validation_stats,
                "by_cleaner": cleaner_stats,
                "summary": summary,
            }

            if basic_stats and basic_stats[0]["total_records"]:
                total_records = basic_stats[0]["total_records"]

                for status_row in validation_stats:
                    status_name = status_row["validation_status_name"]
                    record_count = status_row["record_count"]
                    percentage = round((record_count / total_records * 100), 2)

                    summary[status_name] = {
                        "count": record_count,
                        "percentage": percentage,
                    }

            return stats

        except Exception as general_error:
            self.logger.error(f"Error getting cleaned data statistics: {general_error}")
            return {"basic_stats": {}, "by_validation_status": [], "by_cleaner": [], "summary": {}}

    def get_recent_contents(
        self,
        hours: int = 24,
    ) -> List[CleanedData]:
        """Gets cleaned data created in the last X hours"""

        query = """
            SELECT * FROM cleaned_data
            WHERE created_at >= %s
            ORDER BY created_at DESC
        """

        try:
            from datetime import timedelta

            cutoff_time = datetime.now() - timedelta(hours=hours)
            results = self.db.execute_select_query(query, (cutoff_time,))
            return [CleanedData.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting recent cleaned data from last {hours} hours: {general_error}")
            return []

    def get_by_schema_id(
        self,
        schema_id: int,
    ) -> List[CleanedData]:
        """Gets all cleaned data using a specific metadata schema"""

        query = """
            SELECT * FROM cleaned_data
            WHERE cleaned_data_metadata_schema_id = %s
            ORDER BY created_at DESC
        """

        try:
            results = self.db.execute_select_query(query, (schema_id,))
            return [CleanedData.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting cleaned data by schema id {schema_id}: {general_error}")
            return []

    def get_metadata_field_values(
        self,
        field_name: str,
        limit: Optional[int] = None,
    ) -> List[Any]:
        """Extract specific field values from metadata JSON across all records"""

        query = """
            SELECT metadata ->> %s as field_value
            FROM cleaned_data
            WHERE metadata ->> %s IS NOT NULL
            ORDER BY created_at DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        try:
            results = self.db.execute_select_query(query, (field_name, field_name))
            return [row["field_value"] for row in results if row["field_value"] is not None]

        except Exception as general_error:
            self.logger.error(f"Error extracting metadata field '{field_name}': {general_error}")
            return []

    def delete_single_row(
        self,
        cleaned_data_id: int,
    ) -> bool:
        """Delete a cleaned data record"""

        query = """
            DELETE FROM cleaned_data WHERE id = %s
        """

        try:
            affected_rows = self.db.execute_update_delete_query(query, (cleaned_data_id,))

            if affected_rows > 0:
                self.logger.info(f"Deleted cleaned data {cleaned_data_id}")
                return True
            self.logger.warning(f"No cleaned data found with id {cleaned_data_id} to delete")
            return False

        except Exception as general_error:
            self.logger.error(f"Error deleting cleaned data {cleaned_data_id}: {general_error}")
            return False

    def delete_multiple_rows(
        self,
        days_old: int,
    ) -> int:
        """Delete cleaned data older than specified days"""

        query = """
            DELETE FROM cleaned_data
            WHERE created_at < %s
        """

        try:
            from datetime import timedelta

            cutoff_date = datetime.now() - timedelta(days=days_old)
            affected_rows = self.db.execute_update_delete_query(query, (cutoff_date,))

            if affected_rows > 0:
                self.logger.info(f"Deleted {affected_rows} cleaned data older than {days_old} days")
            else:
                self.logger.info(f"No cleaned data older than {days_old} days found to delete")

            return affected_rows

        except Exception as general_error:
            self.logger.error(f"Error deleting old cleaned data: {general_error}")
            return 0

    def bulk_update_validation_status(
        self,
        cleaned_data_ids: List[int],
        validation_status_id: int,
    ) -> int:
        """Bulk update validation status for multiple cleaned data rows"""

        if not cleaned_data_ids:
            return 0

        placeholders = ",".join(["%s"] * len(cleaned_data_ids))

        query = f"""
            UPDATE cleaned_data
            SET validation_status_id = %s
            WHERE id IN ({placeholders})
        """

        try:
            params = [validation_status_id, *cleaned_data_ids]
            affected_rows = self.db.execute_update_delete_query(query, tuple(params))

            if affected_rows > 0:
                self.logger.info(f"Bulk updated validation status for {affected_rows} cleaned data")

            return affected_rows

        except Exception as general_error:
            self.logger.error(f"Error bulk updating validation status: {general_error}")
            return 0

    def get_cleaning_performance_stats(
        self,
        cleaner_used: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get performance statistics for cleaners"""

        if cleaner_used:
            query = """
                SELECT
                    cleaner_used,
                    cleaner_version,
                    COUNT(*) as total_cleaned,
                    AVG(cleaning_time_ms) as avg_time_ms,
                    MIN(cleaning_time_ms) as min_time_ms,
                    MAX(cleaning_time_ms) as max_time_ms,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cleaning_time_ms) as median_time_ms
                FROM cleaned_data
                WHERE cleaner_used = %s
                GROUP BY cleaner_used, cleaner_version
                ORDER BY total_cleaned DESC
            """
            params = (cleaner_used,)
        else:
            query = """
                SELECT
                    cleaner_used,
                    cleaner_version,
                    COUNT(*) as total_cleaned,
                    AVG(cleaning_time_ms) as avg_time_ms,
                    MIN(cleaning_time_ms) as min_time_ms,
                    MAX(cleaning_time_ms) as max_time_ms,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cleaning_time_ms) as median_time_ms
                FROM cleaned_data
                GROUP BY cleaner_used, cleaner_version
                ORDER BY total_cleaned DESC
            """
            params = ()  # type: ignore

        try:
            results = self.db.execute_select_query(query, params)
            return results

        except Exception as general_error:
            self.logger.error(f"Error getting cleaning performance stats: {general_error}")
            return []
