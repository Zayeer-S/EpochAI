from abc import ABC, abstractmethod
import time
from typing import Any, Dict, List, Optional

from epochai.common.database.models import CleanedData, RawData
from epochai.common.logging_config import get_logger
from epochai.common.services.cleaning_service import CleaningService
from epochai.common.utils.decorators import handle_initialization_errors
from epochai.common.utils.schema_utils import SchemaUtils


class BaseCleaner(ABC):
    @handle_initialization_errors(f"{__name__} Initialization")
    def __init__(
        self,
        cleaner_name: str,
        cleaner_version: str,
    ):
        self.cleaner_name = cleaner_name
        self.cleaner_version = cleaner_version
        self.logger = get_logger(__name__)

        self.service = CleaningService(cleaner_name, cleaner_version)

        from epochai.common.database.dao.cleaned_data_metadata_schemas_dao import CleanedDataMetadataSchemasDAO

        self._schema_utils = SchemaUtils(
            name=self.cleaner_name,
            version=self.cleaner_version,
            metadata_schema_dao_class=CleanedDataMetadataSchemasDAO(),
            schema_name_field="cleaner_name",
            schema_version_field="current_schema_version",
        )

        self.logger.info(f"Initialized {self.cleaner_name} v{self.cleaner_version}")

    @abstractmethod
    def transform_content(
        self,
        raw_data: RawData,
    ) -> Dict[str, Any]:
        """
        Transforms raw data into a format that is accepted and validated against the schema

        Returns:
            Dictionary containing cleaned metadata
        """
        raise NotImplementedError(f"Subclasses must implement {self.transform_content.__name__} method")

    def clean_single_record(
        self,
        raw_data_id: int,
    ) -> Optional[int]:
        """ "
        Transforms and validates a single raw data record

        Returns:
            id of cleaned data row if succeeds, None if fails
        """

        start_time = time.time()

        try:
            raw_data: Optional[RawData] = self.service.raw_data_dao.get_by_id(raw_data_id)
            if not raw_data:
                self.logger.error(f"Raw data with id '{raw_data_id}' not found")
                return None

            self.logger.info(f"Cleaning raw data id '{raw_data_id}': '{raw_data.title}'")

            existing_cleaned: List[CleanedData] = self.service.cleaned_data_dao.get_by_raw_data_id(
                raw_data_id,
            )
            for cleaned in existing_cleaned:
                if cleaned.cleaner_used == self.cleaner_name and cleaned.cleaner_version == self.cleaner_version:
                    self.logger.warning(
                        f"Raw data {raw_data_id} already cleaned by {self.cleaner_name} v{self.cleaner_version}",
                    )
                    return cleaned.id

            transformed_metadata = self.transform_content(raw_data)

            is_valid, validation_error = self._schema_utils.validate_content(transformed_metadata)

            cleaning_time_ms = int((time.time() - start_time) * 1000)
            schema_id = self._schema_utils.get_metadata_schema_id()

            cleaned_data_id = self.service.save_cleaned_content(
                raw_data=raw_data,
                transformed_metadata=transformed_metadata,
                is_valid=is_valid,
                validation_error=validation_error,
                cleaning_time_ms=cleaning_time_ms,
                schema_id=schema_id,
            )

            return cleaned_data_id

        except Exception as general_error:
            cleaning_time_ms = int((time.time() - start_time) * 1000)
            self.logger.error(f"Error cleaning raw data {raw_data_id}: {general_error}")

            schema_id = self._schema_utils.get_metadata_schema_id()
            if "raw_data" in locals() and raw_data:
                schema_id = self._schema_utils.get_metadata_schema_id()
                self.service.save_error_record(raw_data, general_error, cleaning_time_ms, schema_id)

            return None

    def clean_multiple_records(
        self,
        raw_data_ids: List[int],
    ) -> Dict[str, Any]:
        """
        Cleans multiple raw data records

        Returns:
            Dict with cleaning results mapped to stats
        """

        if not raw_data_ids:
            self.logger.warning("No raw data ids provided for cleaning")
            return {"success_count": 0, "error_count": 0, "cleaned_ids": [], "error_ids": []}

        self.logger.info(f"Starting batch cleaning of {len(raw_data_ids)} records")
        start_time = time.time()

        success_count = 0
        complete_fail_count = 0
        cleaned_ids = []
        error_ids = []

        for raw_data_id in raw_data_ids:
            try:
                cleaned_id = self.clean_single_record(raw_data_id)
                if cleaned_id:
                    success_count += 1
                    cleaned_ids.append(cleaned_id)
                else:
                    complete_fail_count += 1
                    error_ids.append(raw_data_id)

            except Exception as general_error:
                self.logger.error(f"Unexpected error cleaning raw data {raw_data_id}: {general_error}")
                complete_fail_count += 1
                error_ids.append(raw_data_id)

        total_time = time.time() - start_time
        self.logger.info(
            f"Batch cleaning completed: {success_count} cleaned with/without errors, {complete_fail_count} failed completely - {total_time}s total, {total_time / len(raw_data_ids) if raw_data_ids else 0}s avg per record",  # noqa
        )

        return {
            "success_count": success_count,
            "error_count": complete_fail_count,
            "cleaned_ids": cleaned_ids,
            "error_ids": error_ids,
            "total_time_seconds": total_time,
            "average_time_per_record": total_time / len(raw_data_ids) if raw_data_ids else 0,
        }

    def clean_by_validation_status(
        self,
        validation_status: str,
    ) -> Dict[str, Any]:
        """Cleans all raw data records by a specific validation status"""
        raw_data_records: List[RawData] = self.service.raw_data_dao.get_by_validation_status(
            validation_status,
        )
        if not raw_data_records:
            self.logger.info(f"No raw data found with validation status '{validation_status}'")
            return {"success_count": 0, "error_count": 0, "cleaned_ids": [], "error_ids": []}

        raw_data_ids = [record.id for record in raw_data_records if record.id]
        return self.clean_multiple_records(raw_data_ids)

    def clean_recent_data(
        self,
        hours: int,
    ) -> Dict[str, Any]:
        """
        Cleans raw data created in the last x hours

        Returns:
            dict containing cleaning results and basic stats
        """
        self.logger.info(f"Cleaning raw data from the last {hours} hours")

        raw_data_records: List[RawData] = self.service.raw_data_dao.get_recent_contents(hours)
        if not raw_data_records:
            self.logger.info(f"No raw data found from the last {hours} hours")
            return {"success_count": 0, "error_count": 0, "cleaned_ids": [], "error_ids": []}

        raw_data_ids = [record.id for record in raw_data_records if record.id]
        return self.clean_multiple_records(raw_data_ids)

    def get_cleaning_statistics(self) -> Dict[str, Any]:
        """
        Gets stats about data cleaned by this cleaner

        Returns:
            dict containing cleaning comprehensive stats
        """

        try:
            cleaned_records: List[CleanedData] = self.service.cleaned_data_dao.get_by_cleaner(
                self.cleaner_name,
                self.cleaner_version,
            )

            if not cleaned_records:
                return {
                    "total_cleaned": 0,
                    "cleaner_name": self.cleaner_name,
                    "cleaner_version": self.cleaner_version,
                }

            cleaning_times = [record.cleaning_time_ms for record in cleaned_records]
            valid_count = sum(
                1 for record in cleaned_records if record.validation_status_id == self.service.get_validation_status_id("valid")
            )
            invalid_count = len(cleaned_records) - valid_count

            stats = {
                "cleaner_name": self.cleaner_name,
                "cleaner_version": self.cleaner_version,
                "total_cleaned": len(cleaned_records),
                "valid_count": valid_count,
                "invalid_count": invalid_count,
                "success_rate": (valid_count / len(cleaned_records) * 100) if cleaned_records else 0,
                "avg_cleaning_time_ms": sum(cleaning_times) / len(cleaning_times) if cleaning_times else 0,
                "min_cleaning_time_ms": min(cleaning_times) if cleaning_times else 0,
                "max_cleaning_time_ms": max(cleaning_times) if cleaning_times else 0,
                "first_cleaned": min(record.created_at for record in cleaned_records if record.created_at),
                "last_cleaned": max(record.created_at for record in cleaned_records if record.created_at),
            }

            return stats

        except Exception as general_error:
            self.logger.error(f"Error getting cleaning statistics: {general_error}")
            return {
                "error": str(general_error),
                "cleaner_name": self.cleaner_name,
                "cleaner_version": self.cleaner_version,
            }
