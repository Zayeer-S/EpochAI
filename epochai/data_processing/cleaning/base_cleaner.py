from abc import ABC, abstractmethod
from datetime import datetime
import time
from typing import Any, Dict, List, Optional, Tuple

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.database.dao.cleaned_data_dao import CleanedDataDAO
from epochai.common.database.dao.cleaned_data_metadata_schemas_dao import CleanedDataMetadataSchemasDAO
from epochai.common.database.dao.raw_data_dao import RawDataDAO
from epochai.common.database.dao.validation_statuses_dao import ValidationStatusesDAO
from epochai.common.database.models import CleanedData, RawData
from epochai.common.logging_config import get_logger


class BaseCleaner(ABC):
    def __init__(
        self,
        cleaner_name: str,
        cleaner_version: str,
    ):
        self.cleaner_name = cleaner_name
        self.cleaner_version = cleaner_version
        self.logger = get_logger(__name__)

        self.raw_data_dao = RawDataDAO()
        self.cleaned_data_dao = CleanedDataDAO()
        self.cleaned_data_metadata_schema_dao = CleanedDataMetadataSchemasDAO()
        self.validation_statuses_dao = ValidationStatusesDAO()
        self.config = ConfigLoader.get_data_config()

        self._validation_status_cache = self._load_validation_statuses()

        self.metadata_schema_id: Optional[int] = None
        self.min_content_length = int(self.config["data_cleaner"]["min_content_length"])

        self.logger.info(f"Initialized {self.cleaner_name} v{self.cleaner_version}")

    def _load_validation_statuses(self) -> Dict[str, int]:
        """Loads and caches validation status ids"""

        try:
            statuses = self.validation_statuses_dao.get_all()
            return {status.validation_status_name: status.id for status in statuses if status.id}
        except Exception as general_error:
            self.logger.error(f"Failed to load validation statuses: {general_error}")
            return {}

    def _get_validation_status_id(
        self,
        status_name: str,
    ) -> Optional[int]:
        """Gets validation status ID"""
        status_id = self._validation_status_cache.get(status_name)
        if status_id is not None:
            return int(status_id)

        self.logger.warning(f"Validation status '{status_name}' not found")
        return None

    @abstractmethod
    def clean_content(
        self,
        raw_data: RawData,
    ) -> Dict[str, Any]:
        """
        Cleans raw data

        Returns:
            Dictionary containing cleaned metadata
        """
        raise NotImplementedError(f"Subclasses must implement {self.clean_content.__name__} method")

    @abstractmethod
    def validate_cleaned_content(
        self,
        cleaned_data: Dict[str, Any],
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Validates cleaned data

        Returns:
            Tuple of (is_valid, validation_error_dict)
        """
        raise NotImplementedError(
            f"Subclasses must implement {self.validate_cleaned_content.__name__} method",
        )

    @abstractmethod
    def get_metadata_schema_id(self) -> Optional[int]:
        """Gets metadata schema id for this cleaner"""
        raise NotImplementedError(f"Subclasses must implement {self.get_metadata_schema_id.__name__} method")

    def clean_single_record(
        self,
        raw_data_id: int,
    ) -> Optional[int]:
        """ "
        Cleans a single raw data record

        Returns:
            id of cleaned data row if succeeds, None if fails
        """

        start_time = time.time()

        try:
            raw_data: Optional[RawData] = self.raw_data_dao.get_by_id(raw_data_id)
            if not raw_data:
                self.logger.error(f"Raw data with id '{raw_data_id}' not found")
                return None

            self.logger.info(f"Cleaning raw data id '{raw_data_id}': '{raw_data.title}'")

            existing_cleaned: List[CleanedData] = self.cleaned_data_dao.get_by_raw_data_id(raw_data_id)
            for cleaned in existing_cleaned:
                if (
                    cleaned.cleaner_used == self.cleaner_name
                    and cleaned.cleaner_version == self.cleaner_version
                ):
                    self.logger.warning(
                        f"Raw data {raw_data_id} already cleaned by {self.cleaner_name} v{self.cleaner_version}",  # noqa
                    )
                    return cleaned.id

            cleaned_metadata = self.clean_content(raw_data)

            is_valid, validation_error = self.validate_cleaned_content(cleaned_metadata)
            validation_status_id: int = self._get_validation_status_id("valid" if is_valid else "invalid")

            cleaning_time_ms = int((time.time() - start_time) * 1000)

            schema_id = self.get_metadata_schema_id()
            if not schema_id:
                self.logger.error(f"No metadata schema id available for {self.cleaner_name}")
                return None

            cleaned_data_id: Optional[int] = self.cleaned_data_dao.create_cleaned_data(
                raw_data_id=raw_data.id,
                cleaned_data_metadata_schema_id=schema_id,
                title=raw_data.title,
                language_code=raw_data.language_code,
                cleaner_used=self.cleaner_name,
                cleaner_version=self.cleaner_version,
                cleaning_time_ms=cleaning_time_ms,
                url=raw_data.url,
                metadata=cleaned_metadata,
                validation_status_id=validation_status_id,
                validation_error=validation_error,
                cleaned_at=datetime.now(),
            )

            if cleaned_data_id:
                status_msg = "valid" if is_valid else "invalid"
                self.logger.info(
                    f"Successfully cleaned raw data {raw_data_id} to cleaned data {cleaned_data_id}. ({status_msg}, {cleaning_time_ms}ms)",  # noqa
                )
            else:
                self.logger.error(f"Failed to save cleaned data for raw data {raw_data_id}")

            return cleaned_data_id

        except Exception as general_error:
            cleaning_time_ms = int((time.time() - start_time) * 1000)
            self.logger.error(f"Error cleaning raw data {raw_data_id}: {general_error}")

            try:
                if "raw_data" in locals() and raw_data:
                    schema_id = self.get_metadata_schema_id()
                    if schema_id:
                        error_dict = {
                            "error": str(general_error),
                            "error_type": type(general_error).__name__,
                            "cleaning_failed": True,
                        }
                        self.cleaned_data_dao.create_cleaned_data(
                            raw_data_id=raw_data.id,
                            cleaned_data_metadata_schema_id=schema_id,
                            title=raw_data.title,
                            language_code=raw_data.language_code,
                            url=raw_data.url,
                            metadata={},
                            validation_status_id=self._get_validation_status_id("invalid"),
                            validation_error=error_dict,
                            cleaner_used=self.cleaner_name,
                            cleaner_version=self.cleaner_version,
                            cleaning_time_ms=cleaning_time_ms,
                            cleaned_at=datetime.now(),
                        )

            except Exception as wtf_error:
                self.logger.error(f"Failed to save error information: {wtf_error}")

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
        error_count = 0
        cleaned_ids = []
        error_ids = []

        for raw_data_id in raw_data_ids:
            try:
                cleaned_id = self.clean_single_record(raw_data_id)
                if cleaned_id:
                    success_count += 1
                    cleaned_ids.append(cleaned_id)
                else:
                    error_count += 1
                    error_ids.append(raw_data_id)

            except Exception as general_error:
                self.logger.error(f"Unexpected error cleaning raw data {raw_data_id}: {general_error}")
                error_count += 1
                error_ids.append(raw_data_id)

        total_time = time.time() - start_time
        self.logger.info(
            f"Batch cleaning completed: {success_count} successful, {error_count} failed - {total_time}s total, {total_time / len(raw_data_ids) if raw_data_ids else 0}s avg per record",  # noqa
        )

        return {
            "success_count": success_count,
            "error_count": error_count,
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
        raw_data_records: List[RawData] = self.raw_data_dao.get_by_validation_status(validation_status)
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

        raw_data_records: List[RawData] = self.raw_data_dao.get_recent_contents(hours)
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
            cleaned_records: List[CleanedData] = self.cleaned_data_dao.get_by_cleaner(
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
                1
                for record in cleaned_records
                if record.validation_status_id == self._get_validation_status_id("valid")
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
