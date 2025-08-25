from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from epochai.common.database.models import CleanedData, RawData
from epochai.data_processing.cleaners.base_cleaner import BaseCleaner


class ConcreteCleaner(BaseCleaner):
    """Concrete implementation of BaseCleaner for testing"""

    def clean_content(self, raw_data: RawData) -> dict:
        """Mock implementation that returns test metadata"""
        return {
            "cleaned_content": f"Cleaned: {raw_data.title}",
            "cleaned_title": raw_data.title,
            "language": raw_data.language_code,
            "page_id": raw_data.id or 1,
            "test_field": "test_value",
        }


class FailingCleaner(BaseCleaner):
    """Cleaner that raises exceptions for testing error handling"""

    def clean_content(self, raw_data: RawData) -> dict:
        raise ValueError("Test cleaning error")


@pytest.fixture
def mock_cleaning_service():
    """Mock CleaningService with all necessary methods"""
    service = Mock()
    service.raw_data_dao = Mock()
    service.cleaned_data_dao = Mock()
    service.cleaned_data_dao.get_by_raw_data_id = Mock(return_value=[])
    service.handle_schema_management = Mock()
    service.validate_cleaned_content = Mock(return_value=(True, None))
    service.save_cleaned_content = Mock(return_value=123)
    service.save_error_record = Mock(return_value=456)
    service.get_validation_status_id = Mock(return_value=1)
    service.reload_schema_from_database = Mock(return_value=True)
    service.get_schema_info = Mock(return_value={"test": "info"})
    service.get_metadata_schema_id = Mock(return_value=1)
    return service


@pytest.fixture
def sample_raw_data():
    """Sample RawData instance for testing"""
    return RawData(
        id=1,
        title="Test Article",
        language_code="en",
        url="https://example.com/test",
        metadata={"source": "test"},
        validation_status_id=1,
    )


@pytest.fixture
def multiple_raw_data():
    """Multiple RawData instances for batch testing"""
    return [
        RawData(id=1, title="Article 1", language_code="en"),
        RawData(id=2, title="Article 2", language_code="en"),
        RawData(id=3, title="Article 3", language_code="en"),
    ]


@pytest.fixture
def cleaned_data_records():
    """Sample CleanedData records for statistics testing"""
    return [
        CleanedData(
            id=1,
            validation_status_id=1,  # valid
            cleaning_time_ms=100,
            created_at=datetime(2023, 1, 1),
        ),
        CleanedData(
            id=2,
            validation_status_id=1,  # valid
            cleaning_time_ms=200,
            created_at=datetime(2023, 1, 2),
        ),
        CleanedData(
            id=3,
            validation_status_id=2,  # invalid
            cleaning_time_ms=50,
            created_at=datetime(2023, 1, 3),
        ),
    ]


class TestBaseCleanerInitialization:
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_initialization_success(self, mock_service_class, mock_cleaning_service):
        mock_service_class.return_value = mock_cleaning_service

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")

        assert cleaner.cleaner_name == "test_cleaner"
        assert cleaner.cleaner_version == "1.0.0"
        assert cleaner.service == mock_cleaning_service
        mock_service_class.assert_called_once_with("test_cleaner", "1.0.0")


class TestCleanContent:
    def test_clean_content_abstract_method(self):
        """Test that BaseCleaner is abstract and requires clean_content implementation"""
        with pytest.raises(TypeError):
            BaseCleaner("test", "1.0")


class TestCleanSingleRecord:
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    @patch("epochai.data_processing.cleaners.base_cleaner.time.time")
    def test_clean_single_record_success(
        self,
        mock_time,
        mock_service_class,
        mock_cleaning_service,
        sample_raw_data,
    ):
        mock_service_class.return_value = mock_cleaning_service
        mock_time.side_effect = [0.0, 0.1]  # start time, end time
        mock_cleaning_service.raw_data_dao.get_by_id.return_value = sample_raw_data

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")
        result = cleaner.clean_single_record(1)

        assert result == 123
        mock_cleaning_service.raw_data_dao.get_by_id.assert_called_once_with(1)
        mock_cleaning_service.handle_schema_management.assert_called_once()
        mock_cleaning_service.validate_cleaned_content.assert_called_once()
        mock_cleaning_service.save_cleaned_content.assert_called_once()

    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_single_record_raw_data_not_found(self, mock_service_class, mock_cleaning_service):
        mock_service_class.return_value = mock_cleaning_service
        mock_cleaning_service.raw_data_dao.get_by_id.return_value = None

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")
        result = cleaner.clean_single_record(999)

        assert result is None
        mock_cleaning_service.raw_data_dao.get_by_id.assert_called_once_with(999)

    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_single_record_already_cleaned(
        self,
        mock_service_class,
        mock_cleaning_service,
        sample_raw_data,
    ):
        mock_service_class.return_value = mock_cleaning_service
        mock_cleaning_service.raw_data_dao.get_by_id.return_value = sample_raw_data

        existing_cleaned = CleanedData(
            id=789,
            cleaner_used="test_cleaner",
            cleaner_version="1.0.0",
        )
        mock_cleaning_service.cleaned_data_dao.get_by_raw_data_id.return_value = [existing_cleaned]

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")
        result = cleaner.clean_single_record(1)

        assert result == 789
        mock_cleaning_service.save_cleaned_content.assert_not_called()

    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    @patch("epochai.data_processing.cleaners.base_cleaner.time.time")
    def test_clean_single_record_validation_error(
        self,
        mock_time,
        mock_service_class,
        mock_cleaning_service,
        sample_raw_data,
    ):
        mock_service_class.return_value = mock_cleaning_service
        mock_time.side_effect = [0.0, 0.05]
        mock_cleaning_service.raw_data_dao.get_by_id.return_value = sample_raw_data
        mock_cleaning_service.validate_cleaned_content.return_value = (False, {"error": "validation failed"})

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")
        result = cleaner.clean_single_record(1)

        assert result == 123
        mock_cleaning_service.save_cleaned_content.assert_called_once()
        call_args = mock_cleaning_service.save_cleaned_content.call_args
        assert call_args[1]["is_valid"] is False
        assert call_args[1]["validation_error"] == {"error": "validation failed"}

    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    @patch("epochai.data_processing.cleaners.base_cleaner.time.time")
    def test_clean_single_record_exception_handling(
        self,
        mock_time,
        mock_service_class,
        mock_cleaning_service,
        sample_raw_data,
    ):
        mock_service_class.return_value = mock_cleaning_service
        mock_time.side_effect = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]
        mock_cleaning_service.raw_data_dao.get_by_id.return_value = sample_raw_data
        mock_cleaning_service.get_metadata_schema_id.return_value = 1

        cleaner = FailingCleaner("failing_cleaner", "1.0.0")
        with patch.object(cleaner, "clean_content", side_effect=ValueError("Test cleaning error")):
            result = cleaner.clean_single_record(1)

        assert result is None
        mock_cleaning_service.save_error_record.assert_called_once()
        call_args = mock_cleaning_service.save_error_record.call_args
        assert call_args[0][0] == sample_raw_data
        assert isinstance(call_args[0][1], ValueError)
        assert call_args[0][2] == 100


class TestCleanMultipleRecords:
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_multiple_records_success(self, mock_service_class, mock_cleaning_service):
        mock_service_class.return_value = mock_cleaning_service

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")

        with patch.object(cleaner, "clean_single_record", side_effect=[123, 124, 125]):
            result = cleaner.clean_multiple_records([1, 2, 3])

        expected_result = {
            "success_count": 3,
            "error_count": 0,
            "cleaned_ids": [123, 124, 125],
            "error_ids": [],
            "total_time_seconds": result["total_time_seconds"],  # Dynamic value
            "average_time_per_record": result["average_time_per_record"],  # Dynamic value
        }

        assert result["success_count"] == expected_result["success_count"]
        assert result["error_count"] == expected_result["error_count"]
        assert result["cleaned_ids"] == expected_result["cleaned_ids"]
        assert result["error_ids"] == expected_result["error_ids"]
        assert "total_time_seconds" in result
        assert "average_time_per_record" in result

    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_multiple_records_mixed_results(self, mock_service_class, mock_cleaning_service):
        mock_service_class.return_value = mock_cleaning_service

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")

        with patch.object(cleaner, "clean_single_record", side_effect=[123, None, 125]):
            result = cleaner.clean_multiple_records([1, 2, 3])

        assert result["success_count"] == 2
        assert result["error_count"] == 1
        assert result["cleaned_ids"] == [123, 125]
        assert result["error_ids"] == [2]

    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_multiple_records_empty_list(self, mock_service_class, mock_cleaning_service):
        mock_service_class.return_value = mock_cleaning_service

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")
        result = cleaner.clean_multiple_records([])

        expected_result = {
            "success_count": 0,
            "error_count": 0,
            "cleaned_ids": [],
            "error_ids": [],
        }

        assert result["success_count"] == expected_result["success_count"]
        assert result["error_count"] == expected_result["error_count"]
        assert result["cleaned_ids"] == expected_result["cleaned_ids"]
        assert result["error_ids"] == expected_result["error_ids"]

    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_multiple_records_exception_handling(self, mock_service_class, mock_cleaning_service):
        mock_service_class.return_value = mock_cleaning_service

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")

        with patch.object(
            cleaner,
            "clean_single_record",
            side_effect=[123, Exception("Unexpected error"), 125],
        ):
            result = cleaner.clean_multiple_records([1, 2, 3])

        assert result["success_count"] == 2
        assert result["error_count"] == 1
        assert result["cleaned_ids"] == [123, 125]
        assert result["error_ids"] == [2]


class TestCleanByValidationStatus:
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_by_validation_status_success(
        self,
        mock_service_class,
        mock_cleaning_service,
        multiple_raw_data,
    ):
        mock_service_class.return_value = mock_cleaning_service
        mock_cleaning_service.raw_data_dao.get_by_validation_status.return_value = multiple_raw_data

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")

        with patch.object(
            cleaner,
            "clean_multiple_records",
            return_value={"success_count": 3},
        ) as mock_clean_multiple:
            result = cleaner.clean_by_validation_status("valid")

        mock_cleaning_service.raw_data_dao.get_by_validation_status.assert_called_once_with("valid")
        mock_clean_multiple.assert_called_once_with([1, 2, 3])
        assert result == {"success_count": 3}

    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_by_validation_status_no_records(self, mock_service_class, mock_cleaning_service):
        mock_service_class.return_value = mock_cleaning_service
        mock_cleaning_service.raw_data_dao.get_by_validation_status.return_value = []

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")
        result = cleaner.clean_by_validation_status("invalid")

        expected_result = {
            "success_count": 0,
            "error_count": 0,
            "cleaned_ids": [],
            "error_ids": [],
        }

        assert result == expected_result


class TestCleanRecentData:
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_recent_data_success(self, mock_service_class, mock_cleaning_service, multiple_raw_data):
        mock_service_class.return_value = mock_cleaning_service
        mock_cleaning_service.raw_data_dao.get_recent_contents.return_value = multiple_raw_data

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")

        with patch.object(
            cleaner,
            "clean_multiple_records",
            return_value={"success_count": 3},
        ) as mock_clean_multiple:
            result = cleaner.clean_recent_data(24)

        mock_cleaning_service.raw_data_dao.get_recent_contents.assert_called_once_with(24)
        mock_clean_multiple.assert_called_once_with([1, 2, 3])
        assert result == {"success_count": 3}

    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_recent_data_no_records(self, mock_service_class, mock_cleaning_service):
        mock_service_class.return_value = mock_cleaning_service
        mock_cleaning_service.raw_data_dao.get_recent_contents.return_value = []

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")
        result = cleaner.clean_recent_data(6)

        expected_result = {
            "success_count": 0,
            "error_count": 0,
            "cleaned_ids": [],
            "error_ids": [],
        }

        assert result == expected_result


class TestGetCleaningStatistics:
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_get_cleaning_statistics_success(
        self,
        mock_service_class,
        mock_cleaning_service,
        cleaned_data_records,
    ):
        mock_service_class.return_value = mock_cleaning_service
        mock_cleaning_service.cleaned_data_dao.get_by_cleaner.return_value = cleaned_data_records
        mock_cleaning_service.get_validation_status_id.return_value = 1

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")
        result = cleaner.get_cleaning_statistics()

        expected_stats = {
            "cleaner_name": "test_cleaner",
            "cleaner_version": "1.0.0",
            "total_cleaned": 3,
            "valid_count": 2,
            "invalid_count": 1,
            "success_rate": 66.66666666666666,
            "avg_cleaning_time_ms": 116.66666666666667,
            "min_cleaning_time_ms": 50,
            "max_cleaning_time_ms": 200,
            "first_cleaned": datetime(2023, 1, 1),
            "last_cleaned": datetime(2023, 1, 3),
        }

        assert result == expected_stats
        mock_cleaning_service.cleaned_data_dao.get_by_cleaner.assert_called_once_with("test_cleaner", "1.0.0")

    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_get_cleaning_statistics_no_records(self, mock_service_class, mock_cleaning_service):
        mock_service_class.return_value = mock_cleaning_service
        mock_cleaning_service.cleaned_data_dao.get_by_cleaner.return_value = []

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")
        result = cleaner.get_cleaning_statistics()

        expected_stats = {
            "total_cleaned": 0,
            "cleaner_name": "test_cleaner",
            "cleaner_version": "1.0.0",
        }

        assert result == expected_stats

    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_get_cleaning_statistics_exception(self, mock_service_class, mock_cleaning_service):
        mock_service_class.return_value = mock_cleaning_service
        mock_cleaning_service.cleaned_data_dao.get_by_cleaner.side_effect = Exception("Database error")

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")
        result = cleaner.get_cleaning_statistics()

        expected_result = {
            "error": "Database error",
            "cleaner_name": "test_cleaner",
            "cleaner_version": "1.0.0",
        }

        assert result == expected_result


class TestSchemaManagement:
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_reload_schema_from_database_success(self, mock_service_class, mock_cleaning_service):
        mock_service_class.return_value = mock_cleaning_service
        mock_cleaning_service.reload_schema_from_database.return_value = True

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")
        result = cleaner.reload_schema_from_database()

        assert result is True
        mock_cleaning_service.reload_schema_from_database.assert_called_once()

    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_get_schema_info(self, mock_service_class, mock_cleaning_service):
        mock_service_class.return_value = mock_cleaning_service
        expected_info = {"schema_cached": True, "schema_id": 1}
        mock_cleaning_service.get_schema_info.return_value = expected_info

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")
        result = cleaner.get_schema_info()

        assert result == expected_info
        mock_cleaning_service.get_schema_info.assert_called_once()

    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_get_metadata_schema_id(self, mock_service_class, mock_cleaning_service):
        mock_service_class.return_value = mock_cleaning_service
        mock_cleaning_service.get_metadata_schema_id.return_value = 42

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")
        result = cleaner.get_metadata_schema_id()

        assert result == 42
        mock_cleaning_service.get_metadata_schema_id.assert_called_once()


class TestCleanSingleRecordTiming:
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    @patch("epochai.data_processing.cleaners.base_cleaner.time.time")
    def test_cleaning_time_calculation(
        self,
        mock_time,
        mock_service_class,
        mock_cleaning_service,
        sample_raw_data,
    ):
        mock_service_class.return_value = mock_cleaning_service
        mock_time.side_effect = [0.0, 0.234]
        mock_cleaning_service.raw_data_dao.get_by_id.return_value = sample_raw_data

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")
        cleaner.clean_single_record(1)

        call_args = mock_cleaning_service.save_cleaned_content.call_args
        assert call_args[1]["cleaning_time_ms"] == 234

    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    @patch("epochai.data_processing.cleaners.base_cleaner.time.time")
    def test_exception_timing_calculation(
        self,
        mock_time,
        mock_service_class,
        mock_cleaning_service,
        sample_raw_data,
    ):
        mock_service_class.return_value = mock_cleaning_service
        mock_time.side_effect = [0.0, 0.156, 0.2, 0.3, 0.4, 0.5]
        mock_cleaning_service.raw_data_dao.get_by_id.return_value = sample_raw_data

        cleaner = FailingCleaner("failing_cleaner", "1.0.0")
        cleaner.clean_single_record(1)

        call_args = mock_cleaning_service.save_error_record.call_args
        assert call_args[0][2] == 156


class TestExistingCleanedDataCheck:
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_different_cleaner_version_processes_again(
        self,
        mock_service_class,
        mock_cleaning_service,
        sample_raw_data,
    ):
        mock_service_class.return_value = mock_cleaning_service
        mock_cleaning_service.raw_data_dao.get_by_id.return_value = sample_raw_data

        existing_cleaned = CleanedData(
            id=789,
            cleaner_used="test_cleaner",
            cleaner_version="0.9.0",
        )
        mock_cleaning_service.cleaned_data_dao.get_by_raw_data_id.return_value = [existing_cleaned]

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")
        result = cleaner.clean_single_record(1)

        assert result == 123
        mock_cleaning_service.save_cleaned_content.assert_called_once()

    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_different_cleaner_name_processes_again(
        self,
        mock_service_class,
        mock_cleaning_service,
        sample_raw_data,
    ):
        mock_service_class.return_value = mock_cleaning_service
        mock_cleaning_service.raw_data_dao.get_by_id.return_value = sample_raw_data

        existing_cleaned = CleanedData(
            id=789,
            cleaner_used="different_cleaner",
            cleaner_version="1.0.0",
        )
        mock_cleaning_service.cleaned_data_dao.get_by_raw_data_id.return_value = [existing_cleaned]

        cleaner = ConcreteCleaner("test_cleaner", "1.0.0")
        result = cleaner.clean_single_record(1)

        assert result == 123
        mock_cleaning_service.save_cleaned_content.assert_called_once()
