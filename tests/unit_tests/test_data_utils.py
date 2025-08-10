# ruff: noqa: SLF001, SIM117

from unittest.mock import Mock, patch

import pytest

from epochai.common.utils.data_utils import DataUtils


@pytest.fixture
def mock_config():
    """Based off config.yml"""
    return {
        "data_output": {
            "file_format": "csv",
            "directory": "data/raw",
            "incremental_saving": {
                "enabled": False,
            },
        },
        "data_validator": {
            "validate_before_save": True,
            "min_content_length": 10,
            "error_logging_limit": 10,
            "utf8_corruption_patterns": ["Ã©", "Ã¨", "â€™", "â€œ"],
            "required_fields_wikipedia": ["title", "content", "url", "language", "collected_at"],
        },
    }


@pytest.fixture
def sample_data():
    """Sample valid data for testing"""
    return [
        {
            "title": "Test Page 1",
            "content": "This is test content that is long enough to pass validation",
            "url": "https://example.com/page1",
            "language": "en",
            "collected_at": "2023-01-01T12:00:00",
        },
        {
            "title": "Test Page 2",
            "content": "This is another test content that is also long enough",
            "url": "https://example.com/page2",
            "language": "fr",
            "collected_at": "2023-01-01T13:00:00",
        },
    ]


@pytest.fixture
def data_utils(mock_config):
    """Create DataUtils instance with mocked dependencies"""
    with patch("epochai.common.utils.data_utils.get_logger") as mock_get_logger:
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        utils = DataUtils(mock_config)
        utils.logger = mock_logger
        return utils


class TestDataUtilsInitialization:
    def test_initialization_with_valid_config(self, mock_config):
        with patch("epochai.common.utils.data_utils.get_logger") as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger

            utils = DataUtils(mock_config)

            assert utils.config == mock_config
            assert utils.incremental_save_counter == 0
            assert utils.current_timestamp is not None
            assert len(utils.current_timestamp) == 15
            mock_get_logger.assert_called_once_with("epochai.common.utils.data_utils")

    def test_initialization_with_none_config(self):
        with patch("epochai.common.utils.data_utils.get_logger"):
            with pytest.raises(Exception, match="General Error: Config cannot be none or empty"):
                DataUtils(None)

    def test_initialization_with_empty_config(self):
        with patch("epochai.common.utils.data_utils.get_logger"):
            with pytest.raises(Exception, match="General Error: Config cannot be none or empty"):
                DataUtils({})

    @patch("epochai.common.utils.data_utils.get_logger")
    def test_initialization_import_error_handling(self, mock_get_logger):
        mock_get_logger.side_effect = ImportError("Module not found")

        with pytest.raises(ImportError, match="Error importing modules: Module not found"):
            DataUtils({"some": "config"})

    @patch("epochai.common.utils.data_utils.get_logger")
    def test_initialization_general_error_handling(self, mock_get_logger):
        mock_get_logger.side_effect = Exception("General error")

        # Should raise exception with "General Error:" prefix
        with pytest.raises(Exception, match="General Error: General error"):
            DataUtils({"some": "config"})


class TestGetSaveVariables:
    def test_get_save_variables_success(self, data_utils, sample_data):
        result = data_utils._get_save_variables(sample_data, "test_data")

        assert result is not None
        file_format, filepath, incremental_saving = result
        assert file_format == "csv"
        assert filepath.endswith("test_data_" + data_utils.current_timestamp + ".csv")
        assert incremental_saving is False

    def test_get_save_variables_no_data(self, data_utils):
        result = data_utils._get_save_variables([], "test_data")

        assert result is None
        data_utils.logger.warning.assert_called_once_with("No data provided to save")

    def test_get_save_variables_with_custom_parameters(self, data_utils, sample_data):
        # fmt: off
        result = data_utils._get_save_variables(
            sample_data,
            "test_data",
            file_format="json",
            output_directory="custom/dir",
            validate_before_save=False,
            incremental_saving=True,
        )
        # fmt: on

        assert result is not None
        file_format, filepath, incremental_saving = result
        assert file_format == "json"
        assert "custom/dir" in filepath
        assert incremental_saving is True

    def test_get_save_variables_validation_failure(self, data_utils, sample_data):
        with patch.object(data_utils, "validate_data_structure_and_quality", return_value=False):
            result = data_utils._get_save_variables(sample_data, "test_data")

            assert result is None
            data_utils.logger.error.assert_called_with("Data validation failed, cannot save invalid data")

    @patch("epochai.common.utils.data_utils.Path")
    def test_get_save_variables_creates_directory(self, mock_path, data_utils, sample_data):
        mock_path_instance = Mock()
        mock_path.return_value = mock_path_instance

        data_utils._get_save_variables(sample_data, "test_data")

        mock_path.assert_called_once_with("data/raw")
        mock_path_instance.mkdir.assert_called_once_with(parents=True, exist_ok=True)


class TestSaveAtEnd:
    @patch("epochai.common.utils.data_utils.pd.DataFrame")
    def test_save_at_end_csv_success(self, mock_dataframe, data_utils, sample_data):
        mock_df = Mock()
        mock_dataframe.return_value = mock_df
        mock_df.__len__ = Mock(return_value=2)

        with patch.object(data_utils, "_get_save_variables") as mock_get_save_vars:
            mock_get_save_vars.return_value = ("csv", "/test/path/file.csv", False)

            result = data_utils.save_at_end(sample_data, "test_data")

            assert result == "/test/path/file.csv"
            mock_dataframe.assert_called_once_with(sample_data)
            mock_df.to_csv.assert_called_once_with("/test/path/file.csv", index=False)
            data_utils.logger.info.assert_any_call("Starting save attempt")
            data_utils.logger.info.assert_any_call("Data saved to: /test/path/file.csv")
            data_utils.logger.info.assert_any_call("Total records saved: 2")

    @patch("epochai.common.utils.data_utils.pd.DataFrame")
    def test_save_at_end_json_success(self, mock_dataframe, data_utils, sample_data):
        mock_df = Mock()
        mock_dataframe.return_value = mock_df
        mock_df.__len__ = Mock(return_value=2)

        with patch.object(data_utils, "_get_save_variables") as mock_get_save_vars:
            mock_get_save_vars.return_value = ("json", "/test/path/file.json", False)

            result = data_utils.save_at_end(sample_data, "test_data")

            assert result == "/test/path/file.json"
            mock_df.to_json.assert_called_once_with("/test/path/file.json", orient="records", indent=2)

    @patch("epochai.common.utils.data_utils.pd.DataFrame")
    def test_save_at_end_excel_success(self, mock_dataframe, data_utils, sample_data):
        mock_df = Mock()
        mock_dataframe.return_value = mock_df
        mock_df.__len__ = Mock(return_value=2)

        with patch.object(data_utils, "_get_save_variables") as mock_get_save_vars:
            mock_get_save_vars.return_value = ("xlsx", "/test/path/file.xlsx", False)

            result = data_utils.save_at_end(sample_data, "test_data")

            assert result == "/test/path/file.xlsx"
            mock_df.to_excel.assert_called_once_with("/test/path/file.xlsx", index=False)

    @patch("epochai.common.utils.data_utils.pd.DataFrame")
    def test_save_at_end_unknown_format_defaults_to_csv(self, mock_dataframe, data_utils, sample_data):
        mock_df = Mock()
        mock_dataframe.return_value = mock_df
        mock_df.__len__ = Mock(return_value=2)

        with patch.object(data_utils, "_get_save_variables") as mock_get_save_vars:
            mock_get_save_vars.return_value = ("unknown", "/test/path/file.unknown", False)

            result = data_utils.save_at_end(sample_data, "test_data")

            assert result == "/test/path/file.csv"
            mock_df.to_csv.assert_called_once_with("/test/path/file.csv", index=False)
            data_utils.logger.warning.assert_called_once()

    def test_save_at_end_get_save_variables_returns_none(self, data_utils, sample_data):
        with patch.object(data_utils, "_get_save_variables", return_value=None) as mock_get_save_vars:
            mock_get_save_vars.__name__ = "_get_save_variables"

            result = data_utils.save_at_end(sample_data, "test_data")

            assert result is None
            data_utils.logger.error.assert_called_with("_get_save_variables is returning None")

    def test_save_at_end_incremental_saving_enabled_error(self, data_utils, sample_data):
        with patch.object(data_utils, "_get_save_variables") as mock_get_save_vars:
            mock_get_save_vars.return_value = ("csv", "/test/path/file.csv", True)

            result = data_utils.save_at_end(sample_data, "test_data")

            assert result is None
            data_utils.logger.error.assert_called_with(
                "Save at end being called even though incremental_saving is True: True",
            )

    @patch("epochai.common.utils.data_utils.pd.DataFrame")
    def test_save_at_end_exception_handling(self, mock_dataframe, data_utils, sample_data):
        mock_df = Mock()
        mock_dataframe.return_value = mock_df
        mock_df.to_csv.side_effect = Exception("Write error")

        with patch.object(data_utils, "_get_save_variables") as mock_get_save_vars:
            mock_get_save_vars.return_value = ("csv", "/test/path/file.csv", False)

            result = data_utils.save_at_end(sample_data, "test_data")

            assert result is None
            data_utils.logger.error.assert_called_with(
                "Error saving data to '/test/path/file.csv': Write error",
            )


class TestGetDataSummary:
    def test_get_data_summary_empty_data(self, data_utils):
        result = data_utils.get_data_summary([])

        assert result == {"total_records": 0}

    @patch("epochai.common.utils.data_utils.pd.DataFrame")
    def test_get_data_summary_with_content_stats(self, mock_dataframe, data_utils):
        mock_df = Mock()
        mock_dataframe.return_value = mock_df
        mock_df.__len__ = Mock(return_value=2)
        mock_df.columns = ["title", "content", "url"]

        mock_content_series = Mock()
        mock_str_accessor = Mock()
        mock_content_lengths = Mock()
        mock_content_lengths.mean.return_value = 50.0
        mock_content_lengths.median.return_value = 45.0
        mock_content_lengths.min.return_value = 30
        mock_content_lengths.max.return_value = 70

        mock_str_accessor.len.return_value = mock_content_lengths
        mock_content_series.str = mock_str_accessor

        # Use side_effect to handle column access
        def mock_getitem(key):
            if key == "content":
                return mock_content_series
            return Mock()

        mock_df.__getitem__ = Mock(side_effect=mock_getitem)

        result = data_utils.get_data_summary([{"content": "test"}])

        expected = {
            "total_records": 2,
            "columns": ["title", "content", "url"],
            "content_stats": {
                "average_length": 50.0,
                "median_length": 45.0,
                "min_length": 30,
                "max_length": 70,
            },
        }

        assert result == expected

    @patch("epochai.common.utils.data_utils.pd.DataFrame")
    def test_get_data_summary_with_language_distribution(self, mock_dataframe, data_utils):
        mock_df = Mock()
        mock_dataframe.return_value = mock_df
        mock_df.__len__ = Mock(return_value=3)
        mock_df.columns = ["title", "language"]

        mock_language_series = Mock()
        mock_value_counts = Mock()
        mock_value_counts.to_dict.return_value = {"en": 2, "fr": 1}
        mock_language_series.value_counts.return_value = mock_value_counts

        def mock_getitem(key):
            if key == "language":
                return mock_language_series
            return Mock()

        mock_df.__getitem__ = Mock(side_effect=mock_getitem)

        result = data_utils.get_data_summary([{"language": "en"}])

        expected = {
            "total_records": 3,
            "columns": ["title", "language"],
            "language_distribution": {"en": 2, "fr": 1},
        }

        assert result == expected


class TestLogDataSummary:
    def test_log_data_summary_basic(self, data_utils):
        with patch.object(data_utils, "get_data_summary") as mock_get_summary:
            mock_get_summary.return_value = {"total_records": 5}

            data_utils.log_data_summary([])

            data_utils.logger.info.assert_any_call("=" * 30)
            data_utils.logger.info.assert_any_call("Data Summary Statistics")
            data_utils.logger.info.assert_any_call("Total records: 5")

    def test_log_data_summary_with_content_stats(self, data_utils):
        with patch.object(data_utils, "get_data_summary") as mock_get_summary:
            mock_get_summary.return_value = {
                "total_records": 5,
                "content_stats": {
                    "average_length": 150.4,  # rounds to nearest int
                    "min_length": 50,
                    "max_length": 300,
                },
            }

            data_utils.log_data_summary([])

            data_utils.logger.info.assert_any_call("Average content length: 150 characters")
            data_utils.logger.info.assert_any_call("Content length range: 50 - 300")

    def test_log_data_summary_with_language_distribution(self, data_utils):
        with patch.object(data_utils, "get_data_summary") as mock_get_summary:
            mock_get_summary.return_value = {
                "total_records": 5,
                "language_distribution": {"en": 3, "fr": 2},
            }

            data_utils.log_data_summary([])

            data_utils.logger.info.assert_any_call("Records by language: ")
            data_utils.logger.info.assert_any_call("en: 3")
            data_utils.logger.info.assert_any_call("fr: 2")


class TestValidateDataStructureAndQuality:
    def test_validate_data_structure_success(self, data_utils, sample_data):
        result = data_utils.validate_data_structure_and_quality(sample_data)

        assert result is True
        data_utils.logger.info.assert_called_with("Data validation passed: 2 records validated successfully")

    def test_validate_data_structure_not_list(self, data_utils):
        result = data_utils.validate_data_structure_and_quality("not a list")

        assert result is False
        data_utils.logger.error.assert_called_with(
            "Data validation failed; Expected list but got <class 'str'>",
        )

    def test_validate_data_structure_empty_data(self, data_utils):
        result = data_utils.validate_data_structure_and_quality([])

        assert result is False
        data_utils.logger.error.assert_called_with("Data validation failed as no data was provided.")

    def test_validate_data_structure_missing_required_fields(self, data_utils):
        invalid_data = [
            {"title": "Test", "content": "Content that is long enough to pass the length validation"},
        ]  # Missing url, language, collected_at

        result = data_utils.validate_data_structure_and_quality(invalid_data)

        assert result is False
        data_utils.logger.warning.assert_any_call("Data validation found 1 issues:")

    def test_validate_data_structure_empty_required_fields(self, data_utils):
        invalid_data = [
            {
                "title": "",
                "content": "Valid content here",
                "url": "https://example.com",
                "language": "en",
                "collected_at": "2023-01-01T12:00:00",
            },
        ]

        result = data_utils.validate_data_structure_and_quality(invalid_data)

        assert result is False

    def test_validate_data_structure_content_too_short(self, data_utils):
        invalid_data = [
            {
                "title": "Test Page",
                "content": "Short",
                "url": "https://example.com",
                "language": "en",
                "collected_at": "2023-01-01T12:00:00",
            },
        ]

        result = data_utils.validate_data_structure_and_quality(invalid_data)

        assert result is False

    def test_validate_data_structure_utf8_corruption(self, data_utils):
        invalid_data = [
            {
                "title": "Test Page",
                "content": "Content with corruption Ã© character",
                "url": "https://example.com",
                "language": "en",
                "collected_at": "2023-01-01T12:00:00",
            },
        ]

        result = data_utils.validate_data_structure_and_quality(invalid_data)

        assert result is False

    def test_validate_data_structure_invalid_language_code(self, data_utils):
        invalid_data = [
            {
                "title": "Test Page",
                "content": "Valid content here",
                "url": "https://example.com",
                "language": "english",
                "collected_at": "2023-01-01T12:00:00",
            },
        ]

        result = data_utils.validate_data_structure_and_quality(invalid_data)

        assert result is False

    def test_validate_data_structure_invalid_url(self, data_utils):
        invalid_data = [
            {
                "title": "Test Page",
                "content": "Valid content here",
                "url": "not-a-valid-url",
                "language": "en",
                "collected_at": "2023-01-01T12:00:00",
            },
        ]

        result = data_utils.validate_data_structure_and_quality(invalid_data)

        assert result is False

    def test_validate_data_structure_non_dict_record(self, data_utils):
        invalid_data = ["not a dict", {"valid": "dict"}]

        result = data_utils.validate_data_structure_and_quality(invalid_data)

        assert result is False

    def test_validate_data_structure_custom_required_fields(self, data_utils):
        data = [{"custom_field": "value"}]
        custom_fields = {"custom_field"}

        result = data_utils.validate_data_structure_and_quality(data, required_fields=custom_fields)

        assert result is True

    def test_validate_data_structure_required_fields_as_list(self, data_utils):
        data = [{"field1": "value1", "field2": "value2"}]
        required_fields_list = ["field1", "field2"]

        result = data_utils.validate_data_structure_and_quality(data, required_fields=required_fields_list)

        assert result is True

    def test_validate_data_structure_invalid_required_fields_type(self, data_utils):
        data = [{"field": "value"}]

        result = data_utils.validate_data_structure_and_quality(data, required_fields="invalid_type")

        assert result is False
        data_utils.logger.error.assert_called_with(
            "required_fields currently <class 'str'>, must be a list or a set.",
        )

    def test_validate_data_structure_skip_content_quality_check(self, data_utils):
        data = [
            {
                "title": "Test Page",
                "content": "Short",
                "url": "https://example.com",
                "language": "en",
                "collected_at": "2023-01-01T12:00:00",
            },
        ]

        result = data_utils.validate_data_structure_and_quality(data, check_content_quality=False)

        assert result is True

    def test_validate_data_structure_error_logging_limit(self, data_utils):
        # Create data with multiple validation errors
        invalid_data = []
        for _i in range(15):  # Must be greater than error logging limit
            invalid_data.append(
                {
                    "title": "",
                    "content": "Valid content",
                    "url": "https://example.com",
                    "language": "en",
                    "collected_at": "2023-01-01T12:00:00",
                },
            )

        result = data_utils.validate_data_structure_and_quality(invalid_data)

        assert result is False
        data_utils.logger.warning.assert_any_call("Data validation found 15 issues:")
        data_utils.logger.warning.assert_any_call("  ... and 5 errors remaining")
