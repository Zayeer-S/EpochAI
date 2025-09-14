from unittest.mock import patch

import pytest

from epochai.common.config.config_validator import (
    CleanersConfig,
    ConfigValidator,
    DatabaseConfig,
    DataOutputConfig,
    DataSettings,
    DataValidatorConfig,
    DefaultsConfig,
    LoggingConfig,
    ValidateWholeConfig,
    WikipediaApiConfig,
    WikipediaCleanerConfig,
    WikipediaConfig,
    WikipediaDefaultApiConfig,
    WikipediaDefaultConfig,
)


@pytest.fixture
def sample_constraints():
    """Sample constraints configuration"""
    return {
        "data_output": {
            "allowed_formats": ["csv", "json", "xlsx"],
            "database": {
                "min_batch_size": 1,
                "max_batch_size": 1000,
            },
        },
        "data_validator": {
            "min_content_length": 10,
            "error_logging_limit": 10,
        },
        "logging_config": {
            "valid_levels": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        },
        "wikipedia": {
            "api": {
                "min_rate_limit_delay": 0.2,
                "min_retries": 1,
                "max_retries": 10,
                "min_request_timeout": 5,
                "min_recursive_limit": 1,
                "max_recursive_limit": 3,
                "search_max_results": 10,
            },
        },
    }


class TestConfigValidator:
    @patch("epochai.common.config.config_loader.ConfigLoader.load_constraints_config")
    def test_get_constraints_config(self, mock_load_constraints, sample_constraints):
        mock_load_constraints.return_value = sample_constraints

        result = ConfigValidator.get_constraints_config()

        mock_load_constraints.assert_called_once()
        assert result == sample_constraints


class TestDatabaseConfig:
    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_database_config_valid(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        config = DatabaseConfig(
            save_to_database=True,
            batch_size=100,
        )

        assert config.save_to_database is True
        assert config.batch_size == 100

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_database_config_batch_size_too_small(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        with pytest.raises(ValueError, match="batch_size currently 0, must be: 1 <= batch_size <= 1000"):
            DatabaseConfig(
                save_to_database=True,
                batch_size=0,
            )

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_database_config_batch_size_too_large(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        with pytest.raises(ValueError, match="batch_size currently 1001, must be: 1 <= batch_size <= 1000"):
            DatabaseConfig(
                save_to_database=True,
                batch_size=1001,
            )

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_database_config_batch_size_boundary_values(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        # Test minimum boundary
        config_min = DatabaseConfig(save_to_database=True, batch_size=1)
        assert config_min.batch_size == 1

        # Test maximum boundary
        config_max = DatabaseConfig(save_to_database=True, batch_size=1000)
        assert config_max.batch_size == 1000


class TestDataOutputConfig:
    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_data_output_config_valid(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        database_config = DatabaseConfig(save_to_database=True, batch_size=10)

        config = DataOutputConfig(
            directory="data/raw",
            default_type_wikipedia="wikipedia_data",
            separate_files_by_year=False,
            file_format="csv",
            database=database_config,
        )

        assert config.directory == "data/raw"
        assert config.file_format == "csv"
        assert config.database.batch_size == 10

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_data_output_config_invalid_format(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        database_config = DatabaseConfig(save_to_database=True, batch_size=10)

        with pytest.raises(ValueError, match="Current file format is 'txt', must be one of:"):
            DataOutputConfig(
                directory="data/raw",
                default_type_wikipedia="wikipedia_data",
                separate_files_by_year=False,
                file_format="txt",
                database=database_config,
            )

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_data_output_config_case_insensitive_format(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        database_config = DatabaseConfig(save_to_database=True, batch_size=10)

        # Should work with uppercase
        config = DataOutputConfig(
            directory="data/raw",
            default_type_wikipedia="wikipedia_data",
            separate_files_by_year=False,
            file_format="CSV",
            database=database_config,
        )

        assert config.file_format == "CSV"


class TestDataValidatorConfig:
    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_data_validator_config_valid(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        config = DataValidatorConfig(
            validate_before_save=True,
            min_content_length=50,
            error_logging_limit=20,
            utf8_corruption_patterns=["Ã©", "Ã¨"],
            required_fields_wikipedia={"title", "content", "url"},
        )

        assert config.validate_before_save is True
        assert config.min_content_length == 50
        assert config.error_logging_limit == 20

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_data_validator_config_min_content_length_too_small(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        with pytest.raises(
            ValueError,
            match="min_content_length currently '5', must be: min_content_length >= 10",
        ):
            DataValidatorConfig(
                validate_before_save=True,
                min_content_length=5,
                error_logging_limit=20,
                utf8_corruption_patterns=["Ã©"],
                required_fields_wikipedia={"title"},
            )

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_data_validator_config_error_logging_limit_too_small(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        with pytest.raises(
            ValueError,
            match="error_logging_limit currently '5', must be: error_logging_limit >= 10",
        ):
            DataValidatorConfig(
                validate_before_save=True,
                min_content_length=15,
                error_logging_limit=5,
                utf8_corruption_patterns=["Ã©"],
                required_fields_wikipedia={"title"},
            )

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_data_validator_config_boundary_values(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        # Test minimum boundary values
        config = DataValidatorConfig(
            validate_before_save=False,
            min_content_length=10,
            error_logging_limit=10,
            utf8_corruption_patterns=[],
            required_fields_wikipedia=set(),
        )

        assert config.min_content_length == 10
        assert config.error_logging_limit == 10


class TestLoggingConfig:
    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_logging_config_valid(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        config = LoggingConfig(
            level="INFO",
            log_to_file=True,
            log_directory="logs",
        )

        assert config.level == "INFO"
        assert config.log_to_file is True
        assert config.log_directory == "logs"

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_logging_config_invalid_level(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        with pytest.raises(ValueError, match="level currently 'INVALID', level must be one of:"):
            LoggingConfig(
                level="INVALID",
                log_to_file=True,
                log_directory="logs",
            )

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_logging_config_case_sensitive_level(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        # Test that lowercase level gets converted to uppercase for validation
        config = LoggingConfig(
            level="info",
            log_to_file=True,
            log_directory="logs",
        )

        assert config.level == "info"

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_logging_config_all_valid_levels(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

        for level in valid_levels:
            config = LoggingConfig(
                level=level,
                log_to_file=True,
                log_directory="logs",
            )
            assert config.level == level


class TestWikipediaApiConfig:
    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_wikipedia_api_config_valid(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        config = WikipediaApiConfig(
            language=["en", "fr"],
            rate_limit_delay=1.0,
            max_retries=5,
            search_max_results=10,
            request_timeout=30,
            recursive_limit=2,
        )

        assert config.language == ["en", "fr"]
        assert config.rate_limit_delay == 1.0

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_wikipedia_api_config_rate_limit_too_small(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        with pytest.raises(
            ValueError,
            match="rate_limit_delay is currently '0.1', must be: rate_limit_delay >= 0.2",
        ):
            WikipediaApiConfig(
                language=["en"],
                rate_limit_delay=0.1,
                max_retries=5,
                search_max_results=10,
                request_timeout=30,
                recursive_limit=2,
            )

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_wikipedia_api_config_max_retries_invalid(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        # Test too small
        with pytest.raises(ValueError, match="max_retries is currently '0', must be: 1 <= max_retries <= 10"):
            WikipediaApiConfig(
                language=["en"],
                rate_limit_delay=0.5,
                max_retries=0,
                search_max_results=10,
                request_timeout=30,
                recursive_limit=2,
            )

        # Test too large
        with pytest.raises(
            ValueError,
            match="max_retries is currently '15', must be: 1 <= max_retries <= 10",
        ):
            WikipediaApiConfig(
                language=["en"],
                rate_limit_delay=0.5,
                max_retries=15,
                search_max_results=10,
                request_timeout=30,
                recursive_limit=2,
            )

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_wikipedia_api_config_search_max_results_too_large(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        with pytest.raises(
            ValueError,
            match="search_max_results is currently '15', must be: search_max_results <= 10",
        ):
            WikipediaApiConfig(
                language=["en"],
                rate_limit_delay=0.5,
                max_retries=5,
                search_max_results=15,
                request_timeout=30,
                recursive_limit=2,
            )

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_wikipedia_api_config_request_timeout_too_small(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        with pytest.raises(
            ValueError,
            match="request_timeout is currently '3', must be: request_timeout >= 5",
        ):
            WikipediaApiConfig(
                language=["en"],
                rate_limit_delay=0.5,
                max_retries=5,
                search_max_results=10,
                request_timeout=3,
                recursive_limit=2,
            )

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_wikipedia_api_config_recursive_limit_invalid(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        # Test too small
        with pytest.raises(
            ValueError,
            match="Current recursive_limit is currently '0' but must be: 1 <= recursive_limit <= 3",
        ):
            WikipediaApiConfig(
                language=["en"],
                rate_limit_delay=0.5,
                max_retries=5,
                search_max_results=10,
                request_timeout=30,
                recursive_limit=0,
            )

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_wikipedia_api_config_recursive_limit_too_large(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        with pytest.raises(
            ValueError,
            match="recursive_limit is currently '5' which is greater than max: 3. Just use iteration at this point.",
        ):
            WikipediaApiConfig(
                language=["en"],
                rate_limit_delay=0.5,
                max_retries=5,
                search_max_results=10,
                request_timeout=30,
                recursive_limit=5,
            )

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_wikipedia_api_config_boundary_values(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        # Test minimum boundary values
        config_min = WikipediaApiConfig(
            language=["en"],
            rate_limit_delay=0.2,
            max_retries=1,
            search_max_results=1,
            request_timeout=5,
            recursive_limit=1,
        )

        assert config_min.rate_limit_delay == 0.2
        assert config_min.max_retries == 1
        assert config_min.request_timeout == 5
        assert config_min.recursive_limit == 1

        # Test maximum boundary values
        config_max = WikipediaApiConfig(
            language=["en"],
            rate_limit_delay=5.0,
            max_retries=10,
            search_max_results=10,
            request_timeout=60,
            recursive_limit=3,
        )

        assert config_max.max_retries == 10
        assert config_max.search_max_results == 10
        assert config_max.recursive_limit == 3


class TestWikipediaConfig:
    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_wikipedia_config_valid(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        api_config = WikipediaApiConfig(
            language=["en", "fr"],
            rate_limit_delay=1.0,
            max_retries=5,
            search_max_results=10,
            request_timeout=30,
            recursive_limit=2,
        )

        config = WikipediaConfig(api=api_config)

        assert config.api.language == ["en", "fr"]


class TestDataSettings:
    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_data_settings_valid(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        database_config = DatabaseConfig(save_to_database=True, batch_size=10)
        data_output_config = DataOutputConfig(
            directory="data/raw",
            default_type_wikipedia="wikipedia_data",
            separate_files_by_year=False,
            file_format="csv",
            database=database_config,
        )
        data_validator_config = DataValidatorConfig(
            validate_before_save=True,
            min_content_length=50,
            error_logging_limit=20,
            utf8_corruption_patterns=["Ã©"],
            required_fields_wikipedia={"title", "content"},
        )
        cleaners_config = CleanersConfig(
            wikipedia=WikipediaCleanerConfig(
                cleaner_name="wikipedia_cleaner",
                current_schema_version="1.0.0",
            ),
        )

        config = DataSettings(
            data_output=data_output_config,
            data_validator=data_validator_config,
            cleaners=cleaners_config,
        )

        assert config.data_output.directory == "data/raw"
        assert config.data_validator.min_content_length == 50
        assert config.cleaners.wikipedia.cleaner_name == "wikipedia_cleaner"


class TestValidateWholeConfig:
    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_validate_whole_config_success(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        # Create valid config components
        database_config = DatabaseConfig(save_to_database=True, batch_size=10)
        data_output_config = DataOutputConfig(
            directory="data/raw",
            default_type_wikipedia="wikipedia_data",
            separate_files_by_year=False,
            file_format="csv",
            database=database_config,
        )
        data_validator_config = DataValidatorConfig(
            validate_before_save=True,
            min_content_length=50,
            error_logging_limit=20,
            utf8_corruption_patterns=["Ã©"],
            required_fields_wikipedia={"title", "content"},
        )
        cleaners_config = CleanersConfig(
            wikipedia=WikipediaCleanerConfig(
                cleaner_name="wikipedia_cleaner",
                current_schema_version="1.0.0",
            ),
        )
        data_settings = DataSettings(
            data_output=data_output_config,
            data_validator=data_validator_config,
            cleaners=cleaners_config,
        )

        logging_config = LoggingConfig(
            level="INFO",
            log_to_file=True,
            log_directory="logs",
        )

        api_config = WikipediaApiConfig(
            language=["en"],
            rate_limit_delay=1.0,
            max_retries=5,
            search_max_results=10,
            request_timeout=30,
            recursive_limit=2,
        )
        wikipedia_config = WikipediaConfig(api=api_config)

        defaults_config = DefaultsConfig(
            wikipedia=WikipediaDefaultConfig(
                collector_name="wikipedia_collector",
                current_schema_version="1.0.0",
                api=WikipediaDefaultApiConfig(
                    language=["en"],
                    rate_limit_delay=2.0,
                    max_retries=3,
                    search_max_results=5,
                    request_timeout=30,
                    recursive_limit=1,
                ),
            ),
        )

        # Test the complete validation
        config = ValidateWholeConfig(
            data_settings=data_settings,
            logging=logging_config,
            wikipedia=wikipedia_config,
            defaults=defaults_config,
        )

        assert config.data_settings.data_output.directory == "data/raw"
        assert config.logging.level == "INFO"
        assert config.wikipedia.api.language == ["en"]
        assert config.defaults.wikipedia.collector_name == "wikipedia_collector"

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_validate_whole_config_via_class_method(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        config_dict = {
            "data_settings": {
                "data_output": {
                    "directory": "data/raw",
                    "default_type_wikipedia": "wikipedia_data",
                    "separate_files_by_year": False,
                    "file_format": "csv",
                    "database": {
                        "save_to_database": True,
                        "batch_size": 10,
                    },
                },
                "data_validator": {
                    "validate_before_save": True,
                    "min_content_length": 50,
                    "error_logging_limit": 20,
                    "utf8_corruption_patterns": ["Ã©"],
                    "required_fields_wikipedia": {"title", "content"},
                },
                "cleaners": {
                    "wikipedia": {
                        "cleaner_name": "wikipedia_cleaner",
                        "current_schema_version": "1.0.0",
                    },
                },
            },
            "logging": {
                "level": "INFO",
                "log_to_file": True,
                "log_directory": "logs",
            },
            "wikipedia": {
                "api": {
                    "language": ["en"],
                    "rate_limit_delay": 1.0,
                    "max_retries": 5,
                    "search_max_results": 10,
                    "request_timeout": 30,
                    "recursive_limit": 2,
                },
            },
            "defaults": {
                "wikipedia": {
                    "collector_name": "wikipedia_collector",
                    "current_schema_version": "1.0.0",
                    "api": {
                        "language": ["en"],
                        "rate_limit_delay": 2.0,
                        "max_retries": 3,
                        "search_max_results": 5,
                        "request_timeout": 30,
                        "recursive_limit": 1,
                    },
                },
            },
        }

        result = ValidateWholeConfig.validate_config(config_dict)

        assert isinstance(result, ValidateWholeConfig)
        assert result.data_settings.data_output.directory == "data/raw"
        assert result.logging.level == "INFO"
        assert result.wikipedia.api.language == ["en"]
        assert result.defaults.wikipedia.collector_name == "wikipedia_collector"

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_validate_whole_config_missing_field(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        # Missing required fields config
        incomplete_config = {
            "data_settings": {
                "data_output": {
                    "directory": "data/raw",
                },
            },
        }

        with pytest.raises(ValueError, match="Error validating config"):
            ValidateWholeConfig.validate_config(incomplete_config)

    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_validate_whole_config_invalid_values(self, mock_constraints, sample_constraints):
        mock_constraints.return_value = sample_constraints

        config_dict = {
            "data_settings": {
                "data_output": {
                    "directory": "data/raw",
                    "default_type_wikipedia": "wikipedia_data",
                    "separate_files_by_year": False,
                    "file_format": "invalid_format",
                    "database": {
                        "save_to_database": True,
                        "batch_size": 10,
                    },
                },
                "data_validator": {
                    "validate_before_save": True,
                    "min_content_length": 50,
                    "error_logging_limit": 20,
                    "utf8_corruption_patterns": ["Ã©"],
                    "required_fields_wikipedia": {"title", "content"},
                },
                "cleaners": {
                    "wikipedia": {
                        "cleaner_name": "wikipedia_cleaner",
                        "current_schema_version": "1.0.0",
                    },
                },
            },
            "logging": {
                "level": "INFO",
                "log_to_file": True,
                "log_directory": "logs",
            },
            "wikipedia": {
                "api": {
                    "language": ["en"],
                    "rate_limit_delay": 1.0,
                    "max_retries": 5,
                    "search_max_results": 10,
                    "request_timeout": 30,
                    "recursive_limit": 2,
                },
            },
            "defaults": {
                "wikipedia": {
                    "collector_name": "wikipedia_collector",
                    "current_schema_version": "1.0.0",
                    "api": {
                        "language": ["en"],
                        "rate_limit_delay": 2.0,
                        "max_retries": 3,
                        "search_max_results": 5,
                        "request_timeout": 30,
                        "recursive_limit": 1,
                    },
                },
            },
        }

        with pytest.raises(ValueError, match="Error validating config"):
            ValidateWholeConfig.validate_config(config_dict)


class TestConfigValidatorIntegration:
    @patch("epochai.common.config.config_validator.ConfigValidator.get_constraints_config")
    def test_multiple_validation_errors(self, mock_constraints, sample_constraints):
        """Test that multiple validation errors are handled properly"""
        mock_constraints.return_value = sample_constraints

        # Invalid value config
        with pytest.raises(ValueError):
            DatabaseConfig(
                save_to_database=True,
                batch_size=69420,
            )

        with pytest.raises(ValueError):
            LoggingConfig(
                level="INVALID_LEVEL",
                log_to_file=True,
                log_directory="logs",
            )
