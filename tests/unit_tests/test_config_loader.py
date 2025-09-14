# ruff: noqa: SIM117

from unittest.mock import Mock, mock_open, patch

import pytest
import yaml

from epochai.common.config.config_loader import ConfigLoader


@pytest.fixture
def sample_config():
    """Sample configuration based on config.yml"""
    return {
        "logging": {
            "level": "INFO",
            "log_to_file": True,
            "log_directory": "logs",
        },
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
                "validate_before_save": False,
                "min_content_length": 10,
                "error_logging_limit": 10,
                "utf8_corruption_patterns": ["Ã©", "Ã¨"],
                "required_fields_wikipedia": ["title", "content", "url"],
            },
        },
        "defaults": {
            "wikipedia": {
                "api": {
                    "collector_name": "wikipedia_collector",
                    "language": ["en"],
                    "rate_limit_delay": 2.0,
                    "max_retries": 3,
                    "search_max_results": 5,
                    "request_timeout": 30,
                    "recursive_limit": 1,
                },
            },
        },
        "wikipedia": {
            "api": {
                "collector_name": "wikipedia_collector",
                "language": ["en", "fr"],
                "rate_limit_delay": 0.5,
                "max_retries": 5,
                "search_max_results": 10,
                "request_timeout": 10,
            },
        },
        "metadata_schema": {
            "schema_cache_limit": 100,
            "schema_check_interval": 50,
        },
    }


@pytest.fixture
def sample_constraints():
    """Sample constraints configuration based on constraints.yml"""
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


class TestConfigLoaderPathGeneration:
    @patch("os.path.dirname")
    @patch("os.path.abspath")
    @patch("os.path.join")
    def test_get_config_path(self, mock_join, mock_abspath, mock_dirname):
        mock_dirname.return_value = "/current/dir"
        mock_abspath.return_value = "/project/root"
        mock_join.side_effect = lambda *args: "/".join(args)

        result = ConfigLoader._get_config_path("config.yml")  # noqa
        mock_dirname.assert_called_once()
        mock_abspath.assert_called_once_with("/current/dir/../../..")
        mock_join.assert_called_with("/project/root", "config.yml")
        assert result == "/project/root/config.yml"


class TestConfigLoaderLoadConfig:
    @patch("epochai.common.config.config_loader.ConfigLoader._get_config_path")
    @patch("epochai.common.config.config_loader.ConfigLoader.validate_whole_config")
    def test_load_config_success(self, mock_validate, mock_get_path, sample_config):
        mock_get_path.return_value = "/path/to/config.yml"
        mock_validate.return_value = sample_config

        yaml_content = yaml.dump(sample_config)
        with patch("builtins.open", mock_open(read_data=yaml_content)):
            result = ConfigLoader.load_the_config()

        assert result == sample_config
        mock_validate.assert_called_once_with(sample_config)

    @patch("epochai.common.config.config_loader.ConfigLoader._get_config_path")
    def test_load_config_file_not_found(self, mock_get_path):
        mock_get_path.return_value = "/nonexistent/config.yml"

        with patch("builtins.open", side_effect=FileNotFoundError("File not found")):
            with pytest.raises(FileNotFoundError, match="Config file not found at location"):
                ConfigLoader.load_the_config()

    @patch("epochai.common.config.config_loader.ConfigLoader._get_config_path")
    def test_load_config_yaml_error(self, mock_get_path):
        mock_get_path.return_value = "/path/to/config.yml"

        with patch("builtins.open", mock_open(read_data="invalid: yaml: content: [")):
            with pytest.raises(ValueError, match="Error in parsing config.yml"):
                ConfigLoader.load_the_config()

    @patch("epochai.common.config.config_loader.ConfigLoader._get_config_path")
    def test_load_config_unicode_error(self, mock_get_path):
        mock_get_path.return_value = "/path/to/config.yml"

        with patch("builtins.open", side_effect=UnicodeDecodeError("utf-8", b"", 0, 1, "error")):
            with pytest.raises(ValueError, match="UTF-8 encoding error"):
                ConfigLoader.load_the_config()

    @patch("epochai.common.config.config_loader.ConfigLoader._get_config_path")
    def test_load_config_none_result(self, mock_get_path):
        mock_get_path.return_value = "/path/to/config.yml"

        with patch("builtins.open", mock_open(read_data="")):
            with pytest.raises(ValueError, match="Config file returning as None"):
                ConfigLoader.load_the_config()


class TestConfigLoaderLoadConstraints:
    @patch("epochai.common.config.config_loader.ConfigLoader._get_config_path")
    def test_load_constraints_success(self, mock_get_path, sample_constraints):
        mock_get_path.return_value = "/path/to/constraints.yml"

        yaml_content = yaml.dump(sample_constraints)
        with patch("builtins.open", mock_open(read_data=yaml_content)):
            result = ConfigLoader.load_constraints_config()

        assert result == sample_constraints

    @patch("epochai.common.config.config_loader.ConfigLoader._get_config_path")
    def test_load_constraints_file_not_found(self, mock_get_path):
        mock_get_path.return_value = "/nonexistent/constraints.yml"

        with patch("builtins.open", side_effect=FileNotFoundError("File not found")):
            with pytest.raises(FileNotFoundError, match="Config file not found at location"):
                ConfigLoader.load_constraints_config()

    @patch("epochai.common.config.config_loader.ConfigLoader._get_config_path")
    def test_load_constraints_yaml_error(self, mock_get_path):
        mock_get_path.return_value = "/path/to/constraints.yml"

        with patch("builtins.open", mock_open(read_data="invalid: yaml: [")):
            with pytest.raises(ValueError, match="Error in parsing config.yml"):
                ConfigLoader.load_constraints_config()

    @patch("epochai.common.config.config_loader.ConfigLoader._get_config_path")
    def test_load_constraints_none_result(self, mock_get_path):
        mock_get_path.return_value = "/path/to/constraints.yml"

        with patch("builtins.open", mock_open(read_data="")):
            with pytest.raises(ValueError, match="File returning as None"):
                ConfigLoader.load_constraints_config()


class TestConfigMerging:
    def test_get_merged_config_success(self, sample_config):
        result = ConfigLoader.get_merged_config(sample_config, "wikipedia")

        # It should merge defaults with specific wikipedia config
        expected = {
            "api": {
                "collector_name": "wikipedia_collector",
                "language": ["en", "fr"],  # Overridden from specific config
                "rate_limit_delay": 0.5,  # Overridden
                "max_retries": 5,  # Overridden
                "search_max_results": 10,  # Overridden
                "request_timeout": 10,  # Overridden
                "recursive_limit": 1,  # From defaults (not overridden)
            },
        }

        assert result == expected

    def test_get_merged_config_no_defaults(self, sample_config):
        # Remove defaults section
        config_without_defaults = sample_config.copy()
        del config_without_defaults["defaults"]

        with pytest.raises(ValueError, match="No 'defaults' section found in config"):
            ConfigLoader.get_merged_config(config_without_defaults, "wikipedia")

    def test_get_merged_config_no_main_section(self, sample_config):
        # Remove wikipedia section
        config_without_main = sample_config.copy()
        del config_without_main["wikipedia"]

        with pytest.raises(ValueError, match="No 'wikipedia' section found in config"):
            ConfigLoader.get_merged_config(config_without_main, "wikipedia")

    def test_override_default_config_values_nested(self):
        defaults = {
            "api": {
                "rate_limit": 1.0,
                "retries": 3,
                "nested": {
                    "value1": "default",
                    "value2": "keep",
                },
            },
            "other": "default_value",
        }

        overrides = {
            "api": {
                "rate_limit": 0.5,
                "nested": {
                    "value1": "overridden",
                },
            },
        }

        result = ConfigLoader.override_default_config_values(defaults, overrides)

        expected = {
            "api": {
                "rate_limit": 0.5,
                "retries": 3,
                "nested": {
                    "value1": "overridden",
                    "value2": "keep",
                },
            },
            "other": "default_value",
        }

        assert result == expected

    def test_override_default_config_values_non_dict(self):
        # Test edge case where one of the values is not a dict
        result = ConfigLoader.override_default_config_values("default", "override")
        assert result == "override"


class TestConfigLoaderValidation:
    @patch("epochai.common.config.config_validator.ValidateWholeConfig.validate_config")
    @patch("epochai.common.config.config_loader.ConfigLoader.get_merged_config")
    def test_validate_whole_config(self, mock_merge, mock_validate, sample_config):
        mock_merge.return_value = {"merged": "config"}
        mock_validate.return_value = Mock()

        result = ConfigLoader.validate_whole_config(sample_config)

        # Should call get_merged_config for wikipedia
        mock_merge.assert_called_once_with(sample_config, "wikipedia")

        # Should call validate_config with the correct structure
        expected_parts = {
            "data_settings": sample_config["data_settings"],
            "logging": sample_config["logging"],
            "wikipedia": {"merged": "config"},
        }
        mock_validate.assert_called_once_with(expected_parts)

        assert result == sample_config


class TestConfigLoaderGetters:
    @patch("epochai.common.config.config_loader.ConfigLoader.load_the_config")
    def test_get_data_config(self, mock_load, sample_config):
        mock_load.return_value = sample_config

        result = ConfigLoader.get_data_config()

        assert result == sample_config["data_settings"]

    @patch("epochai.common.config.config_loader.ConfigLoader.load_the_config")
    def test_get_data_config_missing_section(self, mock_load):
        mock_load.return_value = {"other": "config"}

        result = ConfigLoader.get_data_config()

        assert result == {}

    @patch("epochai.common.config.config_loader.ConfigLoader.load_the_config")
    @patch("epochai.common.config.config_loader.ConfigLoader.get_merged_config")
    def test_get_wikipedia_yaml_config(self, mock_merge, mock_load, sample_config):
        mock_load.return_value = sample_config
        mock_merge.return_value = {"merged": "wikipedia_config"}

        result = ConfigLoader.get_wikipedia_yaml_config()

        mock_merge.assert_called_once_with(sample_config, "wikipedia")
        assert result == {"merged": "wikipedia_config"}

    @patch("epochai.common.config.config_loader.ConfigLoader.load_the_config")
    def test_get_metadata_schema_config(self, mock_load, sample_config):
        mock_load.return_value = sample_config

        result = ConfigLoader.get_metadata_schema_config()

        assert result == sample_config["metadata_schema"]

    @patch("epochai.common.config.config_loader.ConfigLoader.load_the_config")
    def test_get_metadata_schema_config_missing_section(self, mock_load):
        mock_load.return_value = {"other": "config"}

        result = ConfigLoader.get_metadata_schema_config()

        assert result is None

    @patch("epochai.common.config.config_loader.ConfigLoader.load_the_config")
    def test_get_logging_config(self, mock_load, sample_config):
        mock_load.return_value = sample_config

        result = ConfigLoader.get_logging_config()

        assert result == sample_config["logging"]

    @patch("epochai.common.config.config_loader.ConfigLoader.load_the_config")
    def test_get_logging_config_with_defaults(self, mock_load):
        mock_load.return_value = {"other": "config"}

        result = ConfigLoader.get_logging_config()

        expected_defaults = {
            "level": "INFO",
            "log_to_file": True,
            "log_directory": "logs",
        }
        assert result == expected_defaults


class TestConfigLoaderWikipediaTargetsConfig:
    @patch("epochai.common.services.collection_targets_query_service.CollectionTargetsQueryService")
    def test_get_wikipedia_targets_config_success(self, mock_service_class):
        mock_service = Mock()
        mock_service_class.return_value = mock_service
        mock_service.get_wikipedia_targets_config.return_value = {"targets": "config"}

        result = ConfigLoader.get_wikipedia_targets_config(
            collector_name="test_collector",
            collection_status="active",
            collection_types=["articles"],
            language_codes=["en"],
            target_ids=[1, 2, 3],
        )

        mock_service_class.assert_called_once()
        mock_service.get_wikipedia_targets_config.assert_called_once_with(
            collector_name="test_collector",
            collection_status="active",
            collection_types=["articles"],
            language_codes=["en"],
            target_ids=[1, 2, 3],
        )
        assert result == {"targets": "config"}

    @patch("epochai.common.services.collection_targets_query_service.CollectionTargetsQueryService")
    def test_get_wikipedia_targets_config_minimal_params(self, mock_service_class):
        mock_service = Mock()
        mock_service_class.return_value = mock_service
        mock_service.get_wikipedia_targets_config.return_value = {"targets": "minimal"}

        result = ConfigLoader.get_wikipedia_targets_config(
            collector_name="minimal_collector",
            collection_status="pending",
        )

        mock_service.get_wikipedia_targets_config.assert_called_once_with(
            collector_name="minimal_collector",
            collection_status="pending",
            collection_types=None,
            language_codes=None,
            target_ids=None,
        )
        assert result == {"targets": "minimal"}

    @patch("epochai.common.services.collection_targets_query_service.CollectionTargetsQueryService")
    def test_get_wikipedia_targets_config_service_exception(self, mock_service_class):
        mock_service = Mock()
        mock_service_class.return_value = mock_service
        mock_service.get_wikipedia_targets_config.side_effect = Exception("Database error")

        with pytest.raises(Exception, match="Database error"):
            ConfigLoader.get_wikipedia_targets_config(
                collector_name="error_collector",
                collection_status="active",
            )
