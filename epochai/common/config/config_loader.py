import contextlib
import locale
import os
from typing import Any, Dict

import yaml

from epochai.common.config.config_validator import ValidateWholeConfig

for locale_name in ["en_US.UTF-8", "C.UTF-8"]:
    with contextlib.suppress(locale.Error):
        locale.setlocale(locale.LC_ALL, locale_name)
        break


class ConfigLoader:
    @staticmethod
    def _get_config_path(filename: str) -> str:
        """Gets config path of config.yml and constraints.yml"""
        current_dir = os.path.dirname(__file__)
        project_root = os.path.abspath(os.path.join(current_dir, "..", "..", ".."))
        config_path = os.path.join(project_root, f"{filename}")
        return config_path

    @staticmethod
    def load_the_config() -> Dict[str, Any]:
        """
        Loads the config from config.yaml

        Returns:
            Validated config (validated via helper function and config_validator)
        """
        config: Dict[str, Any]
        config_path = ConfigLoader._get_config_path("config.yml")

        try:
            with open(config_path, encoding="utf-8") as file:
                config = yaml.safe_load(file)
            if config is None:
                raise ValueError(f"Config file returning as None: {config}")

            ConfigLoader.validate_whole_config(config)

            return config

        except FileNotFoundError as file_not_found_error:
            raise FileNotFoundError(
                f"Config file not found at location: {config_path} - {file_not_found_error}",
            ) from file_not_found_error

        except yaml.YAMLError as yaml_error:
            raise ValueError(f"Error in parsing config.yml: {yaml_error}") from yaml_error

        except UnicodeDecodeError as unicode_error:
            raise ValueError(f"UTF-8 encoding error in {config_path}: {unicode_error}") from unicode_error

    @staticmethod
    def validate_whole_config(
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Validates parts of config that need validation via ConfigValidator

        Returns:
            The config (dict) originally sent to this
        """
        merged_wikipedia_config = ConfigLoader.get_merged_config(config, "wikipedia")

        config_parts_to_validate = {
            "data_settings": config.get("data_settings"),
            "logging": config.get("logging"),
            "wikipedia": merged_wikipedia_config,
        }

        ValidateWholeConfig.validate_config(config_parts_to_validate)

        return config

    @staticmethod
    def load_constraints_config() -> Dict[str, Any]:
        """Loads the constraints config"""
        constraints_config: Dict[str, Any]
        config_path = ConfigLoader._get_config_path("constraints.yml")

        try:
            with open(config_path) as file:
                constraints_config = yaml.safe_load(file)
            if constraints_config is None:
                raise ValueError(f"File returning as None: {constraints_config}")
            return constraints_config

        except FileNotFoundError as file_not_found_error:
            raise FileNotFoundError(
                f"Config file not found at location: {config_path} - {file_not_found_error}",
            ) from file_not_found_error

        except yaml.YAMLError as yaml_error:
            raise ValueError(f"Error in parsing config.yml: {yaml_error}") from yaml_error

    @staticmethod
    def get_merged_config(
        config: Dict[str, Any],
        config_name: str,
    ) -> Dict[str, Any]:
        """
        Takes the default config and the override config and merges them via a helper function

        Returns:
            Merged config (default config settings overriden by override config)
        """
        all_defaults = config.get("defaults")
        if all_defaults is None:
            raise ValueError("No 'defaults' section found in config for any collector")

        relevant_default = all_defaults.get(config_name)
        if relevant_default is None:
            raise ValueError(f"No 'defaults' section found in config for {config_name}")

        main_config = config.get(config_name)
        if main_config is None:
            raise ValueError(f"No '{config_name}' section found in config")

        merged_config = ConfigLoader.override_default_config_values(relevant_default, main_config)
        return merged_config

    @staticmethod
    def override_default_config_values(
        defaults: Dict[str, Any],
        overrides: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Recursively merges two dictionaries (default and specific) from config.yml"""
        result: Dict[str, Any]

        # mypy thinks this is unreachable so tell it to ignore
        if not isinstance(defaults, dict) or not isinstance(overrides, dict):
            return overrides  # type: ignore[unreachable]

        result = defaults.copy()

        for key, value in overrides.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = ConfigLoader.override_default_config_values(result[key], value)
            else:
                result[key] = value

        return result

    @staticmethod
    def get_data_config() -> Dict[str, Any]:
        """Gets just the YAML data_settings portion of the config"""
        whole_config = ConfigLoader.load_the_config()

        data_settings_config: Dict[str, Any] = whole_config.get("data_settings", {})

        return data_settings_config

    @staticmethod
    def get_wikipedia_yaml_config() -> Dict[str, Any]:
        """Gets Wikipedia collector (YAML only) configuration with defaults applied and validates it"""
        config = ConfigLoader.load_the_config()

        merged_config = ConfigLoader.get_merged_config(config, "wikipedia")

        return merged_config

    @staticmethod
    def get_logging_config() -> Dict[str, Any]:
        """Get logging configuration and validate it"""
        config = ConfigLoader.load_the_config()
        logging_config: Dict[str, Any] = config.get(
            "logging",
            {
                "level": "INFO",
                "log_to_file": True,
                "log_directory": "logs",
            },
        )

        return logging_config

    """@staticmethod
    def get_all_collector_configs() -> Dict[str, Any]:
        Gets all collector configs

        all_configs = {}

        try:
            wikipedia_yaml_config = ConfigLoader.get_wikipedia_yaml_config()
            collector_name = wikipedia_yaml_config["collector_name"]

            from epochai.common.database.collection_targets_manager import CollectionTargetManager

            all_configs["wikipedia"] = CollectionTargetManager.get_combined_wikipedia_target_config(
                collector_name=collector_name,
            )
        except Exception as e:
            print(f"Could not load wikipedia collector: '{e}'")
            all_configs["wikipedia"] = None

        return all_configs"""

    """@staticmethod
    def get_wikipedia_config(
        collector_name: str,
        collection_types: Optional[List[str]],
        language_codes: Optional[List[str]],
        target_ids: Optional[List[int]],
    ) -> Dict[str, Any]:
        ""Gets whole Wikipedia Config (combination of YAML + DB) and returns it""
        yaml_config = ConfigLoader.get_wikipedia_yaml_config()

        from epochai.common.database.collection_targets_manager import CollectionTargetManager

        result: Dict[str, Any] = CollectionTargetManager.get_combined_wikipedia_target_config(
            yaml_config=yaml_config,
            collector_name=collector_name,
            collection_type=collection_types,
            language_code=language_codes,
            target_id=target_ids,
        )

        return result"""

    @staticmethod
    def get_collection_status_summary() -> Dict[str, Any]:
        """Gets collection status summary"""
        from epochai.common.database.collection_targets_manager import CollectionTargetManager

        result: Dict[str, Any] = CollectionTargetManager.get_collection_status_summary()  # type: ignore

        return result
