from typing import List, Set

from pydantic import BaseModel, model_validator


class ConfigValidator:
    @staticmethod
    def get_constraints_config():
        """
        Gets constraints config via config_loader.py
        """
        from epochai.common.config.config_loader import ConfigLoader

        return ConfigLoader.load_constraints_config()


class DatabaseConfig(BaseModel):
    save_to_database: bool
    batch_size: int

    @model_validator(mode="after")
    def validate_using_constraints(self):
        constraints_config = ConfigValidator.get_constraints_config()

        database = constraints_config["data_output"]["database"]

        min_batch_size = database["min_batch_size"]
        max_batch_size = database["max_batch_size"]

        if not (min_batch_size <= self.batch_size <= max_batch_size):
            raise ValueError(
                f"batch_size currently {self.batch_size}, must be: {min_batch_size} <= batch_size <= {max_batch_size}",
            )

        return self


class DataOutputConfig(BaseModel):
    directory: str
    default_type_wikipedia: str
    separate_files_by_year: bool
    file_format: str
    database: DatabaseConfig

    @model_validator(mode="after")
    def validate_using_constraints(self):
        constraints_config = ConfigValidator.get_constraints_config()

        data_output_constraints = constraints_config["data_output"]

        allowed_formats = data_output_constraints.get("allowed_formats")
        if self.file_format.lower() not in allowed_formats:
            raise ValueError(
                f"Current file format is '{self.file_format}', must be one of: {allowed_formats}",
            )

        return self


class DataValidatorConfig(BaseModel):
    validate_before_save: bool
    min_content_length: int
    error_logging_limit: int
    utf8_corruption_patterns: List[str]
    required_fields_wikipedia: Set[str]

    @model_validator(mode="after")
    def validate_using_constraints(self):
        constraints_config = ConfigValidator.get_constraints_config()

        data_validator_constraints = constraints_config.get("data_validator", {})

        min_content_length = data_validator_constraints.get("min_content_length")
        if not (self.min_content_length >= min_content_length):
            raise ValueError(
                f"min_content_length currently '{self.min_content_length}', must be: min_content_length >= {min_content_length}",
            )

        error_logging_limit = data_validator_constraints.get("error_logging_limit")
        if not (self.error_logging_limit >= error_logging_limit):
            raise ValueError(
                f"error_logging_limit currently '{self.error_logging_limit}', must be: error_logging_limit >= {error_logging_limit}",  # noqa
            )

        return self


class CleanerBaseConfig(BaseModel):
    cleaner_name: str
    current_schema_version: str


class CleanersConfig(BaseModel):
    wikipedia: CleanerBaseConfig
    fivethirtyeight: CleanerBaseConfig


class WikipediaDefaultApiConfig(BaseModel):
    language: List[str]
    rate_limit_delay: float
    max_retries: int
    search_max_results: int
    request_timeout: int
    recursive_limit: int

    @model_validator(mode="after")
    def validate_using_constraints(self):
        constraints_config = ConfigValidator.get_constraints_config()

        wikipedia_constraints = constraints_config.get("wikipedia", {})
        api_constraints = wikipedia_constraints.get("api", {})

        min_rate_limit_delay = api_constraints.get("min_rate_limit_delay")
        if self.rate_limit_delay < min_rate_limit_delay:
            raise ValueError(
                f"rate_limit_delay is currently '{self.rate_limit_delay}', must be: rate_limit_delay >= {min_rate_limit_delay}",
            )

        min_retries = api_constraints.get("min_retries")
        max_retries = api_constraints.get("max_retries")
        if not (min_retries <= self.max_retries <= max_retries):
            raise ValueError(
                f"max_retries is currently '{self.max_retries}', must be: {min_retries} <= max_retries <= {max_retries}",
            )

        search_max_results = api_constraints.get("search_max_results")
        if not (self.search_max_results <= search_max_results):
            raise ValueError(
                f"search_max_results is currently '{self.search_max_results}', must be: search_max_results <= {search_max_results}",  # noqa
            )

        min_timeout = api_constraints.get("min_request_timeout")
        if self.request_timeout < min_timeout:
            raise ValueError(
                f"request_timeout is currently '{self.request_timeout}', must be: request_timeout >= {min_timeout}",
            )

        min_recursive_limit = api_constraints.get("min_recursive_limit")
        max_recursive_limit = api_constraints.get("max_recursive_limit")
        if not (min_recursive_limit <= self.recursive_limit <= max_recursive_limit):
            if self.recursive_limit > max_recursive_limit:
                raise ValueError(
                    f"recursive_limit is currently '{self.recursive_limit}' which is greater than max: {max_recursive_limit}. Just use iteration at this point.",  # noqa
                ) from ValueError
            raise ValueError(
                f"Current recursive_limit is currently '{self.recursive_limit}' but must be: {min_recursive_limit} <= recursive_limit <= {max_recursive_limit}",  # noqa
            ) from ValueError

        return self


class FiveThirtyEightApiConfig(BaseModel):
    language: str
    rate_limit_delay: float


class WikipediaDefaultConfig(BaseModel):
    collector_name: str
    current_schema_version: str
    api: WikipediaDefaultApiConfig


class FiveThirtyEightDefaultConfig(BaseModel):
    collector_name: str
    current_schema_version: str
    api: FiveThirtyEightApiConfig


class DefaultsConfig(BaseModel):
    wikipedia: WikipediaDefaultConfig
    fivethirtyeight: FiveThirtyEightDefaultConfig


class DataSettings(BaseModel):
    data_output: DataOutputConfig
    data_validator: DataValidatorConfig
    cleaners: CleanersConfig


class LoggingConfig(BaseModel):
    level: str
    log_to_file: bool
    log_directory: str

    @model_validator(mode="after")
    def validate_logging_config(self):
        constraints_config = ConfigValidator.get_constraints_config()

        logging_constraints = constraints_config.get("logging_config")

        valid_levels = logging_constraints.get("valid_levels")
        if self.level.upper() not in valid_levels:
            raise ValueError(f"level currently '{self.level}', level must be one of: {sorted(valid_levels)}")

        return self


class WikipediaApiConfig(BaseModel):
    language: List[str]
    rate_limit_delay: float
    max_retries: int
    search_max_results: int
    request_timeout: int
    recursive_limit: int

    @model_validator(mode="after")
    def validate_using_constraints(self):
        constraints_config = ConfigValidator.get_constraints_config()

        wikipedia_constraints = constraints_config.get("wikipedia", {})
        api_constraints = wikipedia_constraints.get("api", {})

        min_rate_limit_delay = api_constraints.get("min_rate_limit_delay")
        if self.rate_limit_delay < min_rate_limit_delay:
            raise ValueError(
                f"rate_limit_delay is currently '{self.rate_limit_delay}', must be: rate_limit_delay >= {min_rate_limit_delay}",
            )

        min_retries = api_constraints.get("min_retries")
        max_retries = api_constraints.get("max_retries")
        if not (min_retries <= self.max_retries <= max_retries):
            raise ValueError(
                f"max_retries is currently '{self.max_retries}', must be: {min_retries} <= max_retries <= {max_retries}",
            )

        search_max_results = api_constraints.get("search_max_results")
        if not (self.search_max_results <= search_max_results):
            raise ValueError(
                f"search_max_results is currently '{self.search_max_results}', must be: search_max_results <= {search_max_results}",  # noqa
            )

        min_timeout = api_constraints.get("min_request_timeout")
        if self.request_timeout < min_timeout:
            raise ValueError(
                f"request_timeout is currently '{self.request_timeout}', must be: request_timeout >= {min_timeout}",
            )

        min_recursive_limit = api_constraints.get("min_recursive_limit")
        max_recursive_limit = api_constraints.get("max_recursive_limit")
        if not (min_recursive_limit <= self.recursive_limit <= max_recursive_limit):
            if self.recursive_limit > max_recursive_limit:
                raise ValueError(
                    f"recursive_limit is currently '{self.recursive_limit}' which is greater than max: {max_recursive_limit}. Just use iteration at this point.",  # noqa
                ) from ValueError
            raise ValueError(
                f"Current recursive_limit is currently '{self.recursive_limit}' but must be: {min_recursive_limit} <= recursive_limit <= {max_recursive_limit}",  # noqa
            ) from ValueError

        return self


class WikipediaConfig(BaseModel):
    api: WikipediaApiConfig


class FiveThirtyEightConfig(BaseModel):
    api: FiveThirtyEightApiConfig


class ValidateWholeConfig(BaseModel):
    data_settings: DataSettings
    logging: LoggingConfig
    wikipedia: WikipediaConfig
    fivethirtyeight: FiveThirtyEightConfig
    defaults: DefaultsConfig

    @classmethod
    def validate_config(cls, config):
        """
        Validates config dictionary

        Return:
            The parent class (ValidateWholeConfig) as a pydantic model instance
        """
        try:
            return cls(**config)
        except Exception as general_error:
            raise ValueError(f"Error validating config: {general_error}") from general_error
