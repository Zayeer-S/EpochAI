from pydantic import BaseModel, model_validator
from typing import Dict, List, Set

class ConfigValidator:
    @staticmethod
    def _get_constraints_config():
        from predictai.common.config_loader import ConfigLoader
        return ConfigLoader.load_constraints_config()
                
class DataOutputConfig(BaseModel):
    directory: str
    default_type_wikipedia: str
    separate_files_by_year: bool    
    file_format: str
    
    @model_validator(mode='after')
    def validate_using_constraints(self):
        constraints_config = ConfigValidator._get_constraints_config()
        
        data_output_constraints = constraints_config['data_output']
        
        allowed_formats = data_output_constraints.get('allowed_formats')
        if self.file_format.lower() not in allowed_formats:
            raise ValueError(f"Current file format is '{self.file_format}', must be one of: {allowed_formats}")
        
        return self
        
class DataValidatorConfig(BaseModel):
    validate_before_save: bool
    min_content_length: int
    error_logging_limit: int
    utf8_corruption_patterns: List[str]
    required_fields_wikipedia: Set[str]
    
    @model_validator(mode='after')
    def validate_using_constraints(self):
        constraints_config = ConfigValidator._get_constraints_config()
        
        data_validator_constraints = constraints_config.get('data_validator', {})
        
        min_content_length = data_validator_constraints.get('min_content_length')
        if not (self.min_content_length >= min_content_length):
            raise ValueError(f"min_content_length currently '{self.min_content_length}', must be: min_content_length >= {min_content_length}")
        
        error_logging_limit = data_validator_constraints.get('error_logging_limit')
        if not (self.error_logging_limit >= error_logging_limit):
            raise ValueError(f"error_logging_limit currently '{self.error_logging_limit}', must be: error_logging_limit >= {error_logging_limit}")
        
        return self
    
class DataSettings(BaseModel):
    data_output: DataOutputConfig
    data_validator: DataValidatorConfig
        
class LoggingConfig(BaseModel):
    level: str
    log_to_file: bool
    log_directory: str
    
    @model_validator(mode='after')
    def validate_logging_config(self):
        constraints_config = ConfigValidator._get_constraints_config()
        
        logging_constraints = constraints_config.get('logging_config')
        
        valid_levels = logging_constraints.get('valid_levels')
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
    
    @model_validator(mode='after')
    def validate_using_constraints(self):
        constraints_config = ConfigValidator._get_constraints_config()
        
        wikipedia_constraints = constraints_config.get('wikipedia', {})
        api_constraints = wikipedia_constraints.get('api', {})
        
        min_rate_limit_delay = api_constraints.get('min_rate_limit_delay')
        if self.rate_limit_delay < min_rate_limit_delay:
            raise ValueError(f"rate_limit_delay is currently '{self.rate_limit_delay}', must be: rate_limit_delay >= {min_rate_limit_delay}")
        
        min_retries = api_constraints.get('min_retries')
        max_retries = api_constraints.get('max_retries')
        if not (min_retries <= self.max_retries <= max_retries):
            raise ValueError(f"max_retries is currently '{self.max_retries}', must be: {min_retries} <= max_retries <= {max_retries}")
        
        search_max_results = api_constraints.get('search_max_results')
        if not (self.search_max_results <= search_max_results):
            raise ValueError(f"search_max_results is currently '{self.search_max_results}', must be: search_max_results <= {search_max_results}")

        min_timeout = api_constraints.get('min_request_timeout')
        if self.request_timeout < min_timeout:
            raise ValueError(f"request_timeout is currently '{self.request_timeout}', must be: request_timeout >= {min_timeout}")
        
        min_recursive_limit = api_constraints.get('min_recursive_limit')
        max_recursive_limit = api_constraints.get('max_recursive_limit')
        if not (min_recursive_limit <= self.recursive_limit <= max_recursive_limit):
            if (self.recursive_limit > max_recursive_limit):
                raise ValueError(f"recursive_limit is currently '{self.recursive_limit}' which is greater than max: {max_recursive_limit}. Just use iteration at this point.")
            raise ValueError(f"Current recursive_limit is currently '{self.recursive_limit}' but must be: {min_recursive_limit} <= recursive_limit <= {max_recursive_limit}")
        
        return self
    
class WikipediaConfig(BaseModel):
    collection_years: List[int]
    politicians: Dict[str, List[str]]
    political_topics: Dict[str, List[str]]
    political_events_template: Dict[str, List[str]]
    api: WikipediaApiConfig
    
    @model_validator(mode="after")
    def validate_constraints(self):
        constraints_config = ConfigValidator._get_constraints_config()
        wikipedia_constraints = constraints_config.get('wikipedia')
        
        valid_language_codes = wikipedia_constraints.get('valid_2iso_language_codes')
        
        all_language_codes = set()
        all_language_codes.update(self.politicians.keys())
        all_language_codes.update(self.political_topics.keys())
        all_language_codes.update(self.political_events_template.keys())
        
        invalid_language_codes = all_language_codes - set(valid_language_codes)
        if invalid_language_codes:
            raise ValueError(f"Invalid language codes: {invalid_language_codes}. Valid codes {valid_language_codes}")
        
        return self
    
class ValidateWholeConfig(BaseModel):    
    data_settings: DataSettings
    logging: LoggingConfig
    wikipedia: WikipediaConfig
    
    @classmethod
    def validate_config(cls, config):
        """Returns config as an instance of a class unpacked into keyword arguments"""
        try:
            return cls(**config)
        except Exception as general_error:
            raise ValueError(f"Error validating config: {general_error}")
    