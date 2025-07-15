class ConfigValidator:
    """Validates configuration for all modules"""
    
    VALID_ISO_LANGUAGE_CODES = {
        'en', 'fr', 'de', 'ru', 'it', 'pt', 'pl', 'nl', 'sr', 'ro', # European
        
        'ar', 'tr', 'fa', 'he', # MENA
        
        'ur', 'bn', 'hi', # South asian
        
        'zh', 'ko', 'ja', # East asian
        
        'id', 'vi' # SEA
    }
    
    CORRUPTION_PATTERNS = [
        'Ã©', 'Ã¨', 'Ã ', 'Ã¡', 'Ã¢', 'Ã¤', 'Ã§', 'Ã¯', 'Ã´', 'Ã¹', 'Ã»', 'Ã¼',
        'â€™', 'â€œ', 'â€', 'â€¢', 'â€"', 'â€"', 'Â', 'Ž', 'â€ž', 'â€º'
    ]
    
    @staticmethod
    def validate_wikipedia_config(config):
        """Validates only wikipedia collector configuration by checking for API settings and checks for any UTF-8 corruption"""
        
        required_paths = [
            (['api', 'language'], list),
            (['api', 'rate_limit_delay'], (int, float)),
            (['api', 'max_retries'], int),
            (['api', 'search_max_results'], int),
            (['collection_years'], list),
            (['politicians'], dict),
            (['political_topics'], dict),
            (['political_events_template'], dict),
            (['data_output', 'directory'], str),
            (['data_output', 'default_type_wikipedia'], str)
        ]
        
        for path, expected_type in required_paths:  
            try:
                value = config
                for key in path:
                    value = value[key]
                    
                if not isinstance(value, expected_type):
                    raise ValueError(f"Config {'.'.join(path)} must be of type {expected_type.__name__}, got {type(value).__name__}")
                
                if path == ['api', 'rate_limit_delay'] and value < 0.2:
                    raise ValueError(f"rate_limit_delay must be >= 0.2")
                
                if path == ['api', 'max_retries'] and (value < 1 or value > 10):
                    raise ValueError("max_retries must be 1 <= max_retries <= 10")
                
                if path == ['api', 'search_max_results'] and (value < 1 or value > 15):
                    raise ValueError("search_max_results must be 1 <= search_max_results <= 15")
                
                if path == ['collection_years'] and len(value) == 0:
                    raise ValueError("collection_years cannot be empty")
                
                if path == ['api', 'language']:
                    ConfigValidator._validate_language_codes(value)
             
            except KeyError:
                raise ValueError(f"Missing required config: {'.'.join(path)}")
            
        ConfigValidator._validate_utf8_content(config)
            
    @staticmethod
    def validate_logging_config(config):
        """Validate logging configuration"""
        
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        
        level = config.get('level', 'INFO')
        if level.upper() not in valid_levels:
            raise ValueError(f"Invalid log level '{level}'. Must be one of: {valid_levels}")
        
        log_to_file = config.get('log_to_file', True)
        if not isinstance(log_to_file, bool):
            raise ValueError("log_to_file must be a bool")
        
        log_dir = config.get('log_directory', 'logs')
        if not isinstance(log_dir, str):
            raise ValueError("log_to_directory must be a string")
        
        ConfigValidator._check_string_for_corruption(level, "log level")
        ConfigValidator._check_string_for_corruption(log_dir, "log directory")
        
    @staticmethod
    def _validate_language_codes(language_list):
        """Validates language codes in config are valid/accepted ISO codes and are properly formatted"""
        if not isinstance(language_list, list) or len(language_list) == 0:
            raise ValueError("Language must be a non-empty list")
        
        for language in language_list:
            if not isinstance(language, str):
                raise ValueError(f"Language codes must be a string, got {type(language).__name__}: {language}")
            
            ConfigValidator._check_string_for_corruption(language, f"Language code: '{language}'")
            
            if language.lower() not in ConfigValidator.VALID_ISO_LANGUAGE_CODES:
                raise ValueError (
                    f"Invalid language code: '{language}'. Must be one of: {sorted(ConfigValidator.VALID_ISO_LANGUAGE_CODES)}"
                )
                
    @staticmethod
    def _validate_utf8_content(config, path=''):
        """Recursively validates UTF-8 content in the config"""
        if isinstance(config, dict):
            for key, value in config.items():
                current_path = f"{path}.{key}" if path else key
                
                if isinstance(key, str):
                    ConfigValidator._check_string_for_corruption(key, f"config key '{current_path}'")
                    
                ConfigValidator._validate_utf8_content(value, current_path)
                
        elif isinstance(config, list):
            for i, item in enumerate(config):
                current_path = f"{path}[{i}]"
                ConfigValidator._validate_utf8_content(item, current_path)
                
        elif isinstance(config, str):
            ConfigValidator._check_string_for_corruption(config, f"config value '{path}'")
            
    @staticmethod
    def _check_string_for_corruption(text, context="string"):
        """Checks for common UTF-8 corruption patterns"""
        if not isinstance(text, str):
            raise ValueError(f"{context} must be a string but got type: {type(text).__name__}: {text}")
        
        for pattern in ConfigValidator.CORRUPTION_PATTERNS:
            if pattern in text:
                raise ValueError(
                    f"UTF-8 corruption pattern detected in {context}: '{text}' containts '{pattern}'"
                )
                
        try:
            text.encode('utf-8').decode('utf-8')
        except UnicodeError as e:
            raise ValueError(f"Unicode error in {context}: '{text}' - {e}")