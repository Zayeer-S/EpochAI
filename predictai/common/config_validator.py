class ConfigValidator:
    """Validates configuration for all modules"""
    
    CORRUPTION_PATTERNS = [
        'Ã©', 'Ã¨', 'Ã ', 'Ã¡', 'Ã¢', 'Ã¤', 'Ã§', 'Ã¯', 'Ã´', 'Ã¹', 'Ã»', 'Ã¼',
        'â€™', 'â€œ', 'â€', 'â€¢', 'â€"', 'â€"', 'Â', 'Ž', 'â€ž', 'â€º'
    ]
    
    @staticmethod
    def validate_wikipedia_config(config):
        """Validates only wikipedia collector configuration"""
        
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
            (['data_output', 'default_type'], str)
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
                    if not isinstance(value, list) or len(value) == 0:
                        raise ValueError("language must be a non-empty list")
                    for lang in value:
                        if not isinstance(lang, str):
                            raise ValueError("All language codes must be strings")
             
            except KeyError:
                raise ValueError(f"Missing required config: {'.'.join(path)}")
            
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