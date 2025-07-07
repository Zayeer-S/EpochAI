import time
from typing import Any, Callable, Dict, List

import wikipedia

from predictai.common.logging_config import get_logger

class WikipediaUtils:
    """Shared Wikipedia API Utils across collection and debug files"""
    
def __init__(
    self,
    config
    ):
    
    self.config = config
    self.logger = get_logger(__name__)
        
def switch_language(
    self,
    language_code: str
    ) -> bool:
    """
    Switches language for Wikipedia API. True if successful switch and vice versa.
    """
    
    try: 
        wikipedia.set_lang(language_code)
        self.logger.info(f"Language successfully switched to '{language_code}'")
        return True
    
    except Exception as e:
        self.logger.error(f"Error switching to language: {language_code} - {e}")
        return False
    
def process_items_by_language(
    self, 
    items_by_language_code: Dict[str, List[str]], 
    process_func: Callable
    ) -> Dict[str, List[Any]]:
    """
    Processes items by language and switches language via helper function.
    Uses process_func to call a relevant function.
    
    Args:
        process_func: Function that is called for each item. Must accept 2 parameters:
            (item: str, language_code: str) and returns result or None
    
    Returns:
        Dict mapping language codes to a nullable list 
        e.g.'language_code': [item1, item2, etc]
            'language_code': [null]
    """
    
    results_by_language = {}
    
    for language_code, items in items_by_language_code.items():
        if not items:
            self.logger.warning(f"No items for this language '{language_code}', skipping this language...")
            continue
        
        if not self.switch_language(language_code):
            results_by_language[language_code] = []
            continue
        
        results_by_language[language_code] = []
        
        for item in items:
            try:
                result = process_func(item, language_code)
                if result:
                    results_by_language[language_code].append(result)
                    
                time.sleep(self.config['api']['rate_limit_delay'])
                
            except Exception as e:
                self.logger.error(f"Error processing '{item} in '{language_code}': {e}")
                continue
            
    return results_by_language

