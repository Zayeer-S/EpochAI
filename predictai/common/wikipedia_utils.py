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
