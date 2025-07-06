from predictai.common.logging_config import get_logger

class WikipediaUtils:
    """Shared Wikipedia API Utils across collection and debug files"""
    
def __init__(
    self,
    config
    ):
    
    self.config = config
    self.logger = get_logger(__name__)
