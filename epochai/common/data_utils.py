import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from pathlib import Path

from epochai.common.logging_config import get_logger

class DataUtils:
    def __init__(self, config):
        try:
            if not config:
                raise ValueError(f"Config cannot be none or empty")
            
            self.config = config
            self.logger = get_logger(__name__)
            
            self.incremental_save_counter = 0
            self.current_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            
        except ImportError as import_error:
            raise ImportError(f"Error importing modules: {import_error}")
        
        except Exception as general_error:
            self.logger.error(f"General Error: {general_error}")
            
    def _save_common(
    self,
    collected_data: List[Dict[str, Any]],
    data_type: str,
    file_format: str = None,
    output_directory: str = None,
    validate_before_save: bool = None,
    incremental_saving: bool = None
    ) -> Optional[Tuple[str, str, bool]]:
        """
        Common functionalities both incremental and at end save use
        
        Notes:
            Checks if there is collected_data and validates it, gets config values
            
        Returns (in order):
            file_format, filepath, incremental_saving (all str)
        """
        if not collected_data:
            self.logger.warning("No data provided to save")
            return None
        
        if file_format is None:
            file_format = self.config['data_output']['file_format']
        if output_directory is None:
            output_directory = self.config['data_output']['directory']
        if validate_before_save is None:
            validate_before_save = self.config['data_validator']['validate_before_save']
        if incremental_saving is None:
            incremental_saving = self.config['data_output']['incremental_saving']['enabled']
            
        if validate_before_save:
            if not self.validate_data_structure_and_quality(collected_data):
                self.logger.error("Data validation failed, cannot save invalid data")
                return None
            
        filename = f"{data_type}_{self.current_timestamp}.{file_format}"
            
        Path(output_directory).mkdir(parents=True, exist_ok=True)
        filepath = os.path.join(output_directory, filename)
        
        if any(x is None for x in (file_format, filepath, incremental_saving)):
            self.logger.error(f"Error one of these three is None: file_format: {file_format} - filepath: {filepath} - incremental_saving: {incremental_saving}")
            return None
        
        return file_format, filepath, incremental_saving
            
    def save_at_end(
        self,
        collected_data: List[Dict[str, Any]],
        data_type: str
    ) -> Optional[str]:
        """
        Saves data to a single file at the end of collection
        
        Args:
            data_type (str): the type of data being collected by the collector
        
        Returns:
            filepath (str) of the file where the save occurred
            
        Note:
            Use this for small collections. Fails gracefully!
        """
        self.logger.info("Starting save attempt")
        
        result = self._save_common(
            collected_data,
            data_type,
        )
        if result is None:
            self.logger.error(f"{self._save_common.__name__} is returning None")
            return None
        
        file_format, filepath, incremental_saving = result
        
        if incremental_saving:
            self.logger.error(f"Save at end being called even though incremental_saving is True: {incremental_saving}")
            return None
        
        df = pd.DataFrame(collected_data)
        
        try:
            file_format_lowered = file_format.lower()
            if file_format_lowered == 'json':
                df.to_json(filepath, orient='records', indent=2)
            elif file_format_lowered == 'xlsx' or file_format_lowered == 'excel':
                df.to_excel(filepath, index=False)
            else:
                if file_format_lowered != 'csv':
                    self.logger.warning(f"File format in config is wrong, currently: {file_format_lowered} - Still saving data to {filepath} as .csv")
                filepath = filepath.replace(f'.{file_format}', '.csv')
                df.to_csv(filepath, index=False)
                
        except Exception as general_error:
            self.logger.error(f"Error saving data to '{filepath}': {general_error}")
            return None
        
        self.logger.info(f"Data saved to: {filepath}")
        self.logger.info(f"Total records saved: {len(df)}")
        
        return filepath
        
    def get_data_summary(
        self,
        collected_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """ 
        Generates data summary statistics for collected data
        
        Returns:
            Dictionary containing summary statistics
        """
        if not collected_data:
            return {"total_records": 0}
        
        df = pd.DataFrame(collected_data)
        summary = {
            'total_records': len(df),
            'columns': list(df.columns),
        }
        
        if 'content' in df.columns:
            content_lengths = df['content'].str.len()
            summary["content_stats"] = {
                "average_length": content_lengths.mean(),
                "median_length": content_lengths.median(),
                "min_length": content_lengths.min(),
                "max_length": content_lengths.max()
            }
            
        if 'language' in df.columns:
            summary["language_distribution"] = df['language'].value_counts().to_dict()
            
        return summary
    
    def log_data_summary(
        self,
        collected_data: List[Dict[str, Any]]
        ) -> None:
        """
        Logs summary statistcs and gets them from helper function
        """
        summary = self.get_data_summary(collected_data)
        
        self.logger.info("="*30)
        self.logger.info("Data Summary Statistics")
        self.logger.info("="*30)
        self.logger.info(f"Total records: {summary['total_records']}")
        
        if 'content_stats' in summary:
            stats = summary["content_stats"]
            self.logger.info(f"Average content length: {stats['average_length']:.0f} characters")
            self.logger.info(f"Content length range: {stats['min_length']:.0f} - {stats['max_length']:.0f}")
            
        if 'language_distribution' in summary:
            self.logger.info(f"Records by language: ")
            for language, count in summary["language_distribution"].items():
                self.logger.info(f"{language}: {count}")
        
        self.logger.info("="*30)
        
    def validate_data_structure_and_quality(
        self,
        collected_data: List[Dict[str, Any]],
        required_fields: Optional[Set[str]] = None,
        check_content_quality: bool = True
    ) -> bool:
        """
        Validates structure and quality of collected data.
        
        Returns:
            bool: True if all validation passes, False if otherwise
        """
        if not collected_data:
            self.logger.error("Data validation failed as no data was provided.")
            return False
        if not isinstance(collected_data, list):
            self.logger.error(f"Data validation failed; Expected list but got {type(collected_data)}")
            return False
        
        if required_fields is None:
            required_fields = self.config['data_validator']['required_fields_wikipedia']
            
        if isinstance(required_fields, list):
            required_fields = set(required_fields)
        elif not isinstance(required_fields, set):
            self.logger.error(f"required_fields currently {type(required_fields)}, must be a list or a set.")
            return False
            
        validation_errors = []
        
        for i, record in enumerate(collected_data):
            if not isinstance(record, dict):
                validation_errors.append(f"Record {i}: Expected dict but got type {type(record)}")
                continue
            
            missing_fields = required_fields - set(record.keys())
            if missing_fields:
                validation_errors.append(f"Record {i}: Missing required fields: {missing_fields}")
                
            for field in required_fields:
                if field in record and not record[field]:
                    validation_errors.append(f"Record {i}: Field '{field}' is empty")
                    
            if check_content_quality and 'content' in record:
                content = record['content']
                if isinstance(content, str):
                    min_content_length = self.config['data_validator']['min_content_length']
                    if len(content.strip()) < min_content_length:
                        validation_errors.append(f"Record {i}: Content too short <{min_content_length} characters.")
                       
                    utf8_corruption_patterns = self.config['data_validator']['utf8_corruption_patterns']
                    if any(char in content for char in utf8_corruption_patterns):
                        validation_errors.append(f"Record {i}: Potential encoding issues in content")
                        
            if 'language' in record:
                language = record['language']
                if not isinstance(language, str) or len(language) != 2:
                    validation_errors.append(f"Record {i}: Invalid language code: {language}")
                    
            if 'url' in record:
                url = record['url']
                if not isinstance(url, str) or not url.startswith(('http://', 'https://')):
                    validation_errors.append(f"Record {i}: Invalid URL format: {url}")
                    
        if validation_errors:
            limit = self.config['data_validator']['error_logging_limit']
            self.logger.warning(f"Data validation found {len(validation_errors)} issues:")
            for error in validation_errors[:limit]:
                self.logger.warning(f"  - {error}")
            if len(validation_errors) > limit:
                self.logger.warning(f"  ... and {len(validation_errors) - limit} errors remaining")
            return False

        self.logger.info(f"Data validation passed: {len(collected_data)} records validated successfully")
        return True