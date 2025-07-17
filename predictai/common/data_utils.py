import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import pandas as pd
from pathlib import Path

from predictai.common.config_loader import ConfigLoader
from predictai.common.logging_config import get_logger

class DataUtils:
    def __init__(self, config):
        try:
            if not config:
                raise ValueError(f"Config cannot be none or empty")
            
            self.config = config
            self.logger = get_logger(__name__)
            
        except ImportError as import_error:
            raise ImportError(f"Error importing modules: {import_error}")
        
        except Exception as general_error:
            self.logger.error(f"General Error: {general_error}")
        
    def save_collected_data(
        self,
        data: List[Dict[str, Any]],
        data_type: str,
        file_format: str = None,
        output_directory: str = None,
        filename: str = None,
        validate_before_save: bool = None
        ) -> Optional[str]:
        """ 
        Saves collected data to file in specified format
        
        Returns:
            File path if successful, None if failure occurs or no data
        """
        if not data:
            self.logger.warning("No data provided to save.")
            return None
        
        if file_format is None:
            file_format = self.config['data_output']['file_format']
        if output_directory is None:
            output_directory = self.config['data_output']['directory']
        if filename is None:
            current_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{data_type}_{current_timestamp}.{file_format}"
        if validate_before_save is None:
            validate_before_save = self.config['data_validator']['validate_before_save']
            
        if validate_before_save:
            if not self.validate_data_structure_and_quality(data):
                self.logger.error("Data validation failed, cannot save invalid data")
                return None
            
        Path(output_directory).mkdir(parents=True, exist_ok=True)
        
        df = pd.DataFrame(data)
        
        filepath = os.path.join(output_directory, filename)
        
        try:
            file_format_lowered = file_format.lower()
            if file_format_lowered == 'csv':
                df.to_csv(filepath, index=False)
            elif file_format_lowered == 'json':
                df.to_json(filepath, orient='records', indent=2)
            elif file_format_lowered == 'xlsx' or file_format_lowered == 'excel':
                df.to_excel(filepath, index=False)
            else:
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
        data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """ 
        Generates data summary statistics for collected data
        
        Returns:
            Dictionary containing summary statistics
        """
        if not data:
            return {"total_records": 0}
        
        df = pd.DataFrame(data)
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
        data: List[Dict[str, Any]]
        ) -> None:
        """
        Logs summary statistcs and gets them from helper function
        """
        summary = self.get_data_summary(data)
        
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
            
        validation_errors = []
        
        for i, record in enumerate(collected_data):
            if not isinstance(record, dict):
                validation_errors.append(f"Record {i}: Expected dict but got type {type(record)}")
                continue
            
            missing_fields = required_fields - set(record.keys())
            if missing_fields:
                validation_errors.append(f"Record {i}: Missing required fields: {missing_fields}'")
                
            for field in required_fields:
                if field in record and not record[field]:
                    validation_errors.append(f"Record {i}: Field '{field} is empty")
                    
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
            limit = self.config['data_validator']['validation_error_logging_limit']
            self.logger.warning(f"Data validation found {len(validation_errors)} issues:")
            for error in validation_errors[:limit]:
                self.logger.warning(f"  - {error}")
            if len(validation_errors) > limit:
                self.logger.warning(f"  ... and {len(validation_errors) - limit} errors remaining")
            return False

        self.logger.info(f"Data validation passed: {len(collected_data)} records validated successfully")
        return True