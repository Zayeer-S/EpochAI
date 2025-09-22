# ruff: noqa: RET505

from datetime import datetime
import re
from typing import Any, Dict, List, Optional

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.database.models import RawData
from epochai.common.enums import CollectionTypeNames
from epochai.common.utils.decorators import handle_generic_errors_gracefully, handle_initialization_errors
from epochai.data_processing.cleaners.base_cleaner import BaseCleaner


class FiveThirtyEightCleaner(BaseCleaner):
    @handle_initialization_errors(f"{__name__} Initialization")
    def __init__(self):
        self.config = ConfigLoader.get_data_config()

        super().__init__(
            cleaner_name=self.config.get("cleaners").get("fivethirtyeight").get("cleaner_name"),
            cleaner_version=self.config.get("cleaners").get("fivethirtyeight").get("current_schema_version"),
        )

        self._date_pattern = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")

        self._state_mapping = {
            "North Carolina": "North_Carolina",
            "South Carolina": "South_Carolina",
            "New Hampshire": "New_Hampshire",
            "New Jersey": "New_Jersey",
            "New York": "New_York",
            "West Virginia": "West_Virginia",
            "Rhode Island": "Rhode_Island",
        }

        self.logger.info(f"{__name__} Initialized (Schema ID: {self._schema_utils.get_metadata_schema_id()})")

    @handle_generic_errors_gracefully("while transforming FiveThirtyEight content", {})
    def transform_content(self, raw_data: RawData) -> Dict[str, Any]:
        """
        Cleans FiveThirtyEight polling data

        Returns:
            Dict containing cleaned metadata
        """
        if not raw_data.metadata:
            raise ValueError(f"Raw data ({raw_data.id}) has no metadata to clean")

        raw_metadata = raw_data.metadata
        cleaned_metadata: Dict[str, Any] = {}

        cleaned_metadata["cycle"] = self._clean_numeric_field(raw_metadata.get("cycle"))
        cleaned_metadata["cleaned_state"] = self._normalize_state_name(raw_metadata.get("state"))
        cleaned_metadata["cleaned_candidate_name"] = self._normalize_candidate_name(raw_metadata.get("candidate_name"))

        cleaned_metadata["pct_estimate"] = self._validate_percentage(raw_metadata.get("pct_estimate"))
        cleaned_metadata["pct_trend_adjusted"] = self._validate_percentage(raw_metadata.get("pct_trend_adjusted"))

        modeldate = self._parse_date(raw_metadata.get("modeldate"))
        election_date = self._parse_date(raw_metadata.get("election_date"))

        cleaned_metadata["modeldate"] = modeldate
        cleaned_metadata["election_date"] = election_date

        if modeldate and election_date:
            cleaned_metadata["days_before_election"] = self._calculate_days_difference(modeldate, election_date)
        else:
            cleaned_metadata["days_before_election"] = None

        cycle = cleaned_metadata["cycle"]
        if cycle and cycle <= 2016:
            cleaned_metadata["dataset_type"] = CollectionTypeNames.PRE_2016.value
        else:
            cleaned_metadata["dataset_type"] = CollectionTypeNames.POST_2016.value

        cleaned_metadata["collection_source"] = raw_metadata.get("collection_source")

        cleaned_metadata["data_quality_score"] = self._calculate_data_quality_score(cleaned_metadata)
        cleaned_metadata["is_outlier"] = self._detect_outliers(cleaned_metadata)

        if "sum_influence" in raw_metadata:
            cleaned_metadata["sum_influence"] = self._clean_float_field(raw_metadata.get("sum_influence"))

        cleaned_metadata["cleaned_at"] = datetime.now().isoformat()
        cleaned_metadata["cleaning_operations_applied"] = self._get_cleaning_operations_list()

        return cleaned_metadata

    def _normalize_state_name(self, state: str) -> str:
        """Normalize state names for consistency"""
        if not state or not isinstance(state, str):
            return ""

        cleaned_state = state.strip()
        return self._state_mapping.get(cleaned_state, cleaned_state.replace(" ", "_"))

    def _normalize_candidate_name(self, candidate: str) -> str:
        """Clean and normalize candidate names"""
        if not candidate or not isinstance(candidate, str):
            return ""

        cleaned = re.sub(r"\s+", " ", candidate.strip())

        name_mappings = {
            "Hillary Rodham Clinton": "Hillary Clinton",
            "Donald J. Trump": "Donald Trump",
            "Bernard Sanders": "Bernie Sanders",
        }

        return name_mappings.get(cleaned, cleaned)

    def _parse_date(self, date_str: Any) -> Optional[str]:
        """Parse date strings into ISO format

        Why must the Americans think that months come before dates??"""

        if not date_str:
            return None

        date_str = str(date_str).strip()

        # Handle ISO datetime format
        if "T" in date_str:
            date_str = date_str.split("T")[0]  # Take just the date part

        # Try ISO format (YYYY-MM-DD)
        iso_match = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", date_str)
        if iso_match:
            return date_str

        # Try MM/DD/YYYY format
        mm_dd_yyyy_match = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", date_str)
        if mm_dd_yyyy_match:
            month, day, year = mm_dd_yyyy_match.groups()
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        return None

    def _calculate_days_difference(self, start_date: str, end_date: str) -> Optional[int]:
        """Calculate days between two ISO dates"""
        try:
            from datetime import datetime

            start = datetime.fromisoformat(start_date)
            end = datetime.fromisoformat(end_date)
            return (end - start).days
        except (ValueError, TypeError):
            return None

    def _validate_percentage(self, pct: Any) -> Optional[float]:
        """Validate and clean percentage values"""
        if pct is None:
            return None

        try:
            pct_float = float(pct)
            # Clamp to valid percentage range
            if pct_float < 0:
                self.logger.warning(f"Negative percentage found: {pct_float}, setting to 0")
                return 0.0
            elif pct_float > 100:
                self.logger.warning(f"Percentage > 100 found: {pct_float}, setting to 100")
                return 100.0
            return round(pct_float, 6)  # Round to 6 decimal places for consistency
        except (ValueError, TypeError):
            self.logger.warning(f"Invalid percentage value: {pct}")
            return None

    def _clean_numeric_field(self, value: Any) -> Optional[int]:
        """Clean numeric fields"""
        if value is None:
            return None

        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _clean_float_field(self, value: Any) -> Optional[float]:
        """Clean float fields"""
        if value is None:
            return None

        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _calculate_data_quality_score(self, metadata: Dict[str, Any]) -> float:
        """Calculate a data quality score from 0-1 based on ML-relevant fields"""
        required_fields = ["cycle", "cleaned_state", "cleaned_candidate_name"]
        important_fields = ["modeldate", "election_date", "days_before_election"]
        polling_fields = ["pct_estimate", "pct_trend_adjusted"]

        score = 0.0
        max_score = 0.0

        for field in required_fields:
            max_score += 1.0
            if metadata.get(field):
                score += 1.0

        for field in important_fields:
            max_score += 0.7
            if metadata.get(field) is not None:
                score += 0.7

        max_score += 0.8
        if any(metadata.get(field) is not None for field in polling_fields):
            score += 0.8

        return min(score / max_score, 1.0) if max_score > 0 else 0.0

    def _detect_outliers(self, metadata: Dict[str, Any]) -> bool:
        """Detect potential outlier data points for ML filtering"""
        for pct_field in ["pct_estimate", "pct_trend_adjusted"]:
            pct = metadata.get(pct_field)
            if pct is not None and (pct < 1.0 or pct > 90.0):
                return True

        cycle = metadata.get("cycle")
        if cycle is not None:
            current_year = datetime.now().year
            if cycle < 1968 or cycle > current_year + 4:
                return True

        days_before = metadata.get("days_before_election")
        if days_before is not None and (days_before < 0 or days_before > 1460):  # More than 4 years
            return True

        return False

    def _get_cleaning_operations_list(self) -> List[str]:
        """Return list of cleaning operations applied"""
        return [
            "state_normalization",
            "candidate_name_normalization",
            "date_parsing",
            "percentage_validation",
            "data_type_conversion",
            "outlier_detection",
            "data_quality_scoring",
        ]
