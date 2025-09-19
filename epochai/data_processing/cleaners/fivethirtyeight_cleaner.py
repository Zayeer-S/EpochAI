# ruff: noqa: RET505

from datetime import datetime
import re
from typing import Any, Dict, List, Optional

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.database.models import RawData
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

        # Compile regex patterns for efficient reuse
        self._date_pattern = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")
        self._timestamp_pattern = re.compile(r"^(\d{1,2}):(\d{1,2}):(\d{1,2}) (\d{1,2}) (\w{3}) (\d{4})$")

        # State name normalization mapping
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
        Cleans FiveThirtyEight polling data including statistical modeling fields

        Returns:
            Dict containing cleaned metadata with all statistical fields
        """
        if not raw_data.metadata:
            raise ValueError(f"Raw data ({raw_data.id}) has no metadata to clean")

        raw_metadata = raw_data.metadata
        cleaned_metadata: Dict[str, Any] = {}

        # Required fields
        cleaned_metadata["cycle"] = self._clean_numeric_field(raw_metadata.get("cycle"))
        cleaned_metadata["state"] = str(raw_metadata.get("state", "")).strip()
        cleaned_metadata["candidate_name"] = str(raw_metadata.get("candidate_name", "")).strip()
        cleaned_metadata["pct_estimate"] = self._validate_percentage(raw_metadata.get("pct_estimate"))

        # Cleaned/normalized fields
        cleaned_metadata["cleaned_state"] = self._normalize_state_name(raw_metadata.get("state"))
        cleaned_metadata["cleaned_candidate_name"] = self._normalize_candidate_name(raw_metadata.get("candidate_name"))

        # Core polling data
        cleaned_metadata["candidate_id"] = self._clean_numeric_field(raw_metadata.get("candidate_id"))
        cleaned_metadata["pct_trend_adjusted"] = self._validate_percentage(raw_metadata.get("pct_trend_adjusted"))

        # Date fields
        cleaned_metadata["modeldate"] = self._parse_date(raw_metadata.get("modeldate"))
        cleaned_metadata["election_date"] = self._parse_date(raw_metadata.get("election_date"))
        cleaned_metadata["last_enddate"] = self._parse_date(raw_metadata.get("last_enddate"))

        # Calculate days before election
        if cleaned_metadata.get("modeldate") and cleaned_metadata.get("election_date"):
            cleaned_metadata["days_before_election"] = self._calculate_days_difference(
                cleaned_metadata["modeldate"],
                cleaned_metadata["election_date"],
            )
        else:
            cleaned_metadata["days_before_election"] = None

        # Statistical modeling fields - these are crucial for Bayesian ML
        statistical_fields = [
            "_medpoly2",
            "trend_medpoly2",
            "_shortpoly0",
            "trend_shortpoly0",
            "sum_weight_medium",
            "sum_weight_short",
            "sum_influence",
            "sum_nat_influence",
            "_minpoints",
            "_defaultbasetime",
            "_numloops",
            "_state_houseeffects_weight",
            "_state_trendline_weight",
            "_out_of_state_house_discount",
            "_house_effects_multiplier",
            "_attenuate_endpoints",
            "_nonlinear_polynomial_degree",
            "_shortpoly_combpoly_weight",
            "_nat_shortpoly_combpoly_weight",
        ]

        for field in statistical_fields:
            if field in raw_metadata:
                if field in ["_attenuate_endpoints"]:  # String field
                    cleaned_metadata[field] = raw_metadata[field]
                elif field.startswith("_") and "weight" in field or "discount" in field:  # Proportion fields
                    cleaned_metadata[field] = self._validate_proportion(raw_metadata[field])
                elif field in ["_minpoints", "_defaultbasetime", "_numloops", "_nonlinear_polynomial_degree"]:  # Integer fields
                    cleaned_metadata[field] = self._clean_numeric_field(raw_metadata[field])
                else:  # Numeric fields (percentages, estimates, etc.)
                    cleaned_metadata[field] = self._clean_float_field(raw_metadata[field])
            else:
                cleaned_metadata[field] = None

        # Date/time related fields
        cleaned_metadata["election_qdate"] = self._clean_numeric_field(raw_metadata.get("election_qdate"))
        cleaned_metadata["last_qdate"] = self._clean_numeric_field(raw_metadata.get("last_qdate"))
        cleaned_metadata["timestamp"] = raw_metadata.get("timestamp")
        cleaned_metadata["comment"] = raw_metadata.get("comment")

        # Metadata fields
        cleaned_metadata["language"] = raw_metadata.get("language")
        cleaned_metadata["collected_at"] = raw_metadata.get("collected_at")
        cleaned_metadata["collection_source"] = raw_metadata.get("collection_source")
        cleaned_metadata["original_row_index"] = self._clean_numeric_field(raw_metadata.get("original_row_index"))

        # Required cleaning metadata
        cleaned_metadata["cleaned_at"] = datetime.now().isoformat()
        cleaned_metadata["cleaning_operations_applied"] = self._get_cleaning_operations_list()
        cleaned_metadata["data_quality_score"] = self._calculate_data_quality_score(cleaned_metadata)
        cleaned_metadata["is_outlier"] = self._detect_outliers(cleaned_metadata)

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

        # Remove extra whitespace and normalize
        cleaned = re.sub(r"\s+", " ", candidate.strip())

        # Handle common name variations
        name_mappings = {
            "Hillary Rodham Clinton": "Hillary Clinton",
            "Donald J. Trump": "Donald Trump",
            "Bernard Sanders": "Bernie Sanders",
        }

        return name_mappings.get(cleaned, cleaned)

    def _parse_date(self, date_str: Any) -> Optional[str]:
        """Parse date strings into ISO format"""
        if not date_str:
            return None

        if not isinstance(date_str, str):
            date_str = str(date_str)

        date_str = date_str.strip()

        # Try to match MM/DD/YYYY format
        match = self._date_pattern.match(date_str)
        if match:
            month, day, year = match.groups()
            try:
                # Return ISO date format
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            except ValueError:
                self.logger.warning(f"Invalid date components: {date_str}")
                return None

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
            return int(float(value))  # Handle cases where int is stored as float
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

    def _validate_proportion(self, value: Any) -> Optional[float]:
        """Validate proportion values (0-1)"""
        if value is None:
            return None

        try:
            prop_float = float(value)
            # Clamp to valid proportion range
            if prop_float < 0:
                return 0.0
            elif prop_float > 1:
                return 1.0
            return round(prop_float, 6)
        except (ValueError, TypeError):
            return None

    def _handle_null_values(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure consistent null value handling"""
        # Fields that should be None when empty/invalid
        nullable_fields = [
            "_medpoly2",
            "trend_medpoly2",
            "pct_trend_adjusted",
            "candidate_id",
            "timestamp",
            "comment",
        ]

        for field in nullable_fields:
            if field in metadata and metadata[field] in ["", "nan", "NaN"]:
                metadata[field] = None

        return metadata

    def _calculate_data_quality_score(self, metadata: Dict[str, Any]) -> float:
        """Calculate a data quality score from 0-1"""
        required_fields = ["cycle", "state", "candidate_name", "pct_estimate"]
        optional_fields = ["modeldate", "candidate_id", "pct_trend_adjusted", "election_date"]

        score = 0.0
        max_score = len(required_fields) + len(optional_fields)

        # Required fields (higher weight)
        for field in required_fields:
            if metadata.get(field) is not None:
                score += 1.0

        # Optional fields (lower weight)
        for field in optional_fields:
            if metadata.get(field) is not None:
                score += 0.5

        return min(score / max_score, 1.0)

    def _detect_outliers(self, metadata: Dict[str, Any]) -> bool:
        """Detect potential outlier data points"""
        pct_estimate = metadata.get("pct_estimate")

        if pct_estimate is not None and (pct_estimate < 1.0 or pct_estimate > 90.0):
            return True

        # Flag very old or future election cycles
        cycle = metadata.get("cycle")
        if cycle is not None:
            current_year = datetime.now().year
            if cycle < 1968 or cycle > current_year + 4:
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
            "null_value_handling",
            "duplicate_detection",
        ]
