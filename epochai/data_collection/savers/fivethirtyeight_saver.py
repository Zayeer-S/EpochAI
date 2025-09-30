from typing import Any, Dict, Optional, Tuple

from epochai.common.utils.decorators import handle_generic_errors_gracefully, handle_initialization_errors
from epochai.data_collection.savers.base_saver import BaseSaver


class FiveThirtyEightSaver(BaseSaver):
    @handle_initialization_errors(f"{__name__} Initialization")
    def __init__(
        self,
        collector_name: str,
        collector_version: str,
    ):
        super().__init__(collector_name, collector_version)

        self._required_fields = ["cycle", "state", "candidate_name", "pct_estimate"]
        self._min_pct_estimate = 0.0
        self._max_pct_estimate = 100.0
        self._valid_cycles = list(range(1968, 2025))

    @handle_generic_errors_gracefully("while preparing metadata for storage", {})
    def _prepare_metadata_for_storage(
        self,
        collected_item: Dict[str, Any],
        language_code: str,
    ) -> Dict[str, Any]:
        """Prepares collected FiveThirtyEight polling item for storage with proper field mapping"""
        metadata = {}

        field_mappings = {
            "cycle": "cycle",
            "state": "state",
            "modeldate": "modeldate",
            "candidate_name": "candidate_name",
            "candidate_id": "candidate_id",
            "pct_estimate": "pct_estimate",
            "pct_trend_adjusted": "pct_trend_adjusted",
            "timestamp": "timestamp",
            "comment": "comment",
            "election_date": "election_date",
            "election_qdate": "election_qdate",
            "last_qdate": "last_qdate",
            "last_enddate": "last_enddate",
            "language": language_code,
            "collected_at": "collected_at",
            "collection_source": "collection_source",
            "original_row_index": "original_row_index",
        }

        for our_field, source_field in field_mappings.items():
            if isinstance(source_field, str) and source_field in collected_item:
                metadata[our_field] = collected_item[source_field]
            elif our_field in collected_item:
                metadata[our_field] = collected_item[our_field]
            elif our_field == "language":
                metadata[our_field] = language_code

        csv_columns = [
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

        for column in csv_columns:
            if column in collected_item:
                metadata[column] = collected_item[column]

        return metadata

    @handle_generic_errors_gracefully("during FiveThirtyEight validation function operation", (False, None))
    def fivethirtyeight_validation_function(
        self,
        data: Dict[str, Any],
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Custom validation function for collected FiveThirtyEight polling data"""
        validation_errors = []

        for field in self._required_fields:
            if field not in data:
                validation_errors.append(f"Missing required field: {field}")
            elif data[field] is None:
                validation_errors.append(f"Required field is null: {field}")

        if "cycle" in data and data["cycle"] is not None:
            try:
                cycle = int(data["cycle"])
                if cycle not in self._valid_cycles:
                    validation_errors.append(
                        f"Invalid cycle: {cycle}. Expected between {min(self._valid_cycles)} and {max(self._valid_cycles)}",
                    )
            except (ValueError, TypeError):
                validation_errors.append(f"Cycle must be an integer: {data['cycle']}")

        if "state" in data and data["state"] is not None:
            state = str(data["state"]).strip()
            if len(state) == 0:
                validation_errors.append("State cannot be empty")
            elif len(state) > 50:
                validation_errors.append(f"State name too long: {len(state)} characters (max 50)")

        if "candidate_name" in data and data["candidate_name"] is not None:
            candidate = str(data["candidate_name"]).strip()
            if len(candidate) == 0:
                validation_errors.append("Candidate name cannot be empty")
            elif len(candidate) > 100:
                validation_errors.append(f"Candidate name too long: {len(candidate)} characters (max 100)")

        if "pct_estimate" in data and data["pct_estimate"] is not None:
            try:
                pct = float(data["pct_estimate"])
                if pct < self._min_pct_estimate or pct > self._max_pct_estimate:
                    validation_errors.append(
                        f"Invalid percentage estimate: {pct}. Must be between {self._min_pct_estimate} and {self._max_pct_estimate}",  # noqa
                    )
            except (ValueError, TypeError):
                validation_errors.append(f"Percentage estimate must be a number: {data['pct_estimate']}")

        if "pct_trend_adjusted" in data and data["pct_trend_adjusted"] is not None:
            try:
                pct_trend = float(data["pct_trend_adjusted"])
                if pct_trend < self._min_pct_estimate or pct_trend > self._max_pct_estimate:
                    validation_errors.append(
                        f"Invalid trend-adjusted percentage: {pct_trend}. Must be between {self._min_pct_estimate} and {self._max_pct_estimate}",  # noqa
                    )
            except (ValueError, TypeError):
                validation_errors.append(f"Trend-adjusted percentage must be a number: {data['pct_trend_adjusted']}")

        if "candidate_id" in data and data["candidate_id"] is not None:
            try:
                candidate_id = int(data["candidate_id"])
                if candidate_id <= 0:
                    validation_errors.append(f"Candidate ID must be positive: {candidate_id}")
            except (ValueError, TypeError):
                validation_errors.append(f"Candidate ID must be an integer: {data['candidate_id']}")

        date_fields = ["modeldate", "election_date", "timestamp", "collected_at"]
        for date_field in date_fields:
            if date_field in data and data[date_field] is not None:
                date_value = str(data[date_field]).strip()
                if len(date_value) == 0:
                    validation_errors.append(f"{date_field} cannot be empty string")

        if "language" in data and data["language"] != "en":
            validation_errors.append(f"Invalid language code: {data['language']}. Expected 'en' for FiveThirtyEight data")

        # Check for data consistency
        if (
            "pct_estimate" in data
            and data["pct_estimate"] is not None
            and "pct_trend_adjusted" in data
            and data["pct_trend_adjusted"] is not None
        ):
            try:
                pct_est = float(data["pct_estimate"])
                pct_trend = float(data["pct_trend_adjusted"])
                diff = abs(pct_est - pct_trend)
                if diff > 50:  # sanity check
                    validation_errors.append(
                        f"Large difference between estimate ({pct_est}) and trend-adjusted ({pct_trend}): {diff}%",
                    )
            except (ValueError, TypeError):
                pass  # Already caught

        is_valid = len(validation_errors) == 0
        error_dict = {"validation_errors": validation_errors} if validation_errors else None

        return is_valid, error_dict
