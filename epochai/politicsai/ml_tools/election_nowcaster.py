from typing import Any, Dict, List, Optional
import warnings

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import xgboost as xgb

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.database.dao.cleaned_data_dao import CleanedDataDAO
from epochai.common.logging_config import get_logger
from epochai.common.utils.decorators import handle_generic_errors_gracefully, handle_initialization_errors

warnings.filterwarnings("ignore")


class ElectionNowcaster:
    @handle_initialization_errors(f"{__name__} initialization")
    def __init__(self, election_year: int = 2016):
        self._logger = get_logger(__name__)
        self._cleaned_data_dao = CleanedDataDAO()
        self._election_year = election_year

        config = ConfigLoader.get_data_config()
        config = config["cleaners"]["fivethirtyeight"]
        self._cleaner_name = config["cleaner_name"]
        self._current_schema_version = config["current_schema_version"]

        self._decay_rate = 0.05

        # All electoral votes except D.C. TODO Have multiple years
        self._electoral_votes = {
            "Alabama": 9,
            "Alaska": 3,
            "Arizona": 11,
            "Arkansas": 6,
            "California": 55,
            "Colorado": 9,
            "Connecticut": 7,
            "Delaware": 3,
            "Florida": 29,
            "Georgia": 16,
            "Hawaii": 4,
            "Idaho": 4,
            "Illinois": 20,
            "Indiana": 11,
            "Iowa": 6,
            "Kansas": 6,
            "Kentucky": 8,
            "Louisiana": 8,
            "Maine": 4,
            "Maryland": 10,
            "Massachusetts": 11,
            "Michigan": 16,
            "Minnesota": 10,
            "Mississippi": 6,
            "Missouri": 10,
            "Montana": 3,
            "Nebraska": 5,
            "Nevada": 6,
            "New_Hampshire": 4,
            "New_Jersey": 14,
            "New_Mexico": 5,
            "New_York": 29,
            "North_Carolina": 15,
            "North_Dakota": 3,
            "Ohio": 18,
            "Oklahoma": 7,
            "Oregon": 7,
            "Pennsylvania": 20,
            "Rhode_Island": 4,
            "South_Carolina": 9,
            "South_Dakota": 3,
            "Tennessee": 11,
            "Texas": 38,
            "Utah": 6,
            "Vermont": 3,
            "Virginia": 13,
            "Washington": 12,
            "West_Virginia": 5,
            "Wisconsin": 10,
            "Wyoming": 3,
            "National": 0,
        }

        self._xgb_models: Dict[Any, Any] = {}  # Stores one model per candidate
        self._state_encoder = LabelEncoder()
        self._polling_data: Any = None
        self._candidates: Any = None
        self._current_date: Any = None
        self._target_column: str = ""

        self._logger.info(f"Initialized {__name__} for {election_year}")

    def _get_date_range(self) -> Dict[str, int]:
        """Gets the date range of polling data"""
        if self._polling_data is None or self._polling_data.empty:
            return {}

        max_days = self._polling_data["days_since_poll"].max()
        min_days = self._polling_data["days_since_poll"].min()

        return {
            "days_of_data": int(max_days - min_days) if max_days and min_days else 0,
            "most_recent_poll_days_ago": int(min_days) if min_days else 0,
            "oldest_poll_days_ago": int(max_days) if max_days else 0,
        }

    @handle_generic_errors_gracefully("while loading polling data", pd.DataFrame())
    def load_polling_data(
        self,
        candidates: List[str],
        current_date: str,
        states: Optional[List[str]] = None,
        lookback_days: int = 60,
    ) -> pd.DataFrame:
        """Loads and preprocesses polling data from cleaned_data table

        Args:
            candidates: List of candidate names to filter for
            current_date: Date for nowcast (ISO format YYYY-MM-DD)
            states: Optional list of states to filter for
            lookback_days: How many days of polling data to include before current_date
        """

        if not candidates:
            raise ValueError("No candidates entered")

        self._logger.info(f"Loading polling data for {len(candidates)} candidates for {self._election_year} race")

        cleaned_records = self._cleaned_data_dao.get_by_cleaner(self._cleaner_name, self._current_schema_version)

        if not cleaned_records:
            self._logger.error("No cleaned FiveThirtyEight data found")
            return pd.DataFrame()

        data_dicts = []
        for record in cleaned_records:
            if record.metadata:
                metadata = record.metadata.copy()
                metadata["record_id"] = record.id
                data_dicts.append(metadata)

        df = pd.DataFrame(data_dicts)
        if df.empty:
            return df

        df = df[df["cycle"] == self._election_year]
        df = df[df["cleaned_candidate_name"].isin(candidates)]

        if self._election_year <= 2016:
            self._target_column = "pct_estimate"
        else:
            self._target_column = "pct_trend_adjusted"

        self._logger.info(f"Using {self._target_column} for {self._election_year} election")

        df["model_date"] = pd.to_datetime(df["model_date"], errors="coerce")
        df = df.dropna(subset=["model_date"])

        current_date_parsed = pd.to_datetime(current_date)
        self._current_date = current_date_parsed

        df["days_since_poll"] = (current_date_parsed - df["model_date"]).dt.days

        df = df[df["days_since_poll"] >= 0]
        df = df[df["days_since_poll"] <= lookback_days]

        if len(df) > 1000:
            df = df.sample(n=1000, random_state=42).reset_index(drop=True)
            self._logger.info(f"Sampled down to {len(df)} records for performance")

        if states:
            df = df[df["cleaned_state"].isin(states)]

        required_cols = [
            "cleaned_state",
            "cleaned_candidate_name",
            self._target_column,
        ]
        df = df.dropna(subset=required_cols)

        df["time_weight"] = np.exp(-self._decay_rate * df["days_since_poll"] / 30)

        if "data_quality_score" in df.columns:
            df["data_quality_score"] = df["data_quality_score"].fillna(0.5)
        else:
            df["data_quality_score"] = 0.5

        if "sum_influence" in df.columns:
            df["sum_influence"] = df["sum_influence"].fillna(1.0)
        else:
            df["sum_influence"] = 1.0

        base_weight = df["data_quality_score"]
        fte_weight = df["sum_influence"]
        df["poll_weight"] = base_weight * fte_weight * df["time_weight"]

        self._polling_data = df
        self._candidates = list(df["cleaned_candidate_name"].unique())
        self._logger.info(f"Loaded {len(df)} polling records from {lookback_days} days before {current_date}")

        return df

    @handle_generic_errors_gracefully("while creating ML features", pd.DataFrame())
    def create_ml_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create features for XGBoost model"""

        if df.empty:
            return df

        # Sort by state and days_since_poll for rolling calculations
        df = df.sort_values(["cleaned_state", "cleaned_candidate_name", "days_since_poll"])

        feature_df = df.copy()

        # Time-based features
        feature_df["days_since_poll_norm"] = feature_df["days_since_poll"] / 365
        feature_df["weeks_since_poll"] = feature_df["days_since_poll"] / 7

        # State encoding
        feature_df["state_encoded"] = self._state_encoder.fit_transform(feature_df["cleaned_state"])

        # Rolling averages by candidate and state using relevant target column
        for window in [7, 14, 30]:
            feature_df[f"poll_avg_{window}d"] = (
                feature_df.groupby(["cleaned_state", "cleaned_candidate_name"])[self._target_column]
                .rolling(window=window, min_periods=1)
                .mean()
                .reset_index(level=[0, 1], drop=True)
            )

        # Poll quality features
        max_quality = feature_df["data_quality_score"].max()
        feature_df["quality_score_norm"] = feature_df["data_quality_score"] / max_quality if max_quality > 0 else 0.5

        max_influence = feature_df["sum_influence"].max()
        feature_df["influence_norm"] = feature_df["sum_influence"] / max_influence if max_influence > 0 else 1.0

        # Historical state lean TODO Make dynamic
        safe_dem_states = ["California", "New_York", "Illinois", "Massachusetts", "Washington"]
        safe_rep_states = ["Texas", "Alabama", "Wyoming", "Oklahoma", "Utah"]
        swing_states = ["Florida", "Pennsylvania", "Michigan", "Wisconsin", "North_Carolina"]

        feature_df["state_lean"] = 0  # neutral
        feature_df.loc[feature_df["cleaned_state"].isin(safe_dem_states), "state_lean"] = -1
        feature_df.loc[feature_df["cleaned_state"].isin(safe_rep_states), "state_lean"] = 1

        feature_df["is_swing_state"] = feature_df["cleaned_state"].isin(swing_states).astype(int)

        return feature_df

    @handle_generic_errors_gracefully("while training XGBoost models", {})
    def train_xgb_models(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Train XGBoost models for each candidate"""

        feature_df = self.create_ml_features(df)
        candidates = feature_df["cleaned_candidate_name"].unique()

        feature_cols = [
            "days_since_poll_norm",
            "weeks_since_poll",
            "state_encoded",
            "poll_avg_7d",
            "poll_avg_14d",
            "poll_avg_30d",
            "quality_score_norm",
            "influence_norm",
            "time_weight",
            "state_lean",
            "is_swing_state",
        ]

        self._xgb_models = {}
        model_scores = {}

        # Filter data for this candidate, determine data sufficiency
        # Train/test split, evaluation
        for candidate in candidates:
            self._logger.info(f"Training XGBoost model for {candidate}")

            candidate_data = feature_df[feature_df["cleaned_candidate_name"] == candidate].copy()

            if len(candidate_data) < 10:
                self._logger.warning(f"Insufficient data for {candidate}: {len(candidate_data)} rows")
                continue

            X = candidate_data[feature_cols]
            y = candidate_data[self._target_column]

            if len(X) > 20:
                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            else:
                X_train, X_test, y_train, y_test = X, X, y, y

            trained_model = xgb.XGBRegressor(
                n_estimators=100,
                max_depth=4,
                learning_rate=0.1,
                random_state=42,
                n_jobs=1,
            )

            trained_model.fit(X_train, y_train)

            train_score = trained_model.score(X_train, y_train)
            test_score = trained_model.score(X_test, y_test) if len(X_test) > 0 else train_score

            self._xgb_models[candidate] = trained_model
            model_scores[candidate] = {
                "train_r2": train_score,
                "test_r2": test_score,
                "n_samples": len(candidate_data),
            }

            self._logger.info(f"{candidate} model: R² = {test_score:.3f}")

        return model_scores

    @handle_generic_errors_gracefully("while making XGBoost predictions", {})
    def predict_with_xgb(
        self,
        states: Optional[List[str]] = None,
        shy_voter_adjustment: float = 0.0,
        shy_candidate: str = "",
    ) -> Dict[str, Dict[str, float]]:
        """Generate state predictions using XGBoost models with optional shy voter effect correction"""

        if not self._xgb_models:
            raise ValueError("XGBoost models not trained. Call train_xgb_models first.")

        if not states:
            states = list(self._electoral_votes.keys())
            states = [s for s in states if s != "National"]

        # Create prediction features for each state
        predictions: Dict[str, Dict[str, float]] = {}
        for state in states:
            predictions[state] = {}

            state_data = self._polling_data[self._polling_data["cleaned_state"] == state]

            raw_predictions = {}

            for candidate, model in self._xgb_models.items():
                # Create feature vector for prediction
                if not state_data.empty:
                    recent_data = state_data[state_data["cleaned_candidate_name"] == candidate]
                    if not recent_data.empty:
                        recent_avg = recent_data[self._target_column].mean()
                        days_recent = recent_data["days_since_poll"].min()
                    else:
                        recent_avg = 45.0  # Default
                        days_recent = 30
                else:
                    recent_avg = 45.0  # Default
                    days_recent = 30

                # Create feature vector
                features = pd.DataFrame(
                    {
                        "days_since_poll_norm": [days_recent / 365],
                        "weeks_since_poll": [days_recent / 7],
                        "state_encoded": [
                            self._state_encoder.transform([state])[0] if state in self._state_encoder.classes_ else 0,
                        ],
                        "poll_avg_7d": [recent_avg],
                        "poll_avg_14d": [recent_avg],
                        "poll_avg_30d": [recent_avg],
                        "quality_score_norm": [0.5],
                        "influence_norm": [0.5],
                        "time_weight": [1.0],
                        "state_lean": [0],
                        "is_swing_state": [
                            1 if state in ["Florida", "Pennsylvania", "Michigan", "Wisconsin", "North_Carolina"] else 0,
                        ],
                    },
                )

                # Make prediction
                pred = model.predict(features)[0]
                raw_predictions[candidate] = max(0, min(100, pred))

            # Apply shy voter adjustment if specified
            adjusted_predictions = raw_predictions.copy()
            if shy_candidate and shy_candidate in adjusted_predictions and shy_voter_adjustment != 0:
                original_value = adjusted_predictions[shy_candidate]
                adjusted_predictions[shy_candidate] += shy_voter_adjustment

                # Reduce other candidates proportionally
                other_candidates = [key for key in adjusted_predictions if key != shy_candidate]
                if other_candidates:
                    reduction_factor = shy_voter_adjustment / len(other_candidates)
                    for other_candidate in other_candidates:
                        adjusted_predictions[other_candidate] = max(0, adjusted_predictions[other_candidate] - reduction_factor)

                # Ensure no candidate exceeds 100%
                for candidate in adjusted_predictions:
                    adjusted_predictions[candidate] = min(100, adjusted_predictions[candidate])

                self._logger.debug(
                    f"{state}: {shy_candidate} adjusted {original_value:.1f}% → {adjusted_predictions[shy_candidate]:.1f}%",
                )

            predictions[state] = adjusted_predictions

        return predictions

    @handle_generic_errors_gracefully("while making election predictions", {})
    def predict_election(
        self,
        state_predictions: Dict[str, Dict[str, float]],
        n_simulations: int = 10000,
        uncertainty_std: float = 2.0,
    ) -> Dict[str, Any]:
        """Generate election predictions using Monte Carlo simulation"""

        if not state_predictions:
            raise ValueError("No state predictions provided")

        self._logger.info(f"Running {n_simulations} Electoral College Simulations")

        states = list(state_predictions.keys())
        candidates = list(next(iter(state_predictions.values())).keys())

        ec_results: List[np.ndarray] = []
        state_win_probs = np.zeros((len(states), len(candidates)))

        for _i in range(n_simulations):
            ec_votes = np.zeros(len(candidates))

            # Add uncertainty to predictions and add normal noise to uncertainty
            # Determine winner and add electoral votes
            for state_idx, state in enumerate(states):
                state_results = []
                for candidate in candidates:
                    base_pred = state_predictions[state][candidate]
                    noisy_pred = np.random.normal(base_pred, uncertainty_std)
                    state_results.append(noisy_pred)

                winner_idx = np.argmax(state_results)
                state_win_probs[state_idx, winner_idx] += 1

                if state in self._electoral_votes:
                    ec_votes[winner_idx] += self._electoral_votes[state]

            ec_results.append(ec_votes.copy())

        ec_results_array = np.array(ec_results)
        state_win_probs /= n_simulations

        # Calculate win probabilities
        election_wins = (ec_results_array >= 270).sum(axis=0)
        win_probabilities = election_wins / n_simulations

        # Normalize win probabilities to sum to 100% (no third party can win) TODO MAKE THIS COUNTRY DETERMINT
        total_win_prob = win_probabilities.sum()
        if total_win_prob > 0:
            win_probabilities = win_probabilities / total_win_prob

        expected_ec_votes = ec_results_array.mean(axis=0)

        # Add D.C.'s 3 electoral votes to Democratic candidate
        democratic_candidate_idx = None
        for i, candidate in enumerate(candidates):
            if any(dem_name in candidate for dem_name in ["Clinton", "Biden", "Harris", "Obama"]):
                democratic_candidate_idx = i
                break

        if democratic_candidate_idx is not None:
            expected_ec_votes[democratic_candidate_idx] += 3

        popular_vote_estimates = []
        for candidate in candidates:
            total_support = sum(state_predictions[state][candidate] for state in states)
            avg_support = total_support / len(states)
            popular_vote_estimates.append(avg_support)

        results = {
            "candidates": candidates,
            "win_probabilities": win_probabilities.tolist(),
            "expected_electoral_votes": expected_ec_votes.tolist(),
            "expected_popular_vote": popular_vote_estimates,
            "state_predictions": state_predictions,
            "state_win_probabilities": {state: state_win_probs[i].tolist() for i, state in enumerate(states)},
            "simulation_details": {
                "n_simulations": n_simulations,
                "electoral_votes_needed": 270,
                "total_electoral_votes": 538,
                "uncertainty_std": uncertainty_std,
            },
        }

        self._logger.info("Election prediction completed successfully")
        return results

    @handle_generic_errors_gracefully("while generating model summary", {})
    def get_model_summary(self) -> Dict[str, Any]:
        """Get model diagnostics and summary statistics"""

        summary = {
            "xgb_models": {
                candidate: {
                    "trained": candidate in self._xgb_models,
                    "feature_importance": (
                        dict(
                            zip(
                                [
                                    "days_since_poll_norm",
                                    "weeks_since_poll",
                                    "state",
                                    "avg_7d",
                                    "avg_14d",
                                    "avg_30d",
                                    "quality",
                                    "influence",
                                    "time_weight",
                                    "lean",
                                    "swing",
                                ],
                                self._xgb_models[candidate].feature_importances_,
                            ),
                        )
                        if candidate in self._xgb_models
                        else {}
                    ),
                }
                for candidate in (self._candidates if self._candidates else [])
            },
            "data_summary": {
                "n_polls": len(self._polling_data) if self._polling_data is not None else 0,
                "n_candidates": len(self._candidates) if self._candidates else 0,
                "date_range": self._get_date_range() if self._polling_data is not None else None,
                "nowcast_date": str(self._current_date.date()) if self._current_date else None,
                "target_column": self._target_column,
            },
        }

        return summary

    @handle_generic_errors_gracefully("while running prediction pipeline", {})
    def run_prediction_pipeline(
        self,
        candidates: List[str],
        current_date: str,
        states: Optional[List[str]] = None,
        lookback_days: int = 60,
        n_simulations: int = 10000,
        shy_voter_adjustment: float = 0.0,
        shy_candidate: str = "",
        uncertainty_std: float = 2.0,
    ) -> Dict[str, Any]:
        """Run the complete XGBoost prediction pipeline

        Args:
            candidates: List of candidate names
            current_date: Date for nowcast (ISO format YYYY-MM-DD) - REQUIRED
            states: Optional list of states to filter
            lookback_days: How many days of polling data to use
            n_simulations: Number of Monte Carlo simulations
            shy_voter_adjustment: Adjustment for shy voter effect
            shy_candidate: Which candidate gets the shy voter adjustment
            uncertainty_std: Standard deviation for uncertainty
        """

        self._logger.info(f"Starting XGBoost prediction pipeline for {self._election_year}")

        # Load data
        df = self.load_polling_data(candidates, current_date, states, lookback_days)
        if df.empty:
            return {"error": "No polling data available"}

        # Train XGBoost models
        model_scores = self.train_xgb_models(df)
        if not self._xgb_models:
            return {"error": "Failed to train XGBoost models"}

        # Generate XGBoost predictions for all states
        xgb_predictions = self.predict_with_xgb(
            states=states,
            shy_voter_adjustment=shy_voter_adjustment,
            shy_candidate=shy_candidate,
        )

        # Generate final election predictions with Monte Carlo simulation
        predictions = self.predict_election(
            xgb_predictions,
            n_simulations=n_simulations,
            uncertainty_std=uncertainty_std,
        )

        # Add model diagnostics
        predictions["model_diagnostics"] = self.get_model_summary()
        predictions["xgb_model_scores"] = model_scores

        self._logger.info("XGBoost prediction pipeline completed successfully")
        return predictions


def main():
    election_year = 2016
    candidates = ["Donald Trump", "Hillary Clinton"]
    current_date = "2016-11-08"
    lookback_days = 60
    n_simulations = 1000
    shy_adjustment = 2.0
    shy_candidate = candidates[0]
    uncertainty_std = 3.0

    predictor = ElectionNowcaster(election_year=election_year)

    results = predictor.run_prediction_pipeline(
        candidates=candidates,
        current_date=current_date,
        lookback_days=lookback_days,
        n_simulations=n_simulations,
        shy_voter_adjustment=shy_adjustment,
        shy_candidate=shy_candidate,
        uncertainty_std=uncertainty_std,
    )

    if "error" in results:
        print(f"Error: {results['error']}")
        return

    # Display key results
    candidates = results["candidates"]
    win_probs = results["win_probabilities"]
    expected_ec = results["expected_electoral_votes"]

    print(f"=== {election_year} Election Nowcast ===")
    print(f"Nowcast Date: {results['model_diagnostics']['data_summary']['nowcast_date']}")
    print(f"Data Source: {results['model_diagnostics']['data_summary']['target_column']}")
    if shy_adjustment > 0:
        print(f"Shy voter adjustment: +{shy_adjustment} points for {shy_candidate}")

    for i, candidate in enumerate(candidates):
        print(f"{candidate}:")
        print(f"\tWin Probability: {win_probs[i]:.1%}")
        print(f"\tExpected EC Votes: {expected_ec[i]:.1f}")

    total_ec = sum(expected_ec)
    print(f"\nTotal EC Votes: {total_ec:.1f}")

    print(f"\nModel used {results['model_diagnostics']['data_summary']['n_polls']} polls")
    date_range = results["model_diagnostics"]["data_summary"]["date_range"]
    print(f"Poll recency: {date_range['most_recent_poll_days_ago']} to {date_range['oldest_poll_days_ago']} days ago")
    print("XGBoost Model Scores:")
    for candidate, scores in results["xgb_model_scores"].items():
        print(f"\t{candidate}: R² = {scores['test_r2']:.3f} ({scores['n_samples']} samples)")


if __name__ == "__main__":
    main()
