from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any, Dict, List, Optional


@dataclass
class CollectorNames:
    """collector_names table model"""

    id: Optional[int] = None
    collector_name: str = ""
    updated_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
    ) -> "CollectorNames":
        """Creates instance from database row dictionary"""
        return cls(
            id=data.get("id"),
            collector_name=data.get("collector_name"),
            updated_at=data.get("updated_at"),
            created_at=data.get("created_at"),
        )


@dataclass
class CollectionTypes:
    """collection_types table model"""

    id: Optional[int] = None
    collection_type: str = ""
    updated_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
    ) -> "CollectionTypes":
        """Creates instance from database row dictionary"""
        return cls(
            id=data.get("id"),
            collection_type=data.get("collection_type"),
            updated_at=data.get("updated_at"),
            created_at=data.get("created_at"),
        )


@dataclass
class CollectionTargets:
    """collection_targets table model"""

    id: Optional[int] = None
    collector_name_id: int = 0
    collection_type_id: int = 0
    language_code: str = ""
    collection_name: str = ""
    is_collected: bool = False
    updated_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
    ) -> "CollectionTargets":
        """Creates instance from database row dictionary"""
        return cls(
            id=data.get("id"),
            collector_name_id=data.get("collector_name_id"),
            collection_type_id=data.get("collection_type_id"),
            language_code=data.get("language_code"),
            collection_name=data.get("collection_name"),
            is_collected=data.get("is_collected"),
            updated_at=data.get("updated_at"),
            created_at=data.get("created_at"),
        )


@dataclass
class AttemptStatuses:
    """attempt_statuses table model"""

    id: Optional[int] = None
    attempt_status_name: str = ""
    updated_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
    ) -> "AttemptStatuses":
        """Creates instance from database row dictionary"""
        return cls(
            id=data.get("id"),
            attempt_status_name=data.get("attempt_status_name"),
            updated_at=data.get("updated_at"),
            created_at=data.get("created_at"),
        )


@dataclass
class ErrorTypes:
    """error_types table model"""

    id: Optional[int] = None
    error_type_name: str = ""
    updated_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
    ) -> "ErrorTypes":
        """Creates instance from database row dictionary"""
        return cls(
            id=data.get("id"),
            error_type_name=data.get("error_type_name"),
            updated_at=data.get("updated_at"),
            created_at=data.get("created_at"),
        )


@dataclass
class CollectionAttempts:
    """collection_attempts table model"""

    id: Optional[int] = None
    collection_target_id: int = 0
    language_code_used: str = ""
    search_term_used: str = ""
    attempt_status_id: int = 0
    error_type_id: int = 0
    error_message: str = ""
    created_at: Optional[datetime] = None

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
    ) -> "CollectionAttempts":
        """Creates instance from database row dictionary"""
        return cls(
            id=data.get("id"),
            collection_target_id=data.get("collection_target_id"),
            language_code_used=data.get("language_code_used"),
            search_term_used=data.get("search_term_used"),
            attempt_status_id=data.get("attempt_status_id"),
            error_type_id=data.get("error_type_id"),
            error_message=data.get("error_message"),
            created_at=data.get("created_at"),
        )


@dataclass
class ValidationStatuses:
    """validation_statuses table model"""

    id: Optional[int] = None
    validation_status_name: str = ""
    updated_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
    ) -> "ValidationStatuses":
        """Creates instance from database row dictionary"""
        return cls(
            id=data.get("id"),
            validation_status_name=data.get("validation_status_name"),
            updated_at=data.get("updated_at"),
            created_at=data.get("created_at"),
        )


@dataclass
class RawDataMetadataSchemas:
    """raw_data_metadata_schemas table model"""

    id: Optional[int] = None
    metadata_schema: Dict[str, Any] = None
    updated_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
    ) -> "RawDataMetadataSchemas":
        "Creates instance from database row dictionary"
        schema = data.get("metadata_schema")
        if isinstance(schema, str):
            try:
                schema = json.loads(schema)
            except json.JSONDecodeError:
                schema = {}

        return cls(
            id=data.get("id"),
            metadata_schema=schema,
            updated_at=data.get("updated_at"),
            created_at=data.get("created_at"),
        )


@dataclass
class RawData:
    """raw_data table model"""

    id: Optional[int] = None
    collection_attempt_id: int = 0
    raw_data_metadata_schema_id: int = 0
    title: str = ""
    language_code: str = ""
    url: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    validation_status_id: int = 0
    validation_error: Optional[Dict[str, Any]] = None
    filepath_of_save: str = ""
    created_at: Optional[datetime] = None

    def __post_init__(self):
        if self.validation_error is None:
            self.validation_error = {}
        if self.metadata is None:
            self.metadata = {}

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
    ) -> "RawData":
        """Creates instance from database raw dictionary"""
        validation_error = data.get("validation_error")
        if isinstance(validation_error, str):
            try:
                validation_error = json.loads(validation_error)
            except json.JSONDecodeError:
                validation_error = {}

        metadata = data.get("metadata")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}

        return cls(
            id=data.get("id"),
            collection_attempt_id=data.get("collection_attempt_id"),
            raw_data_metadata_schema_id=data.get("raw_data_metadata_schema_id"),
            title=data.get("title"),
            language_code=data.get("language_code"),
            url=data.get("url"),
            metadata=metadata,  # CHANGE HERE: Add metadata field
            validation_status_id=data.get("validation_status_id"),
            validation_error=validation_error,
            filepath_of_save=data.get("filepath_of_save"),
            created_at=data.get("created_at"),
        )


@dataclass
class CleanedData:
    """cleaned_data table model"""

    id: Optional[int] = None
    raw_data_id: int = 0
    cleaned_data_metadata_schema_id: int = 0
    title: str = ""
    language_code: str = ""
    url: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    validation_status_id: int = 0
    validation_error: Optional[Dict[str, Any]] = None
    cleaner_used: str = ""
    cleaner_version: str = ""
    cleaning_time_ms: int = 0
    cleaned_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    def __post_init__(self):
        if self.validation_error is None:
            self.validation_error = {}
        if self.metadata is None:
            self.metadata = {}

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
    ) -> "CleanedData":
        """Creates instance from database row dictionary"""
        validation_error = data.get("validation_error")
        if isinstance(validation_error, str):
            try:
                validation_error = json.loads(validation_error)
            except json.JSONDecodeError:
                validation_error = {}

        metadata = data.get("metadata")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}

        return cls(
            id=data.get("id"),
            raw_data_id=data.get("raw_data_id"),
            cleaned_data_metadata_schema_id=data.get("cleaned_data_metadata_schema_id"),
            title=data.get("title"),
            language_code=data.get("language_code"),
            url=data.get("url"),
            metadata=metadata,
            validation_status_id=data.get("validation_status_id"),
            validation_error=validation_error,
            cleaner_used=data.get("cleaner_used"),
            cleaner_version=data.get("cleaner_version"),
            cleaning_time_ms=data.get("cleaning_time_ms"),
            cleaned_at=data.get("cleaned_at"),
            created_at=data.get("created_at"),
        )


@dataclass
class CleanedDataMetadataSchemas:
    """cleaned_data_metadata_schemas table model"""

    id: Optional[int] = None
    metadata_schema: Dict[str, Any] = None
    updated_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
    ) -> "CleanedDataMetadataSchemas":
        """Creates instance from database row dictionary"""
        schema = data.get("metadata_schema")
        if isinstance(schema, str):
            try:
                schema = json.loads(schema)
            except json.JSONDecodeError:
                schema = {}

        return cls(
            id=data.get("id"),
            metadata_schema=schema,
            updated_at=data.get("updated_at"),
            created_at=data.get("created_at"),
        )


@dataclass
class RunTypes:
    """run_types table"""

    id: Optional[int] = None
    run_type_name: str = ""
    updated_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
    ) -> "RunTypes":
        """Create instance from database row dict"""
        return cls(
            id=data.get("id"),
            run_type_name=data.get("run_type_name"),
            updated_at=data.get("updated_at"),
            created_at=data.get("created_at"),
        )


@dataclass
class RunStatuses:
    """run_statuses table model"""

    id: Optional[int] = None
    run_status_name: str = ""
    updated_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
    ) -> "RunStatuses":
        """Creates instance from database row dict"""
        return cls(
            id=data.get("id"),
            run_status_name=data.get("run_status_name"),
            updated_at=data.get("updated_at"),
            created_at=data.get("created_at"),
        )


@dataclass
class RunCollectionMetadata:
    """run_collection_metadata table model"""

    id: Optional[int] = None
    collection_attempt_id: Optional[int] = 0
    run_type_id: Optional[int] = 0
    run_status_id: Optional[int] = 0
    attempts_successful: Optional[int] = 0
    attempts_failed: Optional[int] = 0
    config_used: Optional[Dict[str, Any]] = None
    completed_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
    ) -> "RunCollectionMetadata":
        """Creates instance from database row dict"""
        config_used = data.get("config_used")
        if isinstance(config_used, str):
            try:
                config_used = json.loads(config_used)
            except json.JSONDecodeError:
                config_used = {}

        return cls(
            id=data.get("id"),
            collection_attempt_id=data.get("collection_attempt_id"),
            run_type_id=data.get("run_type_id"),
            run_status_id=data.get("run_status_id"),
            attempts_successful=data.get("attempts_successful"),
            attempts_failed=data.get("attempts_failed"),
            config_used=config_used,
            completed_at=data.get("completed_at"),
            created_at=data.get("created_at"),
        )


@dataclass
class LinkAttemptsToRuns:
    """link_attempts_to_runs table model"""

    id: Optional[int] = None
    collection_attempt_id: int = 0
    run_collection_metadata_id: int = 0

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
    ) -> "LinkAttemptsToRuns":
        return cls(
            id=data.get("id"),
            collection_attempt_id=data.get("collection_attempt_id"),
            run_collection_metadata_id=data.get("run_collection_metadata_id"),
        )


@dataclass
class DebugWikipediaResults:
    """debug_wikipedia_results table model"""

    id: Optional[int] = None
    collection_target_id: int = 0
    search_term_used: str = ""
    language_code_used: str = ""
    test_status: str = ""
    search_results_found: List[str] = None
    error_message: str = ""
    test_duration: int = 0
    created_at: Optional[datetime] = None

    def __post_init__(self):
        if self.search_results_found is None:
            self.search_results_found = []

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
    ) -> "DebugWikipediaResults":
        """Creates instance from database row dict"""
        search_results_found = data.get("search_results_found")
        if isinstance(search_results_found, str):
            try:
                search_results_found = json.loads(search_results_found)
            except json.JSONDecodeError:
                search_results_found = []

        return cls(
            id=data.get("id"),
            collection_target_id=data.get("collection_target_id"),
            search_term_used=data.get("search_term_used"),
            # CHANGE HERE: Fix field mapping from language_code to language_code_used
            language_code_used=data.get("language_code_used"),
            test_status=data.get("test_status"),
            search_results_found=search_results_found,
            error_message=data.get("error_message"),
            test_duration=data.get("test_duration"),
            created_at=data.get("created_at"),
        )


@dataclass
class TrackSchemaMigrations:
    id: int
    version: str
    filename: str
    checksum: Optional[str]
    executed_at: datetime
    execution_time_ms: float
    status: str
    error_message: Optional[str]
    rolled_back_at: Optional[datetime]
    created_at: datetime

    @classmethod
    def from_dict(cls, data: dict) -> "TrackSchemaMigrations":
        return cls(**data)
