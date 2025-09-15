from typing import Any, Dict, List, Optional, Protocol, Union, runtime_checkable

from epochai.common.database.models import CleanedDataMetadataSchemas, RawDataMetadataSchemas

MetadataSchemaModel = Union[CleanedDataMetadataSchemas, RawDataMetadataSchemas]


@runtime_checkable
class MetadataSchemaDAOProtocol(Protocol):
    """Protocol defining the interface that DynamicSchemaUtils needs from schema DAOs"""

    def create_schema(self, metadata_schema: Dict[str, Any]) -> Optional[int]:
        """Creates a new metadata schema and returns its ID"""
        ...

    def get_by_id(self, schema_id: int) -> Optional[MetadataSchemaModel]:
        """Gets metadata schema by ID"""
        ...

    def get_all(self) -> List[MetadataSchemaModel]:
        """Gets all metadata schemas"""
        ...

    def find_schema_by_content(self, schema_content: Dict[str, Any]) -> Optional[MetadataSchemaModel]:
        """Finds a schema that matches the given content structure"""
        ...

    def get_or_create_schema(self, metadata_schema: Dict[str, Any]) -> Optional[MetadataSchemaModel]:
        """Get existing schema or create new one if doesn't exist"""
        ...
