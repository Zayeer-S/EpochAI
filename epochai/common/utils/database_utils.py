from typing import Optional, Tuple

from epochai.common.database.dao.collection_statuses_dao import CollectionStatusesDAO
from epochai.common.database.dao.collection_types_dao import CollectionTypesDAO
from epochai.common.database.dao.collector_names_dao import CollectorNamesDAO
from epochai.common.logging_config import get_logger


class DatabaseUtils:
    def __init__(self):
        self._logger = get_logger(__name__)
        self._collector_names_dao = CollectorNamesDAO()
        self._collection_statuses_dao = CollectionStatusesDAO()
        self._collection_types_dao = CollectionTypesDAO()

    def get_name_type_status_ids(
        self,
        collector_name: Optional[str] = None,
        collection_type: Optional[str] = None,
        collection_status_name: Optional[str] = None,
    ) -> Tuple[Optional[int], Optional[int], Optional[int]]:
        """Gets optional collector_name_id, optional collection_type_id and mandatory collection_status_id"""
        collector_name_id = collection_type_id = collection_status_id = None

        if collector_name:
            collector_obj = self._collector_names_dao.get_by_name(collector_name)
            if not (collector_obj and isinstance(collector_obj.id, int)):
                raise ValueError(f"Collector '{collector_name}' not found")
            collector_name_id = collector_obj.id

        if collection_type:
            collection_type_obj = self._collection_types_dao.get_by_name(collection_type)
            if not (collection_type_obj and isinstance(collection_type_obj.id, int)):
                raise ValueError(f"Collection type '{collection_type}' not found")
            collection_type_id = collection_type_obj.id

        if collection_status_name:
            collection_status_id = self._collection_statuses_dao.get_id_by_name(collection_status_name)
            if not isinstance(collection_status_id, int):
                raise ValueError(f"Collection status '{collection_status_name}' not found")

        return collector_name_id, collection_type_id, collection_status_id
