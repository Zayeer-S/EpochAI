from epochai.common.database.dao.collection_statuses_dao import CollectionStatusesDAO
from epochai.common.database.dao.collection_targets_dao import CollectionTargetsDAO
from epochai.common.database.database import get_database
from epochai.common.enums import CollectionStatusNames
from epochai.common.logging_config import get_logger


class TargetStatusManagementService:
    """Manages the collection_status of collection_targets"""

    def __init__(self):
        self._logger = get_logger(__name__)
        self._db_connection = get_database()
        self._collection_statuses_dao = CollectionStatusesDAO()
        self._collection_targets_dao = CollectionTargetsDAO()

    def update_target_collection_status(
        self,
        collection_target_id: int,
        collection_status_name: CollectionStatusNames,
    ) -> bool:
        """Updates a collection status to the passed in value"""

        try:
            if not isinstance(collection_status_name, str):
                self._logger.error(f"Passed in value not str: {collection_status_name}")
                return False

            collection_status_id = self._collection_statuses_dao.get_id_by_name(collection_status_name)
            if not collection_status_id:
                self._logger.error(f"Status ID for '{collection_status_name}' not found for target ID '{collection_target_id}'")
                return False

            success = self._collection_targets_dao.update_collection_status_id(
                collection_target_id,
                collection_status_id,
            )

            if success:
                self._logger.info(f"Marked target {collection_target_id} as collected")
            return bool(success)

        except Exception as general_error:
            self._logger.error(f"Error marking target {collection_target_id} as '{collection_status_name}': {general_error}")
            return False
