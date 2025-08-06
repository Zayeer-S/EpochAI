from contextlib import contextmanager
import os
import threading
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor


from epochai.common.logging_config import get_logger

load_dotenv()

logger = get_logger(__name__)


class DatabaseConnection:
    def __init__(self):
        self.logger = get_logger(__name__)
        self._connection_parameters = self._load_connection_params()
        self._connection = None

    def _load_connection_params(self) -> Dict[str, str]:
        """Loads database connection parameters via environment variables"""

        connection_params = {
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
            "database": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
        }

        missing_vars = [key for key, value in connection_params.items() if value is None]
        if missing_vars:
            raise ValueError(f"Missing required enviroment variables: {missing_vars}")

        self.logger.info(
            f"Database connection configured for: {connection_params['host']}:{connection_params['port']}:{connection_params['database']}",  # noqa
        )

        return connection_params

    def connect_to_database(self) -> bool:
        """Establish connection to the database"""
        try:
            self._connection = psycopg2.connect(
                **self._connection_parameters,
                cursor_factory=RealDictCursor,
            )
            self._connection.autocommit = False
            self.logger.info("Successfully connected to database")
            return True

        except psycopg2.Error as psycopg2_error:
            self.logger.error(f"Failed to connect to database: {psycopg2_error}")
            return False

        except Exception as general_error:
            self.logger.error(f"Unexpected error occurred while connecting to databse: {general_error}")
            return False

    def disconnect_from_database(self):
        """Close the database connection"""
        if self._connection:
            try:
                self._connection.close()
                self.logger.info("Database connection successfully closed")
            except Exception as general_error:
                self.logger.error(f"Error closing database connection: {general_error}")
            finally:
                self._connection = None

    def check_if_connected(self) -> bool:
        """Checks if database connection is active or not"""
        if not self._connection:
            return False

        try:
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            return True

        except psycopg2.Error as psycopg2_error:
            self.logger.error(f"Error checking if database connection is active: {psycopg2_error}")
            return False

        except Exception as general_error:
            self.logger.error(f"Unexpected error checking if database connection is active: {general_error}")
            return False

    def ensure_connection(self) -> bool:
        """Ensure we have an active database connection"""
        if not self.check_if_connected():
            return self.connect_to_database()
        return True

    @contextmanager
    def get_cursor(self):
        """Context manager for database cursors with automatic connnection handling"""
        if not self.ensure_connection():
            raise Exception("Could not establish database connection")

        cursor = None
        try:
            cursor = self._connection.cursor()
            yield cursor
        except Exception as general_error:
            if self._connection:
                self._connection.rollback()
            self.logger.error(f"Database operation failed: {general_error}")
            raise
        finally:
            if cursor:
                cursor.close()

    def execute_select_query(
        self,
        query: str,
        params: Optional[tuple] = None,
    ) -> List[Dict[str, Any]]:
        """Executes SELECT query and returns its results"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)

            results: List[Dict[str, Any]] = cursor.fetchall()

            return results

    def execute_insert_query(
        self,
        query: str,
        params: Optional[tuple] = None,
    ) -> Optional[int]:
        """Executes an INSERT query and returns the inserted row's ID"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)

            if cursor.description and cursor.rowcount > 0:
                result = cursor.fetchone()
                # ruff: noqa
                if result and "id" in result:
                    inserted_id = result["id"]
                else:
                    inserted_id = cursor.rowcount
                # ruff: enable

            else:
                inserted_id = cursor.rowcount

            self._connection.commit()
            return int(inserted_id)

    def execute_update_delete_query(
        self,
        query: str,
        params: Optional[tuple] = None,
    ) -> int:
        """Executes UPDATE/DELETE queries and returns number of affected rows"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            affected_rows = cursor.rowcount
            self._connection.commit()
            return int(affected_rows)

    def execute_transaction(
        self,
        operations: List[tuple],
    ) -> bool:
        """Executes multiple operations in a single transaction"""
        with self.get_cursor() as cursor:
            try:
                for query, params in operations:
                    cursor.execute(query, params)
                self._connection.commit()
                return True
            except Exception as general_error:
                self._connection.rollback()
                self.logger.error(f"Transaction failed, rolled back: {general_error}")
                return False

    def test_connection(
        self,
    ) -> bool:
        """Tests database connection and logs any connection info"""
        if not self.ensure_connection():
            return False

        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT version()")
                version = cursor.fetchone()
                self.logger.info(f"Connected to version: {version['version']}")

                cursor.execute(
                    """
                               SELECT EXISTS (
                                   SELECT FROM information_schema.tables
                                   WHERE table_schema = 'public'
                                   AND table_name = 'collection_configs'
                               )
                               """,
                )
                cursor.fetchone()["exists"]

            return True

        except Exception as general_error:
            self.logger.error(f"Database test failed: {general_error}")
            return False


_db_instance = None
_lock = threading.Lock()


def get_database() -> DatabaseConnection:
    """Gets global database instance via a singleton"""
    global _db_instance
    if _db_instance is None:
        with _lock:
            if _db_instance is None:
                _db_instance = DatabaseConnection()
    return _db_instance


def close_database():
    """Closes global database instance"""
    global _db_instance
    if _db_instance:
        _db_instance.disconnect_from_database()
        _db_instance = None
