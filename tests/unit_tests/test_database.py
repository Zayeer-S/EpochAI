# ruff: noqa: SLF001, SIM117

import os
import threading
from unittest.mock import MagicMock, Mock, patch

import psycopg2
from psycopg2.extras import RealDictCursor
import pytest

from epochai.common.database.database import DatabaseConnection, close_database, get_database


@pytest.fixture
def mock_env_vars():
    """Mock environment variables for database connection"""
    return {
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_NAME": "test_db",
        "DB_USER": "test_user",
        "DB_PASSWORD": "test_password",
    }


@pytest.fixture
def db_connection(mock_env_vars):
    """Create DatabaseConnection instance with mocked environment variables"""
    with patch.dict(os.environ, mock_env_vars):
        return DatabaseConnection()


@pytest.fixture
def mock_psycopg2_connection():
    """Mock psycopg2 connection object"""
    mock_conn = MagicMock()
    mock_conn.autocommit = False
    mock_conn.cursor.return_value = MagicMock()
    mock_conn.commit.return_value = None
    mock_conn.rollback.return_value = None
    mock_conn.close.return_value = None
    return mock_conn


@pytest.fixture
def mock_cursor():
    """Mock database cursor"""
    cursor = Mock()
    cursor.execute.return_value = None
    cursor.fetchone.return_value = {"id": 1, "test": "data"}
    cursor.fetchall.return_value = [{"id": 1, "test": "data"}]
    cursor.rowcount = 1
    cursor.description = [("id",), ("test",)]
    cursor.close.return_value = None
    return cursor


class TestDatabaseConnectionInitialization:
    def test_initialization_with_valid_env_vars(self, mock_env_vars):
        with patch.dict(os.environ, mock_env_vars):
            db_conn = DatabaseConnection()

            assert db_conn._connection_parameters["host"] == "localhost"
            assert db_conn._connection_parameters["port"] == "5432"
            assert db_conn._connection_parameters["database"] == "test_db"
            assert db_conn._connection_parameters["user"] == "test_user"
            assert db_conn._connection_parameters["password"] == "test_password"
            assert db_conn._connection is None

    def test_initialization_missing_env_vars(self):
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                DatabaseConnection()

            assert "Missing required enviroment variables" in str(exc_info.value)
            assert "host" in str(exc_info.value)
            assert "port" in str(exc_info.value)

    def test_initialization_partial_missing_env_vars(self):
        partial_env = {"DB_HOST": "localhost", "DB_PORT": "5432"}
        with patch.dict(os.environ, partial_env, clear=True):
            with pytest.raises(ValueError) as exc_info:
                DatabaseConnection()

            error_message = str(exc_info.value)
            assert "database" in error_message
            assert "user" in error_message
            assert "password" in error_message


class TestConnectToDatabase:
    @patch("epochai.common.database.database.psycopg2.connect")
    def test_connect_success(self, mock_connect, db_connection, mock_psycopg2_connection):
        mock_connect.return_value = mock_psycopg2_connection

        result = db_connection.connect_to_database()

        assert result is True
        assert db_connection._connection == mock_psycopg2_connection
        mock_connect.assert_called_once_with(
            host="localhost",
            port="5432",
            database="test_db",
            user="test_user",
            password="test_password",
            cursor_factory=RealDictCursor,
        )
        assert db_connection._connection.autocommit is False

    @patch("epochai.common.database.database.psycopg2.connect")
    def test_connect_psycopg2_error(self, mock_connect, db_connection):
        mock_connect.side_effect = psycopg2.Error("Connection failed")

        result = db_connection.connect_to_database()

        assert result is False
        assert db_connection._connection is None

    @patch("epochai.common.database.database.psycopg2.connect")
    def test_connect_general_error(self, mock_connect, db_connection):
        mock_connect.side_effect = Exception("Unexpected error")

        result = db_connection.connect_to_database()

        assert result is False
        assert db_connection._connection is None


class TestDisconnectFromDatabase:
    def test_disconnect_success(self, db_connection, mock_psycopg2_connection):
        db_connection._connection = mock_psycopg2_connection

        db_connection.disconnect_from_database()

        mock_psycopg2_connection.close.assert_called_once()
        assert db_connection._connection is None

    def test_disconnect_with_error(self, db_connection, mock_psycopg2_connection):
        mock_psycopg2_connection.close.side_effect = Exception("Close error")
        db_connection._connection = mock_psycopg2_connection

        db_connection.disconnect_from_database()

        mock_psycopg2_connection.close.assert_called_once()
        assert db_connection._connection is None

    def test_disconnect_no_connection(self, db_connection):
        db_connection._connection = None

        # Should not raise any error
        db_connection.disconnect_from_database()

        assert db_connection._connection is None


class TestCheckIfConnected:
    def test_check_connected_true(self, db_connection, mock_psycopg2_connection, mock_cursor):
        db_connection._connection = mock_psycopg2_connection
        mock_psycopg2_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_psycopg2_connection.cursor.return_value.__exit__.return_value = None

        result = db_connection.check_if_connected()

        assert result is True
        mock_cursor.execute.assert_called_once_with("SELECT 1")

    def test_check_connected_no_connection(self, db_connection):
        db_connection._connection = None

        result = db_connection.check_if_connected()

        assert result is False

    def test_check_connected_psycopg2_error(self, db_connection, mock_psycopg2_connection):
        db_connection._connection = mock_psycopg2_connection
        mock_psycopg2_connection.cursor.return_value.__enter__.side_effect = psycopg2.Error("Query failed")

        result = db_connection.check_if_connected()

        assert result is False

    def test_check_connected_general_error(self, db_connection, mock_psycopg2_connection):
        db_connection._connection = mock_psycopg2_connection
        mock_psycopg2_connection.cursor.return_value.__enter__.side_effect = Exception("Unexpected error")

        result = db_connection.check_if_connected()

        assert result is False


class TestEnsureConnection:
    def test_ensure_connection_already_connected(self, db_connection):
        with patch.object(db_connection, "check_if_connected", return_value=True):
            result = db_connection.ensure_connection()

            assert result is True

    def test_ensure_connection_needs_reconnect(self, db_connection):
        with patch.object(db_connection, "check_if_connected", return_value=False), patch.object(
            db_connection,
            "connect_to_database",
            return_value=True,
        ):
            result = db_connection.ensure_connection()

            assert result is True

    def test_ensure_connection_reconnect_fails(self, db_connection):
        with patch.object(db_connection, "check_if_connected", return_value=False), patch.object(
            db_connection,
            "connect_to_database",
            return_value=False,
        ):
            result = db_connection.ensure_connection()

            assert result is False


class TestGetCursor:
    def test_get_cursor_success(self, db_connection, mock_psycopg2_connection, mock_cursor):
        db_connection._connection = mock_psycopg2_connection
        mock_psycopg2_connection.cursor.return_value = mock_cursor

        with patch.object(db_connection, "ensure_connection", return_value=True):
            with db_connection.get_cursor() as cursor:
                assert cursor == mock_cursor

        mock_cursor.close.assert_called_once()

    def test_get_cursor_ensure_connection_fails(self, db_connection):
        with patch.object(db_connection, "ensure_connection", return_value=False):
            with pytest.raises(Exception) as exc_info:
                with db_connection.get_cursor():
                    pass

            assert "Could not establish database connection" in str(exc_info.value)

    def test_get_cursor_with_exception_rollback(self, db_connection, mock_psycopg2_connection, mock_cursor):
        db_connection._connection = mock_psycopg2_connection
        mock_psycopg2_connection.cursor.return_value = mock_cursor

        with patch.object(db_connection, "ensure_connection", return_value=True):
            with pytest.raises(ValueError):
                with db_connection.get_cursor():
                    raise ValueError("Test error")

        mock_psycopg2_connection.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()


class TestExecuteSelectQuery:
    def test_execute_select_query_success(self, db_connection, mock_cursor):
        expected_results = [{"id": 1, "name": "test"}]
        mock_cursor.fetchall.return_value = expected_results

        with patch.object(db_connection, "get_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor
            mock_get_cursor.return_value.__exit__.return_value = None

            results = db_connection.execute_select_query("SELECT * FROM test", ("param",))

            assert results == expected_results
            mock_cursor.execute.assert_called_once_with("SELECT * FROM test", ("param",))
            mock_cursor.fetchall.assert_called_once()

    def test_execute_select_query_no_params(self, db_connection, mock_cursor):
        expected_results = []
        mock_cursor.fetchall.return_value = expected_results

        with patch.object(db_connection, "get_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor
            mock_get_cursor.return_value.__exit__.return_value = None

            results = db_connection.execute_select_query("SELECT * FROM test")

            assert results == expected_results
            mock_cursor.execute.assert_called_once_with("SELECT * FROM test", None)


class TestExecuteInsertQuery:
    def test_execute_insert_query_with_id_return(self, db_connection, mock_cursor, mock_psycopg2_connection):
        db_connection._connection = mock_psycopg2_connection
        mock_cursor.description = [("id",)]
        mock_cursor.rowcount = 1
        mock_cursor.fetchone.return_value = {"id": 123}

        with patch.object(db_connection, "get_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor
            mock_get_cursor.return_value.__exit__.return_value = None

            result = db_connection.execute_insert_query("INSERT INTO test VALUES (%s)", ("value",))

            assert result == 123
            mock_cursor.execute.assert_called_once_with("INSERT INTO test VALUES (%s)", ("value",))
            mock_psycopg2_connection.commit.assert_called_once()

    def test_execute_insert_query_no_id_return_rowcount(
        self,
        db_connection,
        mock_cursor,
        mock_psycopg2_connection,
    ):
        db_connection._connection = mock_psycopg2_connection
        mock_cursor.description = None
        mock_cursor.rowcount = 1

        with patch.object(db_connection, "get_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor
            mock_get_cursor.return_value.__exit__.return_value = None

            result = db_connection.execute_insert_query("INSERT INTO test VALUES (%s)", ("value",))

            assert result == 1
            mock_psycopg2_connection.commit.assert_called_once()

    def test_execute_insert_query_no_id_in_result(
        self,
        db_connection,
        mock_cursor,
        mock_psycopg2_connection,
    ):
        db_connection._connection = mock_psycopg2_connection
        mock_cursor.description = [("name",)]
        mock_cursor.rowcount = 1
        mock_cursor.fetchone.return_value = {"name": "test"}

        with patch.object(db_connection, "get_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor
            mock_get_cursor.return_value.__exit__.return_value = None

            result = db_connection.execute_insert_query("INSERT INTO test VALUES (%s)", ("value",))

            assert result == 1  # Should return rowcount when no id in result


class TestExecuteUpdateDeleteQuery:
    def test_execute_update_delete_query_success(self, db_connection, mock_cursor, mock_psycopg2_connection):
        db_connection._connection = mock_psycopg2_connection
        mock_cursor.rowcount = 3

        with patch.object(db_connection, "get_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor
            mock_get_cursor.return_value.__exit__.return_value = None

            result = db_connection.execute_update_delete_query("UPDATE test SET name = %s", ("new_name",))

            assert result == 3
            mock_cursor.execute.assert_called_once_with("UPDATE test SET name = %s", ("new_name",))
            mock_psycopg2_connection.commit.assert_called_once()


class TestExecuteTransaction:
    def test_execute_transaction_success(self, db_connection, mock_cursor, mock_psycopg2_connection):
        db_connection._connection = mock_psycopg2_connection
        operations = [
            ("INSERT INTO test VALUES (%s)", ("value1",)),
            ("UPDATE test SET name = %s", ("value2",)),
        ]

        with patch.object(db_connection, "get_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor
            mock_get_cursor.return_value.__exit__.return_value = None

            result = db_connection.execute_transaction(operations)

            assert result is True
            assert mock_cursor.execute.call_count == 2
            mock_psycopg2_connection.commit.assert_called_once()

    def test_execute_transaction_failure_rollback(self, db_connection, mock_cursor, mock_psycopg2_connection):
        db_connection._connection = mock_psycopg2_connection
        mock_cursor.execute.side_effect = [None, Exception("Query failed")]
        operations = [
            ("INSERT INTO test VALUES (%s)", ("value1",)),
            ("UPDATE test SET name = %s", ("value2",)),
        ]

        with patch.object(db_connection, "get_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor
            mock_get_cursor.return_value.__exit__.return_value = None

            result = db_connection.execute_transaction(operations)

            assert result is False
            mock_psycopg2_connection.rollback.assert_called_once()


class TestTestConnection:
    def test_test_connection_success(self, db_connection, mock_cursor):
        mock_cursor.fetchone.side_effect = [
            {"version": "PostgreSQL 13.0"},
            {"exists": True},
        ]

        with patch.object(db_connection, "ensure_connection", return_value=True), patch.object(
            db_connection,
            "get_cursor",
        ) as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor
            mock_get_cursor.return_value.__exit__.return_value = None

            result = db_connection.test_connection()

            assert result is True
            assert mock_cursor.execute.call_count == 2

    def test_test_connection_ensure_connection_fails(self, db_connection):
        with patch.object(db_connection, "ensure_connection", return_value=False):
            result = db_connection.test_connection()

            assert result is False

    def test_test_connection_query_fails(self, db_connection, mock_cursor):
        mock_cursor.execute.side_effect = Exception("Query failed")

        with patch.object(db_connection, "ensure_connection", return_value=True), patch.object(
            db_connection,
            "get_cursor",
        ) as mock_get_cursor:
            mock_get_cursor.return_value.__enter__.return_value = mock_cursor
            mock_get_cursor.return_value.__exit__.return_value = None

            result = db_connection.test_connection()

            assert result is False


class TestSingletonPattern:
    def test_get_database_singleton(self):
        # Clear any existing instance
        import epochai.common.database.database as db_module

        db_module._db_instance = None

        with patch.dict(
            os.environ,
            {
                "DB_HOST": "localhost",
                "DB_PORT": "5432",
                "DB_NAME": "test_db",
                "DB_USER": "test_user",
                "DB_PASSWORD": "test_password",
            },
        ):
            db1 = get_database()
            db2 = get_database()

            assert db1 is db2
            assert isinstance(db1, DatabaseConnection)

    def test_close_database(self):
        import epochai.common.database.database as db_module

        # Set up a mock instance
        mock_instance = Mock()
        db_module._db_instance = mock_instance

        close_database()

        mock_instance.disconnect_from_database.assert_called_once()
        assert db_module._db_instance is None

    def test_close_database_no_instance(self):
        import epochai.common.database.database as db_module

        db_module._db_instance = None

        # Should not raise any error
        close_database()

        assert db_module._db_instance is None

    def test_singleton_thread_safety(self):
        """Test that singleton works correctly with multiple threads"""
        import epochai.common.database.database as db_module

        db_module._db_instance = None

        instances = []

        def create_instance():
            with patch.dict(
                os.environ,
                {
                    "DB_HOST": "localhost",
                    "DB_PORT": "5432",
                    "DB_NAME": "test_db",
                    "DB_USER": "test_user",
                    "DB_PASSWORD": "test_password",
                },
            ):
                instances.append(get_database())

        threads = [threading.Thread(target=create_instance) for _ in range(5)]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # All instances should be the same object
        assert len({id(instance) for instance in instances}) == 1
