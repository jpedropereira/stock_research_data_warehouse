from unittest.mock import MagicMock, patch

import pytest
from plugins.operators.enforce_latest_file_operator import EnforceLatestFileOperator


class TestEnforceLatestFileOperator:
    @patch("plugins.operators.enforce_latest_file_operator.PostgresHook")
    def test_execute_with_timestamp_in_file_name(self, mock_postgres_hook):
        mock_postgres = MagicMock()
        mock_postgres_hook.return_value = mock_postgres

        op = EnforceLatestFileOperator(
            task_id="deduplicate_staging",
            deduplication_columns=["symbol", "date"],
            postgres_conn_id="test_postgres",
            schema_name="staging",
            table_name="test_table_name",
            timestamp_in_file_name=True,
        )

        result = op.execute(context={})
        assert result is None
        assert mock_postgres.run.call_count == 2
        view_sql = mock_postgres.run.call_args_list[0][0][0]
        deletion_sql = mock_postgres.run.call_args_list[1][0][0]
        assert "CREATE OR REPLACE VIEW staging.v_test_table_name_latest_files" in view_sql
        assert "DELETE FROM staging.test_table_name" in deletion_sql
        assert "ORDER BY file_name DESC, load_timestamp DESC" in view_sql
        assert "PARTITION BY symbol, date" in view_sql

    @patch("plugins.operators.enforce_latest_file_operator.PostgresHook")
    def test_execute_with_extraction_datetime(self, mock_postgres_hook):
        mock_postgres = MagicMock()
        mock_postgres_hook.return_value = mock_postgres

        op = EnforceLatestFileOperator(
            task_id="deduplicate_staging",
            deduplication_columns=["symbol", "date"],
            postgres_conn_id="test_postgres",
            schema_name="staging",
            table_name="test_table_name",
            timestamp_in_file_name=False,
        )

        result = op.execute(context={})
        assert result is None
        assert mock_postgres.run.call_count == 2
        view_sql = mock_postgres.run.call_args_list[0][0][0]
        assert "CREATE OR REPLACE VIEW staging.v_test_table_name_latest_files" in view_sql
        assert "ORDER BY load_timestamp DESC, file_name DESC" in view_sql
        assert "PARTITION BY symbol, date" in view_sql

    @patch("plugins.operators.enforce_latest_file_operator.PostgresHook")
    def test_execute_with_empty_deduplication_columns(self, mock_postgres_hook):
        mock_postgres = MagicMock()
        mock_postgres_hook.return_value = mock_postgres

        op = EnforceLatestFileOperator(
            task_id="deduplicate_staging",
            deduplication_columns=[],
            postgres_conn_id="test_postgres",
            schema_name="staging",
            table_name="test_table_name",
        )

        with pytest.raises(ValueError) as exc:
            op.execute(context={})
        assert "deduplication_columns must not be empty" in str(exc.value)

    @patch("plugins.operators.enforce_latest_file_operator.PostgresHook")
    def test_execute_postgres_error(self, mock_postgres_hook):
        mock_postgres = MagicMock()
        mock_postgres.run.side_effect = Exception("Database error")
        mock_postgres_hook.return_value = mock_postgres

        op = EnforceLatestFileOperator(
            task_id="deduplicate_staging",
            deduplication_columns=["symbol", "date"],
            postgres_conn_id="test_postgres",
            schema_name="staging",
            table_name="test_table_name",
        )

        with pytest.raises(Exception) as exc:
            op.execute(context={})
        assert "Database error" in str(exc.value)
