from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from plugins.operators.extract_to_staging_operator import ExtractToStagingOperator


class TestExtractToStagingOperator:
    @patch("plugins.operators.extract_to_staging_operator.S3Hook")
    @patch("plugins.operators.extract_to_staging_operator.PostgresHook")
    @patch("plugins.operators.extract_to_staging_operator.load_table_config")
    @patch("pandas.read_csv")
    def test_execute_success(
        self,
        mock_read_csv,
        mock_load_table_config,
        mock_postgres_hook,
        mock_s3_hook,
    ):
        # Setup mocks
        mock_s3 = MagicMock()
        mock_s3.get_key.return_value.get.return_value = {
            "Body": MagicMock(read=lambda: b"col1,col2\n1,2\n3,4")
        }
        mock_s3_hook.return_value = mock_s3

        mock_postgres = MagicMock()
        mock_postgres_hook.return_value = mock_postgres

        mock_load_table_config.return_value = {
            "col1": {"csv_col": "col1", "df_dtype": "int"},
            "col2": {"csv_col": "col2", "df_dtype": "int"},
            "file_name": {"csv_col": "file_name", "df_dtype": "str"},
        }

        df = pd.DataFrame({"col1": [1, 3], "col2": [2, 4]})
        mock_read_csv.return_value = df

        op = ExtractToStagingOperator(
            task_id="test_task",
            s3_conn_id="test_s3",
            bucket_name="test_bucket",
            object_key="test.csv",
            postgres_conn_id="test_postgres",
            schema_name="public",
            table_name="test_table",
            column_mapping_yaml_path="dummy.yaml",
        )

        result = op.execute(context={})
        assert result == 2
        mock_postgres.run.assert_called()
        mock_postgres.copy_expert.assert_called()

    @patch("plugins.operators.extract_to_staging_operator.S3Hook")
    @patch("plugins.operators.extract_to_staging_operator.PostgresHook")
    @patch("plugins.operators.extract_to_staging_operator.load_table_config")
    @patch("pandas.read_csv")
    def test_execute_empty_csv(
        self,
        mock_read_csv,
        mock_load_table_config,
        mock_postgres_hook,
        mock_s3_hook,
    ):
        mock_s3 = MagicMock()
        mock_s3.get_key.return_value.get.return_value = {
            "Body": MagicMock(read=lambda: b"col1,col2\n")
        }
        mock_s3_hook.return_value = mock_s3

        mock_postgres = MagicMock()
        mock_postgres_hook.return_value = mock_postgres

        mock_load_table_config.return_value = {
            "col1": {"csv_col": "col1", "df_dtype": "int"},
            "col2": {"csv_col": "col2", "df_dtype": "int"},
            "file_name": {"csv_col": "file_name", "df_dtype": "str"},
        }

        df = pd.DataFrame()
        mock_read_csv.return_value = df

        op = ExtractToStagingOperator(
            task_id="test_task",
            s3_conn_id="test_s3",
            bucket_name="test_bucket",
            object_key="test.csv",
            postgres_conn_id="test_postgres",
            schema_name="public",
            table_name="test_table",
            column_mapping_yaml_path="dummy.yaml",
        )

        result = op.execute(context={})
        assert result == 0
        mock_postgres.run.assert_not_called()
        mock_postgres.copy_expert.assert_not_called()

    @patch("plugins.operators.extract_to_staging_operator.S3Hook")
    @patch("plugins.operators.extract_to_staging_operator.PostgresHook")
    @patch("plugins.operators.extract_to_staging_operator.load_table_config")
    @patch("pandas.read_csv")
    def test_execute_missing_column(
        self,
        mock_read_csv,
        mock_load_table_config,
        mock_postgres_hook,
        mock_s3_hook,
    ):
        mock_s3 = MagicMock()
        mock_s3.get_key.return_value.get.return_value = {
            "Body": MagicMock(read=lambda: b"col1\n1\n2")
        }
        mock_s3_hook.return_value = mock_s3

        mock_postgres = MagicMock()
        mock_postgres_hook.return_value = mock_postgres

        mock_load_table_config.return_value = {
            "col1": {"csv_col": "col1", "df_dtype": "int"},
            "col2": {"csv_col": "col2", "df_dtype": "int"},
            "file_name": {"csv_col": "file_name", "df_dtype": "str"},
        }

        df = pd.DataFrame({"col1": [1, 2]})
        mock_read_csv.return_value = df

        op = ExtractToStagingOperator(
            task_id="test_task",
            s3_conn_id="test_s3",
            bucket_name="test_bucket",
            object_key="test.csv",
            postgres_conn_id="test_postgres",
            schema_name="public",
            table_name="test_table",
            column_mapping_yaml_path="dummy.yaml",
        )

        with pytest.raises(ValueError) as exc:
            op.execute(context={})
        assert "CSV columns not found in file" in str(exc.value)

    @patch("plugins.operators.extract_to_staging_operator.S3Hook")
    @patch("plugins.operators.extract_to_staging_operator.PostgresHook")
    @patch("plugins.operators.extract_to_staging_operator.load_table_config")
    @patch("pandas.read_csv")
    def test_execute_with_add_label_columns(
        self,
        mock_read_csv,
        mock_load_table_config,
        mock_postgres_hook,
        mock_s3_hook,
    ):
        # Setup mocks
        mock_s3 = MagicMock()
        mock_s3.get_key.return_value.get.return_value = {
            "Body": MagicMock(read=lambda: b"col1,col2\n1,2\n3,4")
        }
        mock_s3_hook.return_value = mock_s3

        mock_postgres = MagicMock()
        mock_postgres_hook.return_value = mock_postgres

        mock_load_table_config.return_value = {
            "col1": {"csv_col": "col1", "df_dtype": "int"},
            "col2": {"csv_col": "col2", "df_dtype": "int"},
            "file_name": {"csv_col": "file_name", "df_dtype": "str"},
            "label_col": {"csv_col": "label_col", "df_dtype": "str"},
        }

        df = pd.DataFrame(
            {
                "col1": [1, 3],
                "col2": [2, 4],
            }
        )
        mock_read_csv.return_value = df

        op = ExtractToStagingOperator(
            task_id="test_task",
            s3_conn_id="test_s3",
            bucket_name="test_bucket",
            object_key="test.csv",
            postgres_conn_id="test_postgres",
            schema_name="public",
            table_name="test_table",
            column_mapping_yaml_path="dummy.yaml",
            add_label_columns={"label_col": "label_value"},
        )

        result = op.execute(context={})
        assert result == 2
        # Check that the label column was added with the correct value for all rows
        args, _ = mock_postgres.copy_expert.call_args
        # The temporary file path is args[1], read it back to check the content
        written_df = pd.read_csv(args[1], header=None)
        # The label column should be present and have the correct value in all rows (robust to column order)
        assert any((written_df == "label_value").all())
