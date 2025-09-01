import datetime
import os
from contextlib import contextmanager
from io import StringIO

import pandas as pd
import psycopg2
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from include.utils import load_table_config


class ExtractToStagingOperator(BaseOperator):
    """
    Generic operator for reading data from MinIO/S3 and loading it into staging tables.

    This operator:
    1. Downloads a CSV file from MinIO/S3
    2. Reads it into a pandas DataFrame
    3. Loads column mapping and data types from a central YAML config file (by table name)
    4. Clears existing data for the specified file (idempotent)
    5. Performs bulk insert into the staging table

    :param s3_conn_id: Airflow connection ID for S3/MinIO
    :param bucket_name: Source bucket name
    :param object_key: Object key (file path) in the bucket
    :param postgres_conn_id: Airflow connection ID for PostgreSQL
    :param schema_name: Target schema name
    :param table_name: Target table name (used to look up column mapping in YAML)
    :param column_mapping_yaml_path: Path to YAML file with column mappings
    :param date_column: Column name used for date range filtering (default: 'date')
    :param file_name_column: Column name used for file-based duplicate prevention (default: 'file_name')
    :param csv_delimiter: CSV delimiter character (default: ',')
    :param add_label_columns: (Optional) Dictionary of {column_name: value} to add constant label
        columns to the ingested data. Pass this if you want to add one or more columns
        with a certain label and value to all rows.
    :param col_mappings: (Optional) Dictionary mapping original column names to new names.
                        Used to rename columns in the DataFrame before further processing. Example: {"shares": "quantity"}

    Note:
            - All column mappings and data types must be defined in the YAML file under the 'tables' key.
            - Use table_name as the subkey for each table's config.
            - The loaded mapping is logged for traceability.
            - No direct column_mapping argument is supported; all mappings are managed centrally in YAML for
                consistency and maintainability.
    """

    template_fields = ("object_key",)

    def __init__(
        self,
        s3_conn_id: str,
        bucket_name: str,
        object_key: str,
        postgres_conn_id: str,
        schema_name: str,
        table_name: str,
        column_mapping_yaml_path: str,
        date_column: str = "date",
        file_name_column: str = "file_name",
        csv_delimiter: str = ",",
        add_label_columns: dict[str, str] = None,
        col_mappings: dict[str, str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.s3_conn_id = s3_conn_id
        self.bucket_name = bucket_name
        self.object_key = object_key
        self.postgres_conn_id = postgres_conn_id
        self.schema_name = schema_name
        self.table_name = table_name
        self.date_column = date_column
        self.file_name_column = file_name_column
        self.column_mapping_yaml_path = column_mapping_yaml_path
        self.csv_delimiter = csv_delimiter
        self.add_label_columns = add_label_columns
        self.col_mappings = col_mappings

    def _normalize_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize DataFrame for PostgreSQL compatibility.

        :param data: Input DataFrame
        :return: Normalized DataFrame
        """
        self.log.info("Starting data normalization")

        # replace NaN with None for PostgreSQL compatibility
        data = data.where(pd.notnull(data), None)

        data = data.replace([float("inf"), float("-inf")], None)

        # Handle datetime columns - ensure they're in ISO format
        for col in data.columns:
            if pd.api.types.is_datetime64_any_dtype(data[col]):
                data[col] = data[col].dt.strftime("%Y-%m-%d %H:%M:%S")

        # Convert boolean columns to lowercase strings for PostgreSQL
        for col in data.columns:
            if pd.api.types.is_bool_dtype(data[col]):
                data[col] = data[col].map({True: "true", False: "false"})

        self.log.info("Data normalization completed")
        return data

    @contextmanager
    def _temporary_file(self, suffix=".csv"):
        """Context manager for temporary file handling."""
        import tempfile

        tmp_file_path = tempfile.mktemp(suffix=suffix)
        try:
            yield tmp_file_path
        finally:
            if os.path.exists(tmp_file_path):
                os.unlink(tmp_file_path)

    def execute(self, context: Context) -> int:
        """Execute the extraction and staging process."""
        self.log.info(f"Resolved YAML path: {self.column_mapping_yaml_path}")
        column_mapping = load_table_config(
            column_mapping_yaml_path=self.column_mapping_yaml_path, table_name=self.table_name
        )

        self.log.info(f"Loaded column mapping: {column_mapping}")

        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        self.log.info(f"Reading {self.object_key} directly from bucket {self.bucket_name}")

        s3_object = s3_hook.get_key(self.object_key, self.bucket_name)
        csv_content = s3_object.get()["Body"].read()

        csv_string = csv_content.decode("utf-8")
        data = pd.read_csv(StringIO(csv_string), delimiter=self.csv_delimiter)

        if data.empty:
            self.log.warning("No data found in the CSV file")
            return 0

        self.log.info(f"Loaded {len(data)} records from CSV")

        data.columns = [col.lower() for col in data.columns]

        if self.col_mappings:
            data.rename(columns=self.col_mappings, inplace=True)

        # Add file_name column
        data[self.file_name_column.lower()] = self.object_key
        column_mapping[self.file_name_column.lower()] = {
            "csv_col": self.file_name_column.lower(),
            "df_dtype": "str",
        }

        # Add load_timestamp column (UTC now)
        load_timestamp_col = "load_timestamp"
        data[load_timestamp_col] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        column_mapping[load_timestamp_col] = {
            "csv_col": load_timestamp_col,
            "df_dtype": "str",
        }

        # Add label columns if add_label_columns is defined
        if self.add_label_columns:
            for key, value in self.add_label_columns.items():
                self.log.info(f"Adding label column {key} with value {value}")
                data[key] = value

        self.log.info(f"Applying column mapping: {column_mapping}")

        rename_map = {v["csv_col"]: k for k, v in column_mapping.items()}
        selected_csv_cols = list(rename_map.keys())

        missing_csv_cols = set(selected_csv_cols) - set(data.columns)
        if missing_csv_cols:
            raise ValueError(f"CSV columns not found in file: {missing_csv_cols}")

        # Keep only required columns and rename them according to the mapping
        data = data[selected_csv_cols].rename(columns=rename_map)

        self.log.info(f"Final columns for database: {data.columns.tolist()}")

        # Drop rows where ticker is null, empty, or whitespace
        if "ticker" in data.columns:
            before = len(data)
            data = data[~data["ticker"].isnull() & (data["ticker"].astype(str).str.strip() != "")]
            after = len(data)
            dropped = before - after
            if dropped > 0:
                self.log.warning(
                    f"Dropped {dropped} rows with null/empty/whitespace ticker before loading."
                )

        # Cast columns to specified dtypes
        for col, val in column_mapping.items():
            dtype = val["df_dtype"]
            if dtype == "int":
                data[col] = data[col].fillna(0).astype(int)
            elif dtype == "float":
                # Remove thousands separators (commas) before casting to float
                data[col] = data[col].astype(str).str.replace(",", "", regex=False)
                data[col] = data[col].replace("", None)
                data[col] = data[col].astype(float)
            elif dtype == "str":
                data[col] = data[col].astype(str)

        # Delete existing records with the same file_name to avoid duplicates
        file_name = data[self.file_name_column].iloc[0]

        delete_sql = f"""
        DELETE FROM {self.schema_name}.{self.table_name}
        WHERE {self.file_name_column} = %s;
        """

        self.log.info(f"Clearing existing records with {self.file_name_column}: {file_name}")
        postgres_hook.run(delete_sql, parameters=(file_name,))

        self.log.info("Preparing data for COPY operation")

        # Use temporary file only for the COPY operation
        with self._temporary_file() as tmp_file_path:
            # Write normalized data to temporary file for COPY
            data.to_csv(tmp_file_path, index=False, header=False, sep=self.csv_delimiter)

            # Perform bulk insert using PostgreSQL COPY command
            full_table_name = f"{self.schema_name}.{self.table_name}"
            self.log.info(f"Inserting {len(data)} records into {full_table_name} using COPY")

            try:
                # Use COPY command for efficient bulk insert with all columns
                table_columns = list(data.columns)
                copy_sql = f"""
                COPY {full_table_name} ({','.join(table_columns)})
                FROM STDIN WITH (FORMAT CSV, DELIMITER '{self.csv_delimiter}');
                """

                postgres_hook.copy_expert(copy_sql, tmp_file_path)

            except psycopg2.DatabaseError as e:
                self.log.error(
                    f"Database error during COPY: {e} | Table: {full_table_name} | "
                    f"File: {self.object_key}"
                )
                raise AirflowException(
                    f"Failed to load {self.object_key} into {full_table_name}: {e}"
                )
            except Exception as e:
                self.log.error(
                    f"Unexpected error during COPY: {e} | Table: {full_table_name} | "
                    f"File: {self.object_key}"
                )
                raise AirflowException(
                    f"Failed to load {self.object_key} into {full_table_name}: {e}"
                )

            self.log.info(f"Successfully loaded {len(data)} records using COPY")
            return len(data)
