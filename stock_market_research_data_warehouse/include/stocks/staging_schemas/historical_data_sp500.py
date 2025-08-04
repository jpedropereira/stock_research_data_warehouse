from include.config import STAGING_SCHEMA

historical_data_sp500_schema = f"""
        CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.historical_data_sp500 (
            date DATE NOT NULL,
            ticker VARCHAR(10) NOT NULL,
            open_price DECIMAL(12,4),
            high_price DECIMAL(12,4),
            low_price DECIMAL(12,4),
            close_price DECIMAL(12,4),
            volume BIGINT,
            extraction_datetime TIMESTAMP NOT NULL,
            file_name VARCHAR(255),
            PRIMARY KEY (date, ticker)
        );
        """

historical_data_sp500_column_mapping = {
    "date": {"csv_col": "date", "df_dtype": "str"},
    "ticker": {"csv_col": "ticker", "df_dtype": "str"},
    "open_price": {"csv_col": "open", "df_dtype": "float"},
    "high_price": {"csv_col": "high", "df_dtype": "float"},
    "low_price": {"csv_col": "low", "df_dtype": "float"},
    "close_price": {"csv_col": "close", "df_dtype": "float"},
    "volume": {"csv_col": "volume", "df_dtype": "int"},
    "extraction_datetime": {"csv_col": "extraction_datetime", "df_dtype": "str"},
}
