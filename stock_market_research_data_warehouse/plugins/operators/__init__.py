"""Custom Airflow operators for the stock market research data warehouse.
This file simplifies the import structure for staging operations.
You can use:
from plugins.operators import ExtractToStagingOperator

instead of:
from plugins.operators.extract_to_staging_operator import ExtractToStagingOperator
"""

from plugins.operators.enforce_latest_file_operator import EnforceLatestFileOperator
from plugins.operators.extract_to_staging_operator import ExtractToStagingOperator

__all__ = ["ExtractToStagingOperator", "EnforceLatestFileOperator"]
