"""Custom Airflow dag plugins for the stock market research data warehouse.
This file simplifies the import structure for staging operations.
You can use:
from plugins.dags import BaseDAGFactory

instead of:
from plugins.dags.dag_factory import BaseDAGFactory
"""

from plugins.dags.dag_factory import BaseDAGFactory

__all__ = ["BaseDAGFactory"]
