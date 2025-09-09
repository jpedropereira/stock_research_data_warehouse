from datetime import datetime

from include.config import OWNER

COMMON_ARGS = {
    "owner": OWNER,
    "start_date": datetime(2025, 7, 28, 1),
    "retries": 0,
    "depends_on_past": False,
    "schedule": None,
}

MINIO_CONN_ID = "minio_conn"
POSTGRES_CONN_ID = "datawarehouse_conn"
