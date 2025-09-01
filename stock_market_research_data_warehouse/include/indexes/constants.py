from datetime import datetime

from include.config import ENV, OWNER

BUCKET_INDEXES_ISHARES_HOLDINGS = f"smr-dw-indexes-ishares-holdings-data-{ENV.lower()}"

COMMON_ARGS = {
    "owner": OWNER,
    "start_date": datetime(2025, 7, 28, 1),
    "retries": 0,
    "depends_on_past": False,
    "schedule": None,
}


COL_MAPPINGS = {
    "shares": "quantity",
}
