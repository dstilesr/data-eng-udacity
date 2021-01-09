from datetime import datetime, timedelta

APPEND_MODE: bool = False

REDSHIFT_CONN_ID: str = "redshift"

AWS_CONN_ID: str = "aws-conn"

S3_BUCKET_NAME: str = "udacity-dend"

DEFAULT_ARGS = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
    "catchup": False,
    "depends_on_past": False
}
