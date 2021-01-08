from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    QUERY_FMT = """INSERT INTO {table_name} ({fields}) {insert_stmt}"""

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id: str = "default",
                 table_name: str = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self._redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        hook = PostgresHook(conn_name_attr=self._redshift_conn_id)
