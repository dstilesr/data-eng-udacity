from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook


class StageToRedshiftOperator(BaseOperator):
    """
    Operator to load the JSON data from S3 to redshift into staging tables.
    """
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 *args,
                 redshift_conn_id: str = "redshift",
                 **kwargs):
        """
        Initialize operator.
        :param args:
        :param redshift_conn_id: ID of the redshift connection.
        :param kwargs:
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self._redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')





