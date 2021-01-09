from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadFactOperator(BaseOperator):
    """
    Operator to load data to the fact table from the staging tables.
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "redshift",
                 table_name: str = "songplays",
                 data_qry: str = "",
                 *args, **kwargs):
        """
        Initialize operator.
        :param redshift_conn_id: Redshift connection name.
        :param table_name: Name of the table where data will be loaded.
        :param data_qry: Query to get data from the staging tables.
        :param args:
        :param kwargs:
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self._redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.data_qry = data_qry

    def has_qry(self):
        """
        Checks that the table name and query are not empty. If they are not,
        raises a ValueError.
        :return:
        """
        null_vals = {None, ""}
        if self.table_name in null_vals:
            self.log.error("No output table specified!")
            raise ValueError("No output table specified!")

        if self.data_qry in null_vals:
            self.log.error("No insert query specified!")
            raise ValueError("No insert query specified!")

    def execute(self, context):
        """
        Load data into the fact table.
        :param context:
        :return:
        """
        self.log.info("Loading data to fact table.")
        self.has_qry()
        hook = PostgresHook(postgres_conn_id=self._redshift_conn_id)

        insert_stmt = f"INSERT INTO {self.table_name} {self.data_qry};"
        hook.run(insert_stmt, autocommit=True)
