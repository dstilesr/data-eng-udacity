from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadDimensionOperator(BaseOperator):
    """
    Operator to load data into a dimension table from the staging tables.
    """

    ui_color = '#80BD9E'

    QUERY_FMT = """INSERT INTO {table_name} {select_stmt};"""

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "default",
                 table_name: str = "",
                 data_qry: str = "",
                 append_data: bool = False,
                 *args, **kwargs):
        """
        Initialize operator.
        :param redshift_conn_id: Redshift connection name.
        :param table_name: Name of the table where data will be loaded.
        :param data_qry: Query to get data from the staging tables.
        :param append_data: Append data to the table or replace existing data.
        :param args:
        :param kwargs:
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self._redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.data_qry = data_qry
        self.append_data = append_data

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
        Load the data into the dimension table.
        :param context:
        :return:
        """
        self.log.info("Loading data to dimension table.")
        self.has_qry()
        hook = PostgresHook(postgres_conn_id=self._redshift_conn_id)
        if self.append_data:
            self.log.info(f"Appending new data to table {self.table_name}")
        else:
            self.log.info(
                f"Truncating table {self.table_name} before loading."
            )
            hook.run(f"truncate table {self.table_name};", autocommit=True)

        stmt = self.QUERY_FMT.format(
            table_name=self.table_name,
            select_stmt=self.data_qry
        )
        hook.run(stmt, autocommit=True)
        self.log.info("Dimension data loaded.")
