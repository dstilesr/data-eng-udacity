from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataQualityOperator(BaseOperator):
    """
    Basic quality check to verify that the given tables are not empty.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "redshift",
                 check_tables: list = ("songplays",),
                 *args, **kwargs):
        """
        Initialize operator.
        :param redshift_conn_id: Name of redshift connection.
        :param check_tables: List of tables to check.
        :param args:
        :param kwargs:
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.check_tables = check_tables
        self.redshift_conn_id = redshift_conn_id

    def get_redshift_hook(self) -> PostgresHook:
        """
        Makes a hook to the redshift DB.
        :return:
        """
        return PostgresHook(postgres_conn_id=self.redshift_conn_id)

    def has_rows(self, table: str):
        """
        Checks whether the given table has rows.
        :param table: Name of table.
        :return:
        """
        self.log.info(f"Checking table {table}")

        hook = self.get_redshift_hook()
        records = hook.get_records(f"select count(*) from {table};")
        if len(records) == 0 or len(records[0]) == 0:
            self.log.error(f"Check for table {table} gave no results!")
            raise ValueError(f"Table {table} failed check.")

        if records[0][0] == 0:
            self.log.error(f"Table {table} has no records!")
            raise ValueError(f"Table {table} failed check.")

        self.log.info(f"Table {table} passed the check.")

    def execute(self, context):
        """
        Runs the check on the given tables.
        :param context:
        :return:
        """
        if len(self.check_tables) < 1:
            self.log.error("No tables to check!")
            raise ValueError("No tables to check!")

        self.log.info(
            f"Running checks on tables: {', '.join(self.check_tables)}"
        )
        for tab in self.check_tables:
            self.has_rows(tab)
