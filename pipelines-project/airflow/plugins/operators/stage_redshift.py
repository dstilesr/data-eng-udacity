from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Operator to load the JSON data from S3 to redshift into staging tables.
    """
    ui_color = '#358140'

    COPY_STMT = """
    copy {table} from '{src_path}'
    credentials 'aws_access_key_id={access_key};aws_secret_access_key={secret}'
    json '{json_paths}'
    region 'us-west-2';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "redshift",
                 s3_bucket: str = "udacity-dend",
                 s3_path: str = "song_data",
                 table_name: str = "staging_events",
                 json_paths: str = "auto",
                 aws_conn_id: str = "",
                 aws_region: str = "us-west-2",
                 *args,
                 **kwargs):
        """
        Initialize operator.
        :param args:
        :param redshift_conn_id: ID of the redshift connection.
        :param s3_bucket: Bucket where source JSON files are stored.
        :param s3_path: Path to source JSON files in bucket.
        :param table_name: Name of table where data will be loaded.
        :param json_paths: Option for Redshift copy statement.
        :param aws_conn_id: Name of AWS connection to read from S3.
        :param aws_region: AWS Region name.
        :param kwargs:
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self._redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.table_name = table_name
        self.json_paths = json_paths
        self.aws_conn_id = aws_conn_id
        self.aws_region = aws_region

    def execute(self, context):
        """
        Executes the redshift COPY operation from the given source.
        :param context: Run context.
        :return:
        """
        if self.aws_conn_id is None or self.aws_conn_id == "":
            self.log.error("No AWS connection specified!")
            raise ValueError("No AWS connection specified!")

        aws_cred = AwsHook(
            aws_conn_id=self.aws_conn_id,
        ).get_credentials()

        hook = PostgresHook(postgres_conn_id=self._redshift_conn_id)

        # Truncate staging table to avoid adding the same data multiple times!
        self.log.info(f"Truncating staging table {self.table_name}.")
        hook.run(f"truncate table {self.table_name};")

        src_path = f"s3://{self.s3_bucket}/{self.s3_path}"
        self.log.info(f"Copying files from source: {src_path}")

        sql_str = self.COPY_STMT.format(
            table=self.table_name,
            src_path=src_path,
            access_key=aws_cred.access_key,
            secret=aws_cred.secret_key,
            json_paths=self.json_paths
        )
        hook.run(sql_str, autocommit=True)
        self.log.info(f"Data staged to {self.table_name}")





