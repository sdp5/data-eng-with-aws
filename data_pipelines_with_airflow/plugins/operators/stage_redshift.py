from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    sql_copy_template = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        SESSION_TOKEN '{}'
        REGION '{}'
        TIMEFORMAT as 'epochmillisecs'
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 region='us-west-2',
                 file_format='JSON',
                 s3_json_paths_format='',
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # the connection id for Redshift configured in Airflow
        self.redshift_conn_id = redshift_conn_id

        # the connection id for IAM configured in Airflow
        self.aws_credentials_id = aws_credentials_id

        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.file_format = file_format
        self.s3_json_paths_format = s3_json_paths_format
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("\n## Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")
        self.log.info("\n\n")

        self.log.info("\n## Copying data from S3 to Redshift")

        if self.file_format.upper() == 'CSV':
            additional = f"IGNOREHEADER {self.ignore_headers} DELIMITER '{self.delimiter}"
        elif self.file_format.upper() == 'JSON':
            additional = "FORMAT AS JSON 'auto'"
            if self.s3_json_paths_format:
                rendered_json_paths_key = self.s3_json_paths_format.format(**context)
                s3_json_paths = f"s3://{self.s3_bucket}/{rendered_json_paths_key}"

                additional = f"FORMAT AS JSON '{s3_json_paths}'"

        else:
            self.log.info(f'*** ERROR: Invalid File Format: {self.file_format}. Try: CSV or JSON')
            return

        rendered_s3_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_s3_key}"

        formatted_sql = StageToRedshiftOperator.sql_copy_template.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            credentials.token,
            self.region,
            additional
        )

        # self.log.info(f'\n\n### formatted_sql = {formatted_sql}')
        self.log.info(f'\n\n### Copying data!')
        redshift.run(formatted_sql)
