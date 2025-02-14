from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql_query='',
                 mode='delete-load',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.mode = mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.mode == 'delete-load':
            self.log.info("\n\n### Deleting data from Dimensional Table")
            redshift.run(f"TRUNCATE {self.table}")

        self.log.info(f'\n\n### Loading Dimensional Table')
        redshift.run(f"INSERT INTO {self.table} {self.sql_query}")
