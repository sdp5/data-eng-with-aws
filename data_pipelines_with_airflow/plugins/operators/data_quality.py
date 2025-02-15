from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 data_quality_count_checks=[],
                 data_quality_null_checks=[],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.data_quality_count_checks = data_quality_count_checks
        self.data_quality_null_checks = data_quality_null_checks

    # Review Comments:
    # The DQ operator is done right, but there is still a different way
    # to make it more flexible for future test cases
    #
    # Check sample code -
    # for i, dq_check in enumerate(self.dq_checks):
    #     records = redshift_hook.get_records(dq_check['test_sql'])
    #     if not dq_check['expected_results'] == records[0][0]:
    #         raise ValueError(
    #             f"Data quality check #{i} failed. {put explanation of the error here}"
    #         )
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # records = should be a list with a single tuple
        # containing a single element: the counting value
        self.log.info('\n\n############ DATA QUALITY COUNT CHECKS ############')
        for query in self.data_quality_count_checks:
            records = redshift.get_records(query)

            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error(f"Data quality check failed. {query} returned no results")
                raise ValueError(f"Data quality check failed. {query} returned no results")

            self.log.info(f"Data quality on query {query} check passed with {records[0][0]} records\n\n")

        # records = should be a list with a single tuple
        # containing a single element: the counting value equals to 0: [(0,)]
        # If it was not, it means that there are records with NULL values
        self.log.info('\n\n############ DATA QUALITY NULL CHECKS ############')

        for query in self.data_quality_null_checks:
            records = redshift.get_records(query)

            if len(records) > 1 or len(records[0]) > 1 or records[0][0] > 0:
                self.log.info(records)
                self.log.error(f"Data quality check failed. {query} returned results")
                raise ValueError(f"Data quality check failed. {query} returned results")

            self.log.info(f"Data quality on query {query} check for NULL values passed with non-NULL values\n\n")
