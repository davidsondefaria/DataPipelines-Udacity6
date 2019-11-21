from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Testing DataQualityOperator')

        # Reading aws credentials to connect to redshift database
#         aws_hook = AwsHook(self.aws_credentials_id)
#         credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("DataQualityOperator checking data quality")
        for table in self.tables:
            self.log.info(f"DataQualityOperator testing {table} table")
            check = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(check) < 1 or len(check[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            if check[0][0] < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.info(f"Data quality on table {table} check passed with {check[0][0]} records")
            
            
            
            
            
            
            
            
            
            
            
            
            
