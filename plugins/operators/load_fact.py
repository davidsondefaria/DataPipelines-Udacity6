from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
    insert_sql = """
        INSERT INTO {}
        {};
        COMMIT;
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id,
        self.table=table
        self.sql_query=sql_query

    def execute(self, context):
        self.log.info('Testing LoadFactOperator')
        
        # Reading aws credentials to connect to redshift database
#         aws_hook = AwsHook(self.aws_credentials_id)
#         credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
#         # Cleaning redshift tables
#         self.log.info('LoadFactOperator deleting tables')
#         redshift.run("DELETE FROM {}".format(self.table))
        
        # Inserting data into fact table
        self.log.info(f'LoadFactOperator inserting data into fact table: {self.table}')
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.sql_query
        )
        redshift.run(formatted_sql)
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        