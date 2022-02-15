from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
#     template_fields = ('s3_key',)
    copy_sql = """
        COPY {} from '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        COMPUPDATE OFF region 'us-west-2'
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        FORMAT AS JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_path='',
                 json_path='',
                 *args, **kwargs):
        
        """
        Constructor
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_path= s3_path
        self.json_path = json_path

    def execute(self, context):
        """
        Copy data to the table from S3
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Clearing data from destination Redshift table')
        redshift.run('DELETE FROM {}'.format(self.table))
        
        self.log.info('Copying data from S3 to Redshift')
        self.log.info('s3_path: {}'.format(self.s3_path))
        self.log.info('json_path:{}'.format(self.json_path))
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )
        redshift.run(formatted_sql)





