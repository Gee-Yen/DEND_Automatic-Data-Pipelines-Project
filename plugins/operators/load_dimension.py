from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql='',
                 delete_records='',
                 *args, **kwargs):

        """
        Constructor
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.delete_records=delete_records

    def execute(self, context):
        """
        Insert records into the table
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.delete_records == True:
            self.log.info('Clearing data from destination Redshift table')
            redshift.run('DELETE FROM {}'.format(self.table))
        
        self.log.info('Inserting records to {} table'.format(self.table))
        redshift.run('INSERT INTO {} {}'.format(self.table, self.sql))
        
