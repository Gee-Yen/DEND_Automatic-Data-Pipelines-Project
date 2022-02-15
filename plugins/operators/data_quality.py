from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tables=[],
                 dq_checks=[],
                 *args, **kwargs):
        """
        Constructor 
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.dq_checks = dq_checks

    def execute(self, context):
        """
        Performs data quality check on table
        """
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            
            records = redshift_hook.get_records('SELECT COUNT(*) FROM {}'.format(table))
            self.log.info('====> len(records): {}'.format(len(records)))
            self.log.info('====> len(records[0]): {}'.format(len(records[0])))

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError('Data quality check failed. {} returned no results'.format(table))

            num_records = records[0][0]
            self.log.info('====> records[0][0]: {}'.format(num_records))
            if num_records < 1:
                raise ValueError('Data quality check failed. {} contained 0 rows'.format(table))

            self.log.info('Data quality on table {} check passed with {} records'.format(table, num_records))
        
                
        for check in self.dq_checks:    
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            
            records = redshift_hook.get_records(sql)[0]
            
            error_count=0

            if exp_result != records[0]:
                error_count += 1
                failed_test.append(sql)
            
            if error_count > 0:
                self.log.info('***** Data Quality check failed *****')
                self.log.info(failed_test)
                raise ValueError('Data quality check failed')
                
            self.log.info('***** SQL tests passed *****')                           
            