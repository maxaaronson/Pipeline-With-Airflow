from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table_list=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_list = table_list

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.table_list:
            self.log.info(f'Retrieving record count from {table}...')
            record_count = redshift.get_records(
                f'SELECT COUNT(*) FROM {table}')

            # check for a valid response from the cluster
            if len(record_count) < 1 or len(record_count[0]) < 1:
                raise ValueError(
                    f'ERROR: Record count for {table} returned no response...')
            if record_count[0][0] < 1:
                raise ValueError(f'ERROR: Record count for {table} is 0...')
            self.log.info(f'{table} contains {record_count[0][0]}records.')
            self.log.info('Data check passed.')
