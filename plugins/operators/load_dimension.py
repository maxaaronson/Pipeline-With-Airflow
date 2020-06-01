from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql='',
                 table_name='',
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id
        self.truncate_table = truncate_table
        self.table_name = table_name

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # drop all records if the parameter is set to True
        if self.truncate_table:
            self.log.info(f'Dropping all records from {self.table_name}...')
            redshift.run(f'''TRUNCATE TABLE {self.table_name}''')

        self.log.info(f'Inserting data into {self.table_name}')

        redshift.run(self.sql)
