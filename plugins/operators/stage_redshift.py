from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 S3_path='',
                 redshift_conn_id='',
                 file_type='',
                 staging_table='',
                 ARN='',
                 json_path='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.staging_table = staging_table
        self.S3_path = S3_path
        self.redshift_conn_id = redshift_conn_id
        self.file_type = file_type
        self.ARN = ARN
        self.json_path = json_path

    def execute(self, context):

        self.log.info('establishing connection to redshift....')

        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info(f'Copying {self.staging_table} data from S3 to Redshift')

        sql = f'''
            copy {self.staging_table}
            from '{self.S3_path}'
            IAM_ROLE '{self.ARN}'
            JSON '{self.json_path}'
            region 'us-west-2'
        '''
        redshift.run(sql)
