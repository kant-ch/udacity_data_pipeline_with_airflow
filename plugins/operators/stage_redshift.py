from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 iam_role="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.iam_role = iam_role
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        
        self.log.info(f"Processing copy to table {self.table}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Delete data in the Redshift table")
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info("Start Loading data from s3 to redshift")
        if self.json_path:
            copy_sql = f"""
            copy {self.table} from 's3://{self.s3_bucket}/{self.s3_key}' 
            credentials 'aws_iam_role={self.iam_role}' 
            format as json 's3://{self.s3_bucket}/{self.json_path}'
            timeformat 'epochmillisecs'
            region 'us-east-1';
            """
        else:
            copy_sql = f"""
            copy {self.table} from 's3://{self.s3_bucket}/{self.s3_key}'
            credentials 'aws_iam_role={self.iam_role}' 
            format as json 'auto'
            region 'us-east-1';                  
            """
        
        self.log.info("Copy data from s3 to redshift finished")
        redshift.run(copy_sql)





