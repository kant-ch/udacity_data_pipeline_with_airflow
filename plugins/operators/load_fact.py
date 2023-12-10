from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_sql="",
                 delete_load=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id,
        self.table=table,
        self.insert_sql=insert_sql,
        self.delete_load = delete_load
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info('Start loading fact table to redshift')
        if self.delete_load:
            self.log.info(f'Truncate table {self.table}')
            truncate_table_script = f"""delete from {self.table}"""
            redshift.run(truncate_table_script)

        self.log.info('Transform data to Redshift finished')
        redshift.run(self.insert_sql)