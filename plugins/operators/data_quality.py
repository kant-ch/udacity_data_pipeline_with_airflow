from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_list=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id,
        self.table_list=table_list

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for table_ in self.table_list:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table_}")

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table_} returned no results")
            num_records = records[0][0]

            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table_} contained 0 rows")
            
            self.log.info(f"Data quality on table {table_} check passed with {records[0][0]} records")