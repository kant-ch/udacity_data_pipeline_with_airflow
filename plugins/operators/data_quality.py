from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 check=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id,
        self.check=check

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for elem in self.check:
            records = redshift_hook.get_records(elem['check_sql'])
            num_records = records[0][0]

            self.log.info(f"Test case: {elem['test_case']}")
            if elem['compare_type'] == '=' and num_records != elem['expected_result']:
                raise ValueError(f"Data quality check failed for case {elem['test_case']}")
            elif elem['compare_type'] == '>' and num_records <= elem['expected_result']:
                raise ValueError(f"Data quality check failed for case {elem['test_case']}")
            elif elem['compare_type'] == '<' and num_records >= elem['expected_result']:
                raise ValueError(f"Data quality check failed for case {elem['test_case']}")
            
            self.log.info(f"Data quality check status: Pass")