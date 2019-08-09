from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        self.log.info(f"Load into {self.table} table")
        sql = """TRUNCATE TABLE public.{};
        INSERT INTO public.{} {}""".format(self.table, self.table, self.sql) 
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(sql)
    