from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        self.log.info("Load into fact table")
        sql = """TRUNCATE TABLE public.songplays;
        INSERT INTO public.songplays {}""".format(self.sql) 
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(sql)