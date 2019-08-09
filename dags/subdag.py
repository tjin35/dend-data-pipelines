from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, 
                               LoadDimensionOperator, DataQualityOperator)

def load_dimension_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        table,
        create_sql_stmt,
        insert_sql_stmt,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
    
    create_table_table = PostgresOperator(
        task_id=f'Create_{table}_table',
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=create_sql_stmt
    )
    
    load_dimension_table = LoadDimensionOperator(
        task_id=f'Load_{table}_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table=table,
        sql=insert_sql_stmt
    )
    
    create_table_table >> load_dimension_table
    
    return dag