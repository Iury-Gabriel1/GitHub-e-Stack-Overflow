from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import sys
sys.path.insert(0, '/opt/airflow/scripts')

from transform_integrated import IntegratedTransformer
from load_postgres import PostgresLoader

default_args = {
    'owner': 'equipe_etl',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}

dag = DAG(
    'integrated_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL integrado GitHub + Stack Overflow',
    schedule_interval='@weekly',
    catchup=False,
    tags=['integrated', 'etl', 'analytics']
)

def integrate_data(**context):
    loader = PostgresLoader()
    
    repos_df = loader.execute_query("SELECT * FROM github_repositories")
    questions_df = loader.execute_query("SELECT * FROM stackoverflow_questions")
    users_df = loader.execute_query("SELECT * FROM stackoverflow_users")
    tags_df = loader.execute_query("SELECT * FROM stackoverflow_tags")
    
    transformer = IntegratedTransformer()
    
    integrated_df = transformer.correlate_github_stackoverflow(
        repos_df, questions_df, tags_df
    )
    
    insights = transformer.generate_insights(repos_df, questions_df, users_df)
    
    validation_repos = transformer.validate_data_quality(repos_df, 'github_repositories')
    validation_questions = transformer.validate_data_quality(questions_df, 'stackoverflow_questions')
    
    context['ti'].xcom_push(key='integrated_df', value=integrated_df.to_dict('records'))
    context['ti'].xcom_push(key='insights', value=insights)

def load_integrated_data(**context):
    ti = context['ti']
    loader = PostgresLoader()
    
    integrated_data = ti.xcom_pull(key='integrated_df', task_ids='integrate_data')
    integrated_df = pd.DataFrame(integrated_data)
    
    if not integrated_df.empty:
        loader.load_dataframe(integrated_df, 'integrated_analysis', if_exists='replace')

integrate_task = PythonOperator(
    task_id='integrate_data',
    python_callable=integrate_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_integrated',
    python_callable=load_integrated_data,
    dag=dag
)

integrate_task >> load_task