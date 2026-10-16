from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import sys
sys.path.insert(0, '/opt/airflow/scripts')

from extract_stackoverflow import StackOverflowExtractor
from transform_stackoverflow import StackOverflowTransformer
from load_postgres import PostgresLoader

default_args = {
    'owner': 'equipe_etl',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}

dag = DAG(
    'stackoverflow_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL para dados do Stack Overflow',
    schedule_interval='@daily',
    catchup=False,
    tags=['stackoverflow', 'etl', 'api']
)

def extract_stackoverflow_data(**context):
    extractor = StackOverflowExtractor()
    
    questions = extractor.extract_questions(max_questions=500)
    question_ids = [q['question_id'] for q in questions[:100]]
    answers = extractor.extract_answers(question_ids=question_ids, max_answers=1000)
    users = extractor.extract_users(max_users=200)
    tags = extractor.extract_tags(max_tags=100)
    
    context['ti'].xcom_push(key='questions', value=questions)
    context['ti'].xcom_push(key='answers', value=answers)
    context['ti'].xcom_push(key='users', value=users)
    context['ti'].xcom_push(key='tags', value=tags)

def transform_stackoverflow_data(**context):
    ti = context['ti']
    questions = ti.xcom_pull(key='questions', task_ids='extract_stackoverflow')
    answers = ti.xcom_pull(key='answers', task_ids='extract_stackoverflow')
    users = ti.xcom_pull(key='users', task_ids='extract_stackoverflow')
    tags = ti.xcom_pull(key='tags', task_ids='extract_stackoverflow')
    
    transformer = StackOverflowTransformer()
    
    questions_df = transformer.clean_questions(questions)
    answers_df = transformer.clean_answers(answers)
    users_df = transformer.clean_users(users)
    tags_df = transformer.clean_tags(tags)
    
    response_df = transformer.calculate_response_time(questions_df, answers_df)
    trends_df = transformer.aggregate_topic_trends(questions_df)
    
    ti.xcom_push(key='questions_df', value=questions_df.to_dict('records'))
    ti.xcom_push(key='answers_df', value=answers_df.to_dict('records'))
    ti.xcom_push(key='users_df', value=users_df.to_dict('records'))
    ti.xcom_push(key='tags_df', value=tags_df.to_dict('records'))

def load_stackoverflow_data(**context):
    ti = context['ti']
    loader = PostgresLoader()
    
    questions_df = pd.DataFrame(ti.xcom_pull(key='questions_df', task_ids='transform_stackoverflow'))
    answers_df = pd.DataFrame(ti.xcom_pull(key='answers_df', task_ids='transform_stackoverflow'))
    users_df = pd.DataFrame(ti.xcom_pull(key='users_df', task_ids='transform_stackoverflow'))
    tags_df = pd.DataFrame(ti.xcom_pull(key='tags_df', task_ids='transform_stackoverflow'))
    
    loader.upsert_dataframe(questions_df, 'stackoverflow_questions', ['question_id'])
    loader.upsert_dataframe(answers_df, 'stackoverflow_answers', ['answer_id'])
    loader.upsert_dataframe(users_df, 'stackoverflow_users', ['user_id'])
    loader.upsert_dataframe(tags_df, 'stackoverflow_tags', ['tag_name'])

extract_task = PythonOperator(
    task_id='extract_stackoverflow',
    python_callable=extract_stackoverflow_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_stackoverflow',
    python_callable=transform_stackoverflow_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_stackoverflow',
    python_callable=load_stackoverflow_data,
    dag=dag
)

extract_task >> transform_task >> load_task