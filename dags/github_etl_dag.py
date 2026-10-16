from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
sys.path.insert(0, '/opt/airflow/scripts')

from extract_github import GitHubExtractor
from transform_github import GitHubTransformer
from load_postgres import PostgresLoader

default_args = {
    'owner': 'equipe_etl',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}

dag = DAG(
    'github_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL para dados do GitHub',
    schedule_interval='@daily',
    catchup=False,
    tags=['github', 'etl', 'api']
)

def extract_github_data(**context):
    extractor = GitHubExtractor()
    
    repos = extractor.extract_popular_repositories(min_stars=1000, max_results=200)
    
    all_commits = []
    all_issues = []
    all_prs = []
    
    for repo in repos[:20]:
        repo_name = repo['full_name']
        try:
            commits = extractor.extract_commits(repo_name, max_commits=50)
            all_commits.extend(commits)
            
            issues = extractor.extract_issues(repo_name, max_issues=50)
            all_issues.extend(issues)
            
            prs = extractor.extract_pull_requests(repo_name, max_prs=50)
            all_prs.extend(prs)
        except Exception as e:
            print(f"Erro ao extrair {repo_name}: {e}")
    
    context['ti'].xcom_push(key='repos', value=repos)
    context['ti'].xcom_push(key='commits', value=all_commits)
    context['ti'].xcom_push(key='issues', value=all_issues)
    context['ti'].xcom_push(key='prs', value=all_prs)

def transform_github_data(**context):
    ti = context['ti']
    repos = ti.xcom_pull(key='repos', task_ids='extract_github')
    commits = ti.xcom_pull(key='commits', task_ids='extract_github')
    issues = ti.xcom_pull(key='issues', task_ids='extract_github')
    prs = ti.xcom_pull(key='prs', task_ids='extract_github')
    
    transformer = GitHubTransformer()
    
    repos_df = transformer.clean_repositories(repos)
    commits_df = transformer.clean_commits(commits)
    issues_df = transformer.clean_issues(issues)
    prs_df = transformer.clean_pull_requests(prs)
    
    activity_df = transformer.aggregate_activity(commits_df, issues_df, prs_df)
    
    ti.xcom_push(key='repos_df', value=repos_df.to_dict('records'))
    ti.xcom_push(key='commits_df', value=commits_df.to_dict('records'))
    ti.xcom_push(key='issues_df', value=issues_df.to_dict('records'))
    ti.xcom_push(key='prs_df', value=prs_df.to_dict('records'))
    ti.xcom_push(key='activity_df', value=activity_df.to_dict('records'))

def load_github_data(**context):
    ti = context['ti']
    loader = PostgresLoader()
    
    repos_df = pd.DataFrame(ti.xcom_pull(key='repos_df', task_ids='transform_github'))
    commits_df = pd.DataFrame(ti.xcom_pull(key='commits_df', task_ids='transform_github'))
    issues_df = pd.DataFrame(ti.xcom_pull(key='issues_df', task_ids='transform_github'))
    prs_df = pd.DataFrame(ti.xcom_pull(key='prs_df', task_ids='transform_github'))
    
    loader.upsert_dataframe(repos_df, 'github_repositories', ['id'])
    loader.upsert_dataframe(commits_df, 'github_commits', ['sha'])
    loader.upsert_dataframe(issues_df, 'github_issues', ['issue_id'])
    loader.upsert_dataframe(prs_df, 'github_pull_requests', ['pr_id'])

extract_task = PythonOperator(
    task_id='extract_github',
    python_callable=extract_github_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_github',
    python_callable=transform_github_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_github',
    python_callable=load_github_data,
    dag=dag
)

extract_task >> transform_task >> load_task