import pandas as pd
import logging
from datetime import datetime
from typing import List, Dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GitHubTransformer:
    
    @staticmethod
    def clean_repositories(repos: List[Dict]) -> pd.DataFrame:
        """Limpa e padroniza dados de repositórios."""
        logger.info("Transformando dados de repositórios...")
        df = pd.DataFrame(repos)
        
        if df.empty:
            return df
        
        df['description'] = df['description'].fillna('').str.strip()
        df['language'] = df['language'].fillna('Unknown')
        df['license'] = df['license'].fillna('No License')
        
        date_columns = ['created_at', 'updated_at', 'pushed_at']
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
        
        df['repo_age_days'] = (pd.Timestamp.now(tz='UTC') - df['created_at']).dt.days
        df['days_since_last_push'] = (pd.Timestamp.now(tz='UTC') - df['pushed_at']).dt.days
        df['stars_per_day'] = df['stars'] / df['repo_age_days'].replace(0, 1)
        df['forks_per_star'] = df['forks'] / df['stars'].replace(0, 1)
        df['issues_per_star'] = df['open_issues'] / df['stars'].replace(0, 1)
        
        df['activity_score'] = (
            df['stars'] * 0.4 + 
            df['forks'] * 0.3 + 
            df['watchers'] * 0.2 + 
            (1 / (df['days_since_last_push'] + 1)) * 0.1
        )
        
        df['popularity_tier'] = pd.cut(
            df['stars'],
            bins=[0, 1000, 10000, 50000, float('inf')],
            labels=['Baixa', 'Média', 'Alta', 'Muito Alta']
        )
        
        logger.info(f"Transformação concluída: {len(df)} repositórios.")
        return df
    
    @staticmethod
    def clean_commits(commits: List[Dict]) -> pd.DataFrame:
        """Limpa e padroniza dados de commits."""
        logger.info("Transformando dados de commits...")
        df = pd.DataFrame(commits)
        
        if df.empty:
            return df
        
        df['date'] = pd.to_datetime(df['date'], errors='coerce', utc=True)
        df['message'] = df['message'].fillna('').str.strip()
        df['author'] = df['author'].fillna('Unknown')
        
        df['commit_date'] = df['date'].dt.date
        df['commit_hour'] = df['date'].dt.hour
        df['commit_day_of_week'] = df['date'].dt.day_name()
        df['commit_month'] = df['date'].dt.to_period('M').astype(str)
        
        df['message_length'] = df['message'].str.len()
        df['has_issue_reference'] = df['message'].str.contains(r'#\d+', regex=True, na=False)
        
        logger.info(f"Transformação concluída: {len(df)} commits.")
        return df
    
    @staticmethod
    def clean_issues(issues: List[Dict]) -> pd.DataFrame:
        """Limpa e padroniza dados de issues."""
        logger.info("Transformando dados de issues...")
        df = pd.DataFrame(issues)
        
        if df.empty:
            return df
        
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)
        df['closed_at'] = pd.to_datetime(df['closed_at'], errors='coerce', utc=True)
        df['title'] = df['title'].fillna('').str.strip()
        
        df['resolution_time_days'] = (df['closed_at'] - df['created_at']).dt.days
        df['is_closed'] = df['state'] == 'closed'
        df['created_date'] = df['created_at'].dt.date
        df['created_month'] = df['created_at'].dt.to_period('M').astype(str)
        
        logger.info(f"Transformação concluída: {len(df)} issues.")
        return df
    
    @staticmethod
    def clean_pull_requests(prs: List[Dict]) -> pd.DataFrame:
        """Limpa e padroniza dados de pull requests."""
        logger.info("Transformando dados de pull requests...")
        df = pd.DataFrame(prs)
        
        if df.empty:
            return df
        
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)
        df['closed_at'] = pd.to_datetime(df['closed_at'], errors='coerce', utc=True)
        df['merged_at'] = pd.to_datetime(df['merged_at'], errors='coerce', utc=True)
        df['title'] = df['title'].fillna('').str.strip()
        
        df['time_to_merge_days'] = (df['merged_at'] - df['created_at']).dt.days
        df['is_merged'] = df['merged_at'].notna()
        df['created_date'] = df['created_at'].dt.date
        df['created_month'] = df['created_at'].dt.to_period('M').astype(str)
        
        logger.info(f"Transformação concluída: {len(df)} pull requests.")
        return df
    
    @staticmethod
    def aggregate_activity(commits_df: pd.DataFrame, issues_df: pd.DataFrame, 
                          prs_df: pd.DataFrame) -> pd.DataFrame:
        """Agrega métricas de atividade por repositório."""
        logger.info("Agregando métricas de atividade...")
        
        commits_agg = commits_df.groupby('repo_full_name').agg(
            total_commits=('sha', 'count'),
            avg_additions=('additions', 'mean'),
            avg_deletions=('deletions', 'mean'),
            first_commit=('date', 'min'),
            last_commit=('date', 'max')
        ).reset_index() if not commits_df.empty else pd.DataFrame()
        
        issues_agg = issues_df.groupby('repo_full_name').agg(
            total_issues=('issue_id', 'count'),
            open_issues=('is_closed', lambda x: (~x).sum()),
            closed_issues=('is_closed', 'sum'),
            avg_resolution_time=('resolution_time_days', 'mean')
        ).reset_index() if not issues_df.empty else pd.DataFrame()
        
        prs_agg = prs_df.groupby('repo_full_name').agg(
            total_prs=('pr_id', 'count'),
            merged_prs=('is_merged', 'sum'),
            avg_merge_time=('time_to_merge_days', 'mean')
        ).reset_index() if not prs_df.empty else pd.DataFrame()
        
        result = commits_agg
        if not issues_agg.empty:
            result = result.merge(issues_agg, on='repo_full_name', how='outer') if not result.empty else issues_agg
        if not prs_agg.empty:
            result = result.merge(prs_agg, on='repo_full_name', how='outer') if not result.empty else prs_agg
        
        if not result.empty:
            result = result.fillna(0)
        
        logger.info(f"Agregação concluída: {len(result)} repositórios.")
        return result