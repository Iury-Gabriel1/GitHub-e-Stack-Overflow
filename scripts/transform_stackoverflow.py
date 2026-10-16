import pandas as pd
import logging
from datetime import datetime
from typing import List, Dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StackOverflowTransformer:
    
    @staticmethod
    def clean_questions(questions: List[Dict]) -> pd.DataFrame:
        """Limpa e padroniza dados de perguntas."""
        logger.info("Transformando dados de perguntas...")
        df = pd.DataFrame(questions)
        
        if df.empty:
            return df
        
        df['title'] = df['title'].fillna('').str.strip()
        df['body'] = df['body'].fillna('')
        
        df['creation_date'] = pd.to_datetime(df['creation_date'], unit='s', errors='coerce', utc=True)
        df['last_activity_date'] = pd.to_datetime(df['last_activity_date'], unit='s', errors='coerce', utc=True)
        
        df['creation_year'] = df['creation_date'].dt.year
        df['creation_month'] = df['creation_date'].dt.to_period('M').astype(str)
        df['creation_day_of_week'] = df['creation_date'].dt.day_name()
        
        df['age_days'] = (pd.Timestamp.now(tz='UTC') - df['creation_date']).dt.days
        df['views_per_day'] = df['view_count'] / df['age_days'].replace(0, 1)
        df['score_per_view'] = df['score'] / df['view_count'].replace(0, 1)
        df['answer_rate'] = df['answer_count'] / df['view_count'].replace(0, 1)
        
        df['difficulty_level'] = pd.cut(
            df['score'],
            bins=[-float('inf'), 0, 10, 50, float('inf')],
            labels=['Baixa', 'Média', 'Alta', 'Muito Alta']
        )
        
        df['tag_list'] = df['tags'].str.split(',')
        df['primary_tag'] = df['tag_list'].apply(lambda x: x[0] if x and len(x) > 0 else 'Unknown')
        df['tag_count'] = df['tag_list'].apply(len)
        
        logger.info(f"Transformação concluída: {len(df)} perguntas.")
        return df
    
    @staticmethod
    def clean_answers(answers: List[Dict]) -> pd.DataFrame:
        """Limpa e padroniza dados de respostas."""
        logger.info("Transformando dados de respostas...")
        df = pd.DataFrame(answers)
        
        if df.empty:
            return df
        
        df['body'] = df['body'].fillna('')
        df['creation_date'] = pd.to_datetime(df['creation_date'], unit='s', errors='coerce', utc=True)
        df['last_activity_date'] = pd.to_datetime(df['last_activity_date'], unit='s', errors='coerce', utc=True)
        
        df['creation_year'] = df['creation_date'].dt.year
        df['creation_month'] = df['creation_date'].dt.to_period('M').astype(str)
        
        df['body_length'] = df['body'].str.len()
        df['is_high_score'] = df['score'] > 10
        
        logger.info(f"Transformação concluída: {len(df)} respostas.")
        return df
    
    @staticmethod
    def clean_users(users: List[Dict]) -> pd.DataFrame:
        """Limpa e padroniza dados de usuários."""
        logger.info("Transformando dados de usuários...")
        df = pd.DataFrame(users)
        
        if df.empty:
            return df
        
        df['display_name'] = df['display_name'].fillna('Unknown')
        df['location'] = df['location'].fillna('Unknown')
        
        df['creation_date'] = pd.to_datetime(df['creation_date'], unit='s', errors='coerce', utc=True)
        df['last_access_date'] = pd.to_datetime(df['last_access_date'], unit='s', errors='coerce', utc=True)
        
        df['account_age_days'] = (pd.Timestamp.now(tz='UTC') - df['creation_date']).dt.days
        df['reputation_per_day'] = df['reputation'] / df['account_age_days'].replace(0, 1)
        df['total_badges'] = df['badge_gold'] + df['badge_silver'] + df['badge_bronze']
        df['answer_question_ratio'] = df['answer_count'] / df['question_count'].replace(0, 1)
        
        df['reputation_tier'] = pd.cut(
            df['reputation'],
            bins=[0, 1000, 10000, 100000, float('inf')],
            labels=['Iniciante', 'Intermediário', 'Avançado', 'Expert']
        )
        
        logger.info(f"Transformação concluída: {len(df)} usuários.")
        return df
    
    @staticmethod
    def clean_tags(tags: List[Dict]) -> pd.DataFrame:
        """Limpa e padroniza dados de tags."""
        logger.info("Transformando dados de tags...")
        df = pd.DataFrame(tags)
        
        if df.empty:
            return df
        
        df['tag_name'] = df['tag_name'].fillna('Unknown')
        df['tag_popularity'] = pd.cut(
            df['count'],
            bins=[0, 1000, 10000, 100000, float('inf')],
            labels=['Baixa', 'Média', 'Alta', 'Muito Alta']
        )
        
        logger.info(f"Transformação concluída: {len(df)} tags.")
        return df
    
    @staticmethod
    def calculate_response_time(questions_df: pd.DataFrame, 
                                answers_df: pd.DataFrame) -> pd.DataFrame:
        """Calcula tempo médio de resposta por pergunta."""
        logger.info("Calculando tempo de resposta...")
        
        if questions_df.empty or answers_df.empty:
            return pd.DataFrame()
        
        first_answers = answers_df.groupby('question_id').agg(
            first_answer_date=('creation_date', 'min'),
            answer_count=('answer_id', 'count'),
            avg_answer_score=('score', 'mean'),
            accepted_answer=('is_accepted', 'max')
        ).reset_index()
        
        merged = questions_df.merge(first_answers, on='question_id', how='left')
        merged['response_time_hours'] = (
            merged['first_answer_date'] - merged['creation_date']
        ).dt.total_seconds() / 3600
        
        merged['response_time_category'] = pd.cut(
            merged['response_time_hours'],
            bins=[-1, 1, 24, 168, float('inf')],
            labels=['Muito Rápido (<1h)', 'Rápido (1-24h)', 
                   'Moderado (1-7 dias)', 'Lento (>7 dias)']
        )
        
        logger.info("Cálculo de tempo de resposta concluído.")
        return merged
    
    @staticmethod
    def aggregate_topic_trends(questions_df: pd.DataFrame) -> pd.DataFrame:
        """Agrega tendências por tópico (tag)."""
        logger.info("Agregando tendências por tópico...")
        
        if questions_df.empty:
            return pd.DataFrame()
        
        exploded = questions_df.explode('tag_list')
        exploded = exploded[exploded['tag_list'].notna()]
        
        trends = exploded.groupby(['tag_list', 'creation_month']).agg(
            question_count=('question_id', 'count'),
            avg_score=('score', 'mean'),
            avg_views=('view_count', 'mean'),
            avg_answers=('answer_count', 'mean'),
            answered_rate=('is_answered', 'mean')
        ).reset_index()
        
        trends.rename(columns={'tag_list': 'tag'}, inplace=True)
        
        logger.info(f"Agregação de tópicos concluída: {len(trends)} registros.")
        return trends