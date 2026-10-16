import pandas as pd
import logging
from typing import Dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IntegratedTransformer:
    
    @staticmethod
    def correlate_github_stackoverflow(repos_df: pd.DataFrame, 
                                        questions_df: pd.DataFrame,
                                        tags_df: pd.DataFrame) -> pd.DataFrame:
        """Cruza dados do GitHub com Stack Overflow por linguagem/tópico."""
        logger.info("Cruzando dados do GitHub e Stack Overflow...")
        
        if repos_df.empty or questions_df.empty:
            return pd.DataFrame()
        
        lang_stats = repos_df.groupby('language').agg(
            repo_count=('id', 'count'),
            total_stars=('stars', 'sum'),
            avg_stars=('stars', 'mean'),
            total_forks=('forks', 'sum'),
            avg_activity_score=('activity_score', 'mean')
        ).reset_index()
        
        tag_stats = questions_df.groupby('primary_tag').agg(
            question_count=('question_id', 'count'),
            avg_score=('score', 'mean'),
            avg_views=('view_count', 'mean'),
            answered_rate=('is_answered', 'mean')
        ).reset_index()
        
        tag_stats['tag_lower'] = tag_stats['primary_tag'].str.lower()
        lang_stats['language_lower'] = lang_stats['language'].str.lower()
        
        merged = lang_stats.merge(
            tag_stats,
            left_on='language_lower',
            right_on='tag_lower',
            how='inner'
        )
        
        if not merged.empty:
            merged['questions_per_repo'] = merged['question_count'] / merged['repo_count']
            merged['stars_per_question'] = merged['total_stars'] / merged['question_count']
            
            merged['community_engagement_score'] = (
                merged['repo_count'].rank(pct=True) * 0.3 +
                merged['total_stars'].rank(pct=True) * 0.3 +
                merged['question_count'].rank(pct=True) * 0.2 +
                merged['answered_rate'] * 0.2
            )
        
        logger.info(f"Cruzamento concluído: {len(merged)} linguagens/tópicos correspondentes.")
        return merged
    
    @staticmethod
    def generate_insights(repos_df: pd.DataFrame, 
                         questions_df: pd.DataFrame,
                         users_df: pd.DataFrame) -> Dict:
        """Gera insights consolidados a partir dos dados."""
        logger.info("Gerando insights consolidados...")
        
        insights = {}
        
        if not repos_df.empty:
            insights['top_languages'] = repos_df.groupby('language').agg(
                count=('id', 'count'),
                total_stars=('stars', 'sum')
            ).sort_values('total_stars', ascending=False).head(10).to_dict()
            
            insights['most_active_repos'] = repos_df.nlargest(10, 'activity_score')[
                ['full_name', 'stars', 'forks', 'activity_score']
            ].to_dict('records')
            
            insights['avg_repo_age'] = repos_df['repo_age_days'].mean()
        
        if not questions_df.empty:
            insights['top_tags'] = questions_df['primary_tag'].value_counts().head(10).to_dict()
            insights['avg_answer_rate'] = questions_df['is_answered'].mean()
            insights['total_questions'] = len(questions_df)
        
        if not users_df.empty:
            insights['top_users'] = users_df.nlargest(10, 'reputation')[
                ['display_name', 'reputation', 'total_badges']
            ].to_dict('records')
            insights['avg_reputation'] = users_df['reputation'].mean()
        
        logger.info("Insights gerados com sucesso.")
        return insights
    
    @staticmethod
    def validate_data_quality(df: pd.DataFrame, name: str) -> Dict:
        """Valida qualidade dos dados transformados."""
        logger.info(f"Validando qualidade dos dados: {name}")
        
        validation = {
            'dataset': name,
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'null_counts': df.isnull().sum().to_dict(),
            'duplicate_rows': df.duplicated().sum(),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
        }
        
        null_percentage = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
        validation['null_percentage'] = null_percentage
        
        if null_percentage > 50:
            logger.warning(f"Dataset {name} possui {null_percentage:.2f}% de valores nulos.")
        
        return validation