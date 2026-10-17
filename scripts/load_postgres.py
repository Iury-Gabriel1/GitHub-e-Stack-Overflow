import pandas as pd
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import yaml
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PostgresLoader:
    def __init__(self, config_path: str = '/opt/airflow/config/config.yaml'):
        default_config_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'config',
            'config.yaml'
        )
        if not os.path.exists(config_path) and os.path.exists(default_config_path):
            config_path = default_config_path

        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f) or {}

        db = self.config.get('database', {})
        self.connection_string = (
            f"postgresql+psycopg2://{db.get('user', 'etl_user')}:{db.get('password', 'etl_password')}"
            f"@{db.get('host', 'postgres')}:{db.get('port', 5432)}/{db.get('database', 'etl_db')}"
        )
        self.engine = create_engine(self.connection_string, pool_pre_ping=True)

    def create_tables(self, sql_file: str = '/opt/airflow/sql/create_tables.sql'):
        """Cria as tabelas no banco de dados."""
        if not os.path.exists(sql_file):
            local_sql_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                'sql',
                'create_tables.sql'
            )
            if os.path.exists(local_sql_path):
                sql_file = local_sql_path

        logger.info("Criando tabelas no PostgreSQL...")

        with open(sql_file, 'r') as f:
            sql = f.read()
        
        with self.engine.begin() as conn:
            conn.execute(text(sql))
        
        logger.info("Tabelas criadas com sucesso.")
    
    def load_dataframe(self, df: pd.DataFrame, table_name: str, 
                       if_exists: str = 'append', chunk_size: int = 1000):
        """Carrega um DataFrame em uma tabela PostgreSQL."""
        if df.empty:
            logger.warning(f"DataFrame vazio para tabela {table_name}. Ignorando.")
            return
        
        logger.info(f"Carregando {len(df)} registros na tabela {table_name}...")
        
        try:
            df.to_sql(
                table_name,
                self.engine,
                if_exists=if_exists,
                index=False,
                chunksize=chunk_size,
                method='multi'
            )
            logger.info(f"Carga concluída: {table_name}")
        except SQLAlchemyError as e:
            logger.error(f"Erro ao carregar {table_name}: {e}")
            raise
    
    def upsert_dataframe(self, df: pd.DataFrame, table_name: str, 
                         conflict_columns: list):
        """Realiza upsert (INSERT ... ON CONFLICT UPDATE) no PostgreSQL."""
        if df.empty:
            logger.warning(f"DataFrame vazio para tabela {table_name}. Ignorando.")
            return
        
        logger.info(f"Realizando upsert de {len(df)} registros em {table_name}...")
        
        columns = df.columns.tolist()
        columns_str = ', '.join(columns)
        placeholders = ', '.join([f':{col}' for col in columns])
        
        update_columns = [col for col in columns if col not in conflict_columns]
        update_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        
        conflict_str = ', '.join(conflict_columns)
        
        query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_str})
            DO UPDATE SET {update_str}
        """
        
        records = df.to_dict('records')
        
        with self.engine.begin() as conn:
            for i in range(0, len(records), 1000):
                batch = records[i:i + 1000]
                conn.execute(text(query), batch)
        
        logger.info(f"Upsert concluído: {table_name}")
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Executa uma consulta SQL e retorna um DataFrame."""
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.mappings().all()
            return pd.DataFrame(rows)
    
    def get_table_counts(self) -> dict:
        """Retorna a contagem de registros por tabela."""
        tables = [
            'github_repositories', 'github_commits', 'github_issues',
            'github_pull_requests', 'stackoverflow_questions',
            'stackoverflow_answers', 'stackoverflow_users',
            'stackoverflow_tags', 'integrated_analysis'
        ]
        
        counts = {}
        for table in tables:
            try:
                result = self.execute_query(f"SELECT COUNT(*) as count FROM {table}")
                counts[table] = int(result['count'].iloc[0])
            except Exception as e:
                logger.warning(f"Erro ao contar {table}: {e}")
                counts[table] = 0
        
        return counts