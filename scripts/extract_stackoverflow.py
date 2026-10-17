import requests
import time
import logging
from typing import List, Dict
import yaml
import os
import gzip
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StackOverflowExtractor:
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

        stack_config = self.config.get('stackexchange', {})
        self.base_url = stack_config.get('base_url', 'https://api.stackexchange.com/2.3')
        self.site = stack_config.get('site', 'stackoverflow')
        self.pagesize = stack_config.get('pagesize', 100)

        raw_key = os.getenv('STACKEXCHANGE_KEY') or stack_config.get('key', '')
        if isinstance(raw_key, str):
            raw_key = raw_key.strip()
            if raw_key in {'', '$******', '******', 'changeme', 'your_stackexchange_key', 'your_api_key'}:
                raw_key = None
        self.key = raw_key
        self.session = requests.Session()
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Executa requisição à API do Stack Exchange com tratamento de erros."""
        if params is None:
            params = {}
        
        params.update({
            'site': self.site,
            'pagesize': self.pagesize,
            'filter': 'withbody'
        })
        if self.key:
            params['key'] = self.key
        
        url = f"{self.base_url}{endpoint}"
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if 'backoff' in data:
                        backoff = data['backoff']
                        logger.warning(f"Backoff solicitado: {backoff} segundos.")
                        time.sleep(backoff)
                    
                    quota = data.get('quota_remaining', 0)
                    if quota < 100:
                        logger.warning(f"Quota baixa: {quota} requisições restantes.")
                    
                    return data
                elif response.status_code == 400:
                    logger.error(f"Requisição inválida: {response.text}")
                    return {}
                elif response.status_code == 429:
                    logger.warning(f"Rate limit atingido. Aguardando...")
                    time.sleep(30)
                else:
                    logger.error(f"Erro HTTP {response.status_code}: {response.text}")
                    response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f"Erro na requisição (tentativa {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(5)
        
        return {}
    
    def extract_questions(self, tagged: str = None, sort: str = 'votes', 
                          max_questions: int = 500) -> List[Dict]:
        """Extrai perguntas do Stack Overflow."""
        logger.info(f"Iniciando extração de perguntas (tag: {tagged or 'todas'})...")
        questions = []
        page = 1
        
        while len(questions) < max_questions:
            params = {
                'sort': sort,
                'order': 'desc',
                'page': page
            }
            
            if tagged:
                params['tagged'] = tagged
            
            data = self._make_request('/questions', params)
            items = data.get('items', [])
            
            if not items:
                break
            
            for q in items:
                questions.append({
                    'question_id': q['question_id'],
                    'title': q['title'],
                    'body': q.get('body', '')[:5000],
                    'score': q['score'],
                    'view_count': q['view_count'],
                    'answer_count': q['answer_count'],
                    'comment_count': q.get('comment_count', 0),
                    'favorite_count': q.get('favorite_count', 0),
                    'creation_date': q['creation_date'],
                    'last_activity_date': q['last_activity_date'],
                    'tags': ','.join(q.get('tags', [])),
                    'is_answered': q['is_answered'],
                    'accepted_answer_id': q.get('accepted_answer_id'),
                    'owner_user_id': q.get('owner', {}).get('user_id'),
                    'owner_reputation': q.get('owner', {}).get('reputation', 0),
                    'link': q['link']
                })
            
            logger.info(f"Página {page} extraída. Total: {len(questions)} perguntas.")
            page += 1
            
            if not data.get('has_more', False):
                break
        
        logger.info(f"Extração concluída: {len(questions)} perguntas.")
        return questions
    
    def extract_answers(self, question_ids: List[int] = None, 
                        max_answers: int = 1000) -> List[Dict]:
        """Extrai respostas do Stack Overflow."""
        logger.info("Iniciando extração de respostas...")
        answers = []
        page = 1
        
        while len(answers) < max_answers:
            params = {
                'sort': 'votes',
                'order': 'desc',
                'page': page
            }
            
            if question_ids:
                params['question_ids'] = ';'.join(map(str, question_ids[:100]))
            
            data = self._make_request('/answers', params)
            items = data.get('items', [])
            
            if not items:
                break
            
            for a in items:
                answers.append({
                    'answer_id': a['answer_id'],
                    'question_id': a['question_id'],
                    'body': a.get('body', '')[:5000],
                    'score': a['score'],
                    'is_accepted': a['is_accepted'],
                    'creation_date': a['creation_date'],
                    'last_activity_date': a['last_activity_date'],
                    'owner_user_id': a.get('owner', {}).get('user_id'),
                    'owner_reputation': a.get('owner', {}).get('reputation', 0)
                })
            
            page += 1
            if not data.get('has_more', False):
                break
        
        logger.info(f"Extração concluída: {len(answers)} respostas.")
        return answers
    
    def extract_users(self, max_users: int = 500) -> List[Dict]:
        """Extrai usuários do Stack Overflow."""
        logger.info("Iniciando extração de usuários...")
        users = []
        page = 1
        
        while len(users) < max_users:
            params = {
                'sort': 'reputation',
                'order': 'desc',
                'page': page
            }
            
            data = self._make_request('/users', params)
            items = data.get('items', [])
            
            if not items:
                break
            
            for u in items:
                users.append({
                    'user_id': u['user_id'],
                    'display_name': u['display_name'],
                    'reputation': u['reputation'],
                    'creation_date': u['creation_date'],
                    'last_access_date': u['last_access_date'],
                    'location': u.get('location', ''),
                    'website_url': u.get('website_url', ''),
                    'badge_gold': u.get('badge_counts', {}).get('gold', 0),
                    'badge_silver': u.get('badge_counts', {}).get('silver', 0),
                    'badge_bronze': u.get('badge_counts', {}).get('bronze', 0),
                    'answer_count': u.get('answer_count', 0),
                    'question_count': u.get('question_count', 0),
                    'view_count': u.get('view_count', 0)
                })
            
            page += 1
            if not data.get('has_more', False):
                break
        
        logger.info(f"Extração concluída: {len(users)} usuários.")
        return users
    
    def extract_tags(self, max_tags: int = 100) -> List[Dict]:
        """Extrai tags populares do Stack Overflow."""
        logger.info("Iniciando extração de tags...")
        tags = []
        page = 1
        
        while len(tags) < max_tags:
            params = {
                'sort': 'popular',
                'order': 'desc',
                'page': page
            }
            
            data = self._make_request('/tags', params)
            items = data.get('items', [])
            
            if not items:
                break
            
            for t in items:
                tags.append({
                    'tag_name': t['name'],
                    'count': t['count'],
                    'has_synonyms': t.get('has_synonyms', False),
                    'is_moderator_only': t.get('is_moderator_only', False),
                    'is_required': t.get('is_required', False)
                })
            
            page += 1
            if not data.get('has_more', False):
                break
        
        logger.info(f"Extração concluída: {len(tags)} tags.")
        return tags