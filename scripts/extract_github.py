import requests
import time
import logging
from typing import List, Dict
import yaml
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GitHubExtractor:
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

        github_config = self.config.get('github', {})
        self.base_url = github_config.get('base_url', 'https://api.github.com')
        self.per_page = github_config.get('per_page', 100)

        raw_token = os.getenv('GITHUB_TOKEN') or github_config.get('token', '')
        if isinstance(raw_token, str):
            raw_token = raw_token.strip()
            if raw_token in {'', '$******', '******', 'changeme', 'your_github_token'}:
                raw_token = None
        self.token = raw_token
        self.headers = {
            'Accept': 'application/vnd.github.v3+json'
        }
        if self.token:
            self.headers['Authorization'] = f'token {self.token}'
    
    def _handle_rate_limit(self, response: requests.Response):
        """Trata limites de requisição da API GitHub."""
        remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
        if remaining < 10:
            reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
            sleep_time = max(reset_time - time.time(), 0) + 10
            logger.warning(f"Rate limit próximo. Aguardando {sleep_time:.0f} segundos.")
            time.sleep(sleep_time)
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Executa requisição com tratamento de erros e retry."""
        url = f"{self.base_url}{endpoint}"
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
                self._handle_rate_limit(response)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 403:
                    logger.warning(f"Rate limit atingido. Tentativa {attempt + 1}/{max_retries}")
                    time.sleep(60)
                else:
                    logger.error(f"Erro HTTP {response.status_code}: {response.text}")
                    response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f"Erro na requisição (tentativa {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(5)
        
        return {}
    
    def extract_popular_repositories(self, min_stars: int = 1000, max_results: int = 500) -> List[Dict]:
        """Extrai repositórios populares do GitHub."""
        logger.info("Iniciando extração de repositórios populares...")
        repositories = []
        page = 1
        
        while len(repositories) < max_results:
            params = {
                'q': f'stars:>={min_stars}',
                'sort': 'stars',
                'order': 'desc',
                'per_page': self.per_page,
                'page': page
            }
            
            data = self._make_request('/search/repositories', params)
            items = data.get('items', [])
            
            if not items:
                break
            
            for repo in items:
                repositories.append({
                    'id': repo['id'],
                    'name': repo['name'],
                    'full_name': repo['full_name'],
                    'owner': repo['owner']['login'],
                    'description': repo.get('description', ''),
                    'language': repo.get('language', 'Unknown'),
                    'stars': repo['stargazers_count'],
                    'forks': repo['forks_count'],
                    'open_issues': repo['open_issues_count'],
                    'watchers': repo['watchers_count'],
                    'created_at': repo['created_at'],
                    'updated_at': repo['updated_at'],
                    'pushed_at': repo['pushed_at'],
                    'size': repo['size'],
                    'license': repo.get('license', {}).get('name', 'No License') if repo.get('license') else 'No License',
                    'topics': ','.join(repo.get('topics', [])),
                    'url': repo['html_url']
                })
            
            logger.info(f"Página {page} extraída. Total: {len(repositories)} repositórios.")
            page += 1
            
            if len(items) < self.per_page:
                break
        
        logger.info(f"Extração concluída: {len(repositories)} repositórios.")
        return repositories
    
    def extract_commits(self, repo_full_name: str, max_commits: int = 100) -> List[Dict]:
        """Extrai commits de um repositório específico."""
        commits = []
        page = 1
        
        while len(commits) < max_commits:
            params = {
                'per_page': min(self.per_page, 100),
                'page': page
            }
            
            data = self._make_request(f'/repos/{repo_full_name}/commits', params)
            
            if not data or not isinstance(data, list):
                break
            
            for commit in data:
                commits.append({
                    'repo_full_name': repo_full_name,
                    'sha': commit['sha'],
                    'author': commit['commit']['author']['name'],
                    'author_email': commit['commit']['author']['email'],
                    'date': commit['commit']['author']['date'],
                    'message': commit['commit']['message'][:500],
                    'additions': commit.get('stats', {}).get('additions', 0),
                    'deletions': commit.get('stats', {}).get('deletions', 0),
                    'total_changes': commit.get('stats', {}).get('total', 0)
                })
            
            page += 1
            if len(data) < self.per_page:
                break
        
        return commits
    
    def extract_issues(self, repo_full_name: str, state: str = 'all', max_issues: int = 100) -> List[Dict]:
        """Extrai issues de um repositório específico."""
        issues = []
        page = 1
        
        while len(issues) < max_issues:
            params = {
                'state': state,
                'per_page': min(self.per_page, 100),
                'page': page
            }
            
            data = self._make_request(f'/repos/{repo_full_name}/issues', params)
            
            if not data or not isinstance(data, list):
                break
            
            for issue in data:
                if 'pull_request' in issue:
                    continue
                
                issues.append({
                    'repo_full_name': repo_full_name,
                    'issue_id': issue['id'],
                    'number': issue['number'],
                    'title': issue['title'][:500],
                    'state': issue['state'],
                    'created_at': issue['created_at'],
                    'closed_at': issue.get('closed_at'),
                    'comments': issue['comments'],
                    'labels': ','.join([l['name'] for l in issue.get('labels', [])]),
                    'user': issue['user']['login']
                })
            
            page += 1
            if len(data) < self.per_page:
                break
        
        return issues
    
    def extract_pull_requests(self, repo_full_name: str, state: str = 'all', max_prs: int = 100) -> List[Dict]:
        """Extrai pull requests de um repositório específico."""
        prs = []
        page = 1
        
        while len(prs) < max_prs:
            params = {
                'state': state,
                'per_page': min(self.per_page, 100),
                'page': page
            }
            
            data = self._make_request(f'/repos/{repo_full_name}/pulls', params)
            
            if not data or not isinstance(data, list):
                break
            
            for pr in data:
                prs.append({
                    'repo_full_name': repo_full_name,
                    'pr_id': pr['id'],
                    'number': pr['number'],
                    'title': pr['title'][:500],
                    'state': pr['state'],
                    'created_at': pr['created_at'],
                    'closed_at': pr.get('closed_at'),
                    'merged_at': pr.get('merged_at'),
                    'user': pr['user']['login'],
                    'additions': pr.get('additions', 0),
                    'deletions': pr.get('deletions', 0),
                    'changed_files': pr.get('changed_files', 0)
                })
            
            page += 1
            if len(data) < self.per_page:
                break
        
        return prs