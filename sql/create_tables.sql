-- Tabela de repositórios GitHub
CREATE TABLE IF NOT EXISTS github_repositories (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    full_name VARCHAR(500) NOT NULL,
    owner VARCHAR(255),
    description TEXT,
    language VARCHAR(100),
    stars INTEGER DEFAULT 0,
    forks INTEGER DEFAULT 0,
    open_issues INTEGER DEFAULT 0,
    watchers INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    pushed_at TIMESTAMP WITH TIME ZONE,
    size INTEGER,
    license VARCHAR(255),
    topics TEXT,
    url VARCHAR(500),
    repo_age_days INTEGER,
    days_since_last_push INTEGER,
    stars_per_day NUMERIC(10, 4),
    forks_per_star NUMERIC(10, 4),
    issues_per_star NUMERIC(10, 4),
    activity_score NUMERIC(15, 4),
    popularity_tier VARCHAR(50),
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de commits
CREATE TABLE IF NOT EXISTS github_commits (
    sha VARCHAR(40) PRIMARY KEY,
    repo_full_name VARCHAR(500) NOT NULL,
    author VARCHAR(255),
    author_email VARCHAR(255),
    date TIMESTAMP WITH TIME ZONE,
    message TEXT,
    additions INTEGER DEFAULT 0,
    deletions INTEGER DEFAULT 0,
    total_changes INTEGER DEFAULT 0,
    commit_date DATE,
    commit_hour INTEGER,
    commit_day_of_week VARCHAR(20),
    commit_month VARCHAR(10),
    message_length INTEGER,
    has_issue_reference BOOLEAN,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de issues
CREATE TABLE IF NOT EXISTS github_issues (
    issue_id BIGINT PRIMARY KEY,
    repo_full_name VARCHAR(500) NOT NULL,
    number INTEGER,
    title TEXT,
    state VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE,
    closed_at TIMESTAMP WITH TIME ZONE,
    comments INTEGER DEFAULT 0,
    labels TEXT,
    user_login VARCHAR(255),
    resolution_time_days NUMERIC(10, 2),
    is_closed BOOLEAN,
    created_date DATE,
    created_month VARCHAR(10),
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de pull requests
CREATE TABLE IF NOT EXISTS github_pull_requests (
    pr_id BIGINT PRIMARY KEY,
    repo_full_name VARCHAR(500) NOT NULL,
    number INTEGER,
    title TEXT,
    state VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE,
    closed_at TIMESTAMP WITH TIME ZONE,
    merged_at TIMESTAMP WITH TIME ZONE,
    user_login VARCHAR(255),
    additions INTEGER DEFAULT 0,
    deletions INTEGER DEFAULT 0,
    changed_files INTEGER DEFAULT 0,
    time_to_merge_days NUMERIC(10, 2),
    is_merged BOOLEAN,
    created_date DATE,
    created_month VARCHAR(10),
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de perguntas Stack Overflow
CREATE TABLE IF NOT EXISTS stackoverflow_questions (
    question_id BIGINT PRIMARY KEY,
    title TEXT NOT NULL,
    body TEXT,
    score INTEGER DEFAULT 0,
    view_count INTEGER DEFAULT 0,
    answer_count INTEGER DEFAULT 0,
    comment_count INTEGER DEFAULT 0,
    favorite_count INTEGER DEFAULT 0,
    creation_date TIMESTAMP WITH TIME ZONE,
    last_activity_date TIMESTAMP WITH TIME ZONE,
    tags TEXT,
    is_answered BOOLEAN,
    accepted_answer_id BIGINT,
    owner_user_id BIGINT,
    owner_reputation INTEGER DEFAULT 0,
    link VARCHAR(500),
    creation_year INTEGER,
    creation_month VARCHAR(10),
    creation_day_of_week VARCHAR(20),
    age_days INTEGER,
    views_per_day NUMERIC(10, 4),
    score_per_view NUMERIC(10, 6),
    answer_rate NUMERIC(10, 6),
    difficulty_level VARCHAR(50),
    primary_tag VARCHAR(100),
    tag_count INTEGER,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de respostas Stack Overflow
CREATE TABLE IF NOT EXISTS stackoverflow_answers (
    answer_id BIGINT PRIMARY KEY,
    question_id BIGINT NOT NULL,
    body TEXT,
    score INTEGER DEFAULT 0,
    is_accepted BOOLEAN,
    creation_date TIMESTAMP WITH TIME ZONE,
    last_activity_date TIMESTAMP WITH TIME ZONE,
    owner_user_id BIGINT,
    owner_reputation INTEGER DEFAULT 0,
    creation_year INTEGER,
    creation_month VARCHAR(10),
    body_length INTEGER,
    is_high_score BOOLEAN,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de usuários Stack Overflow
CREATE TABLE IF NOT EXISTS stackoverflow_users (
    user_id BIGINT PRIMARY KEY,
    display_name VARCHAR(255),
    reputation INTEGER DEFAULT 0,
    creation_date TIMESTAMP WITH TIME ZONE,
    last_access_date TIMESTAMP WITH TIME ZONE,
    location VARCHAR(255),
    website_url VARCHAR(500),
    badge_gold INTEGER DEFAULT 0,
    badge_silver INTEGER DEFAULT 0,
    badge_bronze INTEGER DEFAULT 0,
    answer_count INTEGER DEFAULT 0,
    question_count INTEGER DEFAULT 0,
    view_count INTEGER DEFAULT 0,
    account_age_days INTEGER,
    reputation_per_day NUMERIC(10, 4),
    total_badges INTEGER,
    answer_question_ratio NUMERIC(10, 4),
    reputation_tier VARCHAR(50),
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de tags Stack Overflow
CREATE TABLE IF NOT EXISTS stackoverflow_tags (
    tag_name VARCHAR(100) PRIMARY KEY,
    count INTEGER DEFAULT 0,
    has_synonyms BOOLEAN,
    is_moderator_only BOOLEAN,
    is_required BOOLEAN,
    tag_popularity VARCHAR(50),
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de análise integrada
CREATE TABLE IF NOT EXISTS integrated_analysis (
    id SERIAL PRIMARY KEY,
    language VARCHAR(100),
    repo_count INTEGER,
    total_stars BIGINT,
    avg_stars NUMERIC(15, 2),
    total_forks BIGINT,
    avg_activity_score NUMERIC(15, 4),
    question_count INTEGER,
    avg_score NUMERIC(10, 2),
    avg_views NUMERIC(15, 2),
    answered_rate NUMERIC(5, 4),
    questions_per_repo NUMERIC(10, 4),
    stars_per_question NUMERIC(15, 4),
    community_engagement_score NUMERIC(10, 6),
    analyzed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_repos_language ON github_repositories(language);
CREATE INDEX IF NOT EXISTS idx_repos_stars ON github_repositories(stars DESC);
CREATE INDEX IF NOT EXISTS idx_repos_activity ON github_repositories(activity_score DESC);
CREATE INDEX IF NOT EXISTS idx_commits_repo ON github_commits(repo_full_name);
CREATE INDEX IF NOT EXISTS idx_commits_date ON github_commits(date);
CREATE INDEX IF NOT EXISTS idx_issues_repo ON github_issues(repo_full_name);
CREATE INDEX IF NOT EXISTS idx_issues_state ON github_issues(state);
CREATE INDEX IF NOT EXISTS idx_prs_repo ON github_pull_requests(repo_full_name);
CREATE INDEX IF NOT EXISTS idx_questions_tags ON stackoverflow_questions(primary_tag);
CREATE INDEX IF NOT EXISTS idx_questions_creation ON stackoverflow_questions(creation_date);
CREATE INDEX IF NOT EXISTS idx_questions_score ON stackoverflow_questions(score DESC);
CREATE INDEX IF NOT EXISTS idx_answers_question ON stackoverflow_answers(question_id);
CREATE INDEX IF NOT EXISTS idx_users_reputation ON stackoverflow_users(reputation DESC);
CREATE INDEX IF NOT EXISTS idx_tags_count ON stackoverflow_tags(count DESC);
CREATE INDEX IF NOT EXISTS idx_integrated_language ON integrated_analysis(language);