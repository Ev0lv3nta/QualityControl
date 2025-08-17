-- Создание таблицы для токенов продолжения сессий
CREATE TABLE IF NOT EXISTS action_tokens (
    token TEXT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    action_type VARCHAR(50) NOT NULL,
    token_data JSONB,
    expires_at TIMESTAMPTZ NOT NULL
);

-- Индекс для быстрой очистки просроченных токенов
CREATE INDEX IF NOT EXISTS idx_action_tokens_expires_at ON action_tokens (expires_at);

-- Индекс для быстрого поиска токенов по пользователю и типу
CREATE INDEX IF NOT EXISTS idx_action_tokens_user_id_action_type ON action_tokens (user_id, action_type);
