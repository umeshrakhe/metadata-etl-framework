-- Security and Credential Management Schema
-- This schema supports the SecurityManager class functionality

-- Users table for authentication
CREATE TABLE IF NOT EXISTS USERS (
    user_id VARCHAR(36) PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL DEFAULT 'viewer',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP NULL,
    email VARCHAR(255),
    full_name VARCHAR(255),
    department VARCHAR(100),
    INDEX idx_username (username),
    INDEX idx_role (role),
    INDEX idx_active (is_active)
);

-- Credentials table for encrypted storage
CREATE TABLE IF NOT EXISTS CREDENTIALS (
    credential_id VARCHAR(36) PRIMARY KEY,
    connection_id VARCHAR(255) NOT NULL,
    credential_type VARCHAR(50) NOT NULL, -- password, api_key, token, certificate
    encrypted_value TEXT NOT NULL,
    encryption_method VARCHAR(50) NOT NULL DEFAULT 'fernet',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NULL,
    last_rotated_at TIMESTAMP NULL,
    rotation_policy VARCHAR(100), -- daily, weekly, monthly, never
    rotation_count INT DEFAULT 0,
    last_accessed_at TIMESTAMP NULL,
    access_count INT DEFAULT 0,
    INDEX idx_connection_type (connection_id, credential_type),
    INDEX idx_expires (expires_at),
    INDEX idx_rotation (rotation_policy, last_rotated_at)
);

-- API tokens table for JWT token management
CREATE TABLE IF NOT EXISTS API_TOKENS (
    token_id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    token_hash VARCHAR(128) NOT NULL UNIQUE,
    expires_at TIMESTAMP NOT NULL,
    is_revoked BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMP NULL,
    usage_count INT DEFAULT 0,
    ip_address VARCHAR(45), -- IPv4/IPv6
    user_agent TEXT,
    FOREIGN KEY (user_id) REFERENCES USERS(user_id) ON DELETE CASCADE,
    INDEX idx_user (user_id),
    INDEX idx_hash (token_hash),
    INDEX idx_expires (expires_at),
    INDEX idx_revoked (is_revoked)
);

-- Security events table for audit logging
CREATE TABLE IF NOT EXISTS SECURITY_EVENTS (
    event_id VARCHAR(36) PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    user_id VARCHAR(36),
    resource VARCHAR(255),
    action VARCHAR(100),
    details JSON,
    ip_address VARCHAR(45),
    user_agent TEXT,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    severity VARCHAR(20) NOT NULL DEFAULT 'info', -- info, warning, error, critical
    session_id VARCHAR(36),
    request_id VARCHAR(36),
    FOREIGN KEY (user_id) REFERENCES USERS(user_id) ON DELETE SET NULL,
    INDEX idx_event_type (event_type),
    INDEX idx_user_timestamp (user_id, timestamp),
    INDEX idx_timestamp (timestamp),
    INDEX idx_severity (severity),
    INDEX idx_resource (resource)
);

-- SSL certificates table for certificate management
CREATE TABLE IF NOT EXISTS SSL_CERTIFICATES (
    cert_id VARCHAR(36) PRIMARY KEY,
    host VARCHAR(255) NOT NULL,
    port INT NOT NULL DEFAULT 443,
    cert_data TEXT NOT NULL,
    key_data TEXT, -- Encrypted private key
    cert_chain TEXT,
    issued_by VARCHAR(255),
    issued_to VARCHAR(255),
    valid_from TIMESTAMP NOT NULL,
    valid_until TIMESTAMP NOT NULL,
    serial_number VARCHAR(100),
    fingerprint VARCHAR(128),
    auto_renew BOOLEAN DEFAULT FALSE,
    renewal_days INT DEFAULT 30,
    last_checked TIMESTAMP NULL,
    status VARCHAR(20) DEFAULT 'active', -- active, expired, revoked
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_host_port (host, port),
    INDEX idx_valid_until (valid_until),
    INDEX idx_status (status)
);

-- User permissions table for fine-grained authorization
CREATE TABLE IF NOT EXISTS USER_PERMISSIONS (
    permission_id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    resource_type VARCHAR(50) NOT NULL, -- pipeline, connection, dataset, etc.
    resource_id VARCHAR(255) NOT NULL,
    permission VARCHAR(50) NOT NULL, -- read, write, execute, delete, admin
    granted_by VARCHAR(36) NOT NULL,
    granted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    conditions JSON, -- Additional conditions for permission
    FOREIGN KEY (user_id) REFERENCES USERS(user_id) ON DELETE CASCADE,
    FOREIGN KEY (granted_by) REFERENCES USERS(user_id),
    INDEX idx_user_resource (user_id, resource_type, resource_id),
    INDEX idx_permission (permission),
    INDEX idx_active (is_active),
    INDEX idx_expires (expires_at)
);

-- Password policies table
CREATE TABLE IF NOT EXISTS PASSWORD_POLICIES (
    policy_id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    min_length INT NOT NULL DEFAULT 8,
    require_uppercase BOOLEAN NOT NULL DEFAULT TRUE,
    require_lowercase BOOLEAN NOT NULL DEFAULT TRUE,
    require_digits BOOLEAN NOT NULL DEFAULT TRUE,
    require_special_chars BOOLEAN NOT NULL DEFAULT TRUE,
    max_age_days INT DEFAULT 90,
    prevent_reuse_count INT DEFAULT 5,
    lockout_attempts INT DEFAULT 5,
    lockout_duration_minutes INT DEFAULT 30,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_name (name)
);

-- User password history for preventing reuse
CREATE TABLE IF NOT EXISTS PASSWORD_HISTORY (
    history_id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    changed_by VARCHAR(36), -- NULL for self-change
    FOREIGN KEY (user_id) REFERENCES USERS(user_id) ON DELETE CASCADE,
    FOREIGN KEY (changed_by) REFERENCES USERS(user_id),
    INDEX idx_user_changed (user_id, changed_at)
);

-- Encryption keys table for key management
CREATE TABLE IF NOT EXISTS ENCRYPTION_KEYS (
    key_id VARCHAR(36) PRIMARY KEY,
    key_name VARCHAR(100) NOT NULL UNIQUE,
    key_type VARCHAR(50) NOT NULL, -- fernet, aes, rsa_public, rsa_private
    encrypted_key TEXT NOT NULL,
    key_version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    rotation_policy VARCHAR(100),
    last_rotated_at TIMESTAMP NULL,
    INDEX idx_name_type (key_name, key_type),
    INDEX idx_active (is_active),
    INDEX idx_expires (expires_at)
);

-- Security configuration table
CREATE TABLE IF NOT EXISTS SECURITY_CONFIG (
    config_key VARCHAR(100) PRIMARY KEY,
    config_value TEXT,
    config_type VARCHAR(50) DEFAULT 'string', -- string, int, bool, json
    description TEXT,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    updated_by VARCHAR(36),
    FOREIGN KEY (updated_by) REFERENCES USERS(user_id),
    INDEX idx_config_key (config_key)
);

-- Insert default security configuration
INSERT IGNORE INTO SECURITY_CONFIG (config_key, config_value, config_type, description) VALUES
('jwt_expiry_days', '30', 'int', 'Default JWT token expiry in days'),
('password_min_length', '8', 'int', 'Minimum password length'),
('max_login_attempts', '5', 'int', 'Maximum failed login attempts before lockout'),
('lockout_duration_minutes', '30', 'int', 'Account lockout duration in minutes'),
('session_timeout_minutes', '480', 'int', 'User session timeout in minutes'),
('audit_log_retention_days', '365', 'int', 'Security audit log retention period'),
('credential_rotation_days', '90', 'int', 'Default credential rotation interval'),
('ssl_cert_check_interval_hours', '24', 'int', 'SSL certificate check interval'),
('enable_2fa', 'false', 'bool', 'Enable two-factor authentication'),
('password_history_count', '5', 'int', 'Number of previous passwords to prevent reuse');

-- Insert default password policy
INSERT IGNORE INTO PASSWORD_POLICIES (
    policy_id, name, min_length, require_uppercase, require_lowercase,
    require_digits, require_special_chars, max_age_days, prevent_reuse_count,
    lockout_attempts, lockout_duration_minutes
) VALUES (
    'default_policy', 'Default Policy', 8, TRUE, TRUE, TRUE, TRUE, 90, 5, 5, 30
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_security_events_timestamp_type ON SECURITY_EVENTS (timestamp, event_type);
CREATE INDEX IF NOT EXISTS idx_credentials_connection_expires ON CREDENTIALS (connection_id, expires_at);
CREATE INDEX IF NOT EXISTS idx_api_tokens_user_expires ON API_TOKENS (user_id, expires_at, is_revoked);
CREATE INDEX IF NOT EXISTS idx_user_permissions_user_resource ON USER_PERMISSIONS (user_id, resource_type, resource_id, is_active);