# Security Manager

The SecurityManager class provides comprehensive security and credential management for the ETL framework, including encryption, authentication, authorization, and integration with external secret management services.

## Features

### Encryption Methods
- **Fernet**: Symmetric encryption using cryptography library
- **AES-256**: Advanced symmetric encryption
- **RSA**: Asymmetric encryption for key exchange (planned)
- **AWS KMS**: Cloud-based key management
- **Azure Key Vault**: Cloud-based secret storage
- **HashiCorp Vault**: Enterprise secret management

### Credential Management
- Secure storage of database passwords, API keys, and tokens
- Automatic credential rotation with configurable policies
- Expiry tracking and renewal notifications
- Encrypted storage with multiple encryption methods

### Authentication & Authorization
- User authentication with secure password hashing
- Role-based access control (RBAC)
- JWT-based API token management
- Fine-grained permissions system

### Security Auditing
- Comprehensive audit logging of security events
- Configurable retention policies
- Event correlation and analysis
- Compliance reporting capabilities

### SSL/TLS Management
- Certificate validation and monitoring
- Expiry tracking and renewal alerts
- Secure connection string building

## Usage Examples

### Basic Encryption/Decryption

```python
from utils.security_manager import SecurityManager

security = SecurityManager()

# Encrypt a password
encrypted = security.encrypt_credential("my_password", "fernet")

# Decrypt it back
decrypted = security.decrypt_credential(encrypted, "fernet")
```

### Credential Storage

```python
# Store a database credential
cred_id = security.store_credential(
    connection_id="prod_db",
    credential_type="password",
    credential_value="secret_password",
    encryption_method="fernet",
    expires_days=90
)

# Retrieve it later
password = security.retrieve_credential("prod_db", "password")
```

### API Token Management

```python
# Generate JWT token for user
token = security.generate_api_token("user_123", expiry_days=30)

# Validate token in requests
user_id = security.validate_api_token(token)

# Revoke token if needed
security.revoke_api_token(token)
```

### User Authentication

```python
# Authenticate user
user = security.authenticate_user("john_doe", "password123")

if user:
    # Check authorization
    can_execute = security.authorize_action(user.user_id, "pipeline", "execute")
```

### SSL Certificate Validation

```python
# Check SSL certificate
cert_info = security.check_ssl_certificate("api.example.com", 443)

if not cert_info["valid"]:
    print(f"Certificate issue: {cert_info['status']}")
```

### Cloud Secret Integration

```python
# AWS Secrets Manager
secrets = security.integrate_aws_secrets_manager("my-secret-name")

# Azure Key Vault
secret_value = security.integrate_azure_key_vault(
    "https://myvault.vault.azure.net/",
    "my-secret"
)

# HashiCorp Vault
secrets = security.integrate_hashicorp_vault(
    "https://vault.example.com:8200",
    "secret/myapp/config"
)
```

## Database Schema

The SecurityManager requires several database tables:

- `USERS`: User accounts and authentication
- `CREDENTIALS`: Encrypted credential storage
- `API_TOKENS`: JWT token management
- `SECURITY_EVENTS`: Audit logging
- `SSL_CERTIFICATES`: Certificate management
- `USER_PERMISSIONS`: Fine-grained permissions
- `PASSWORD_POLICIES`: Password requirements
- `ENCRYPTION_KEYS`: Key management

See `database/security_schema.sql` for the complete schema.

## Configuration

Security settings can be configured via the `SECURITY_CONFIG` table or environment variables:

- `jwt_expiry_days`: Default token expiry (default: 30)
- `password_min_length`: Minimum password length (default: 8)
- `max_login_attempts`: Failed login threshold (default: 5)
- `audit_log_retention_days`: Log retention period (default: 365)

## Security Best Practices

1. **Key Management**: Regularly rotate encryption keys
2. **Credential Rotation**: Implement automatic credential rotation
3. **Audit Logging**: Enable comprehensive security event logging
4. **Access Control**: Use principle of least privilege
5. **SSL/TLS**: Keep certificates current and monitor expiry
6. **Token Management**: Set appropriate token expiry times
7. **Password Policies**: Enforce strong password requirements

## Dependencies

### Required
- `cryptography`: For encryption operations

### Optional
- `PyJWT`: For API token management
- `boto3`: For AWS Secrets Manager integration
- `azure-identity` + `azure-keyvault-secrets`: For Azure Key Vault
- `hvac`: For HashiCorp Vault integration

Install optional dependencies as needed:

```bash
pip install PyJWT boto3 azure-identity azure-keyvault-secrets hvac
```

## Error Handling

The SecurityManager includes comprehensive error handling:

- Graceful degradation when optional dependencies are missing
- Detailed logging of security events
- Secure failure modes that don't expose sensitive information
- Validation of inputs and outputs

## Integration with ETL Framework

The SecurityManager integrates with other framework components:

- **ConnectorFactory**: Retrieves secure connection credentials
- **OrchestratorManager**: Checks user permissions for pipeline execution
- **AuditLogger**: Logs security events alongside operational events
- **ConfigLoader**: Loads security configuration settings

## Monitoring and Compliance

- Security event monitoring and alerting
- Compliance reporting for audits
- Integration with SIEM systems
- Automated security health checks