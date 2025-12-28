import logging
import base64
import hashlib
import hmac
import json
import secrets
import string
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
import uuid

# Optional imports
try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.primitives.asymmetric import rsa, padding
    from cryptography.hazmat.primitives import serialization
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False

try:
    import boto3
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

try:
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

try:
    import hvac
    HVAC_AVAILABLE = True
except ImportError:
    HVAC_AVAILABLE = False

try:
    import jwt
    JWT_AVAILABLE = True
except ImportError:
    JWT_AVAILABLE = False

try:
    import ssl
    import socket
    SSL_AVAILABLE = True
except ImportError:
    SSL_AVAILABLE = False

logger = logging.getLogger("SecurityManager")


@dataclass
class Credential:
    """Represents a stored credential."""
    credential_id: str
    connection_id: str
    credential_type: str  # password, api_key, token, certificate
    encrypted_value: str
    encryption_method: str
    created_at: datetime
    expires_at: Optional[datetime] = None
    last_rotated_at: Optional[datetime] = None
    rotation_policy: Optional[str] = None


@dataclass
class User:
    """Represents a system user."""
    user_id: str
    username: str
    hashed_password: str
    role: str
    is_active: bool = True
    created_at: datetime = None
    last_login: Optional[datetime] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()


@dataclass
class APIToken:
    """Represents an API token."""
    token_id: str
    user_id: str
    token_hash: str
    expires_at: datetime
    is_revoked: bool = False
    created_at: datetime = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()


@dataclass
class SecurityEvent:
    """Represents a security audit event."""
    event_id: str
    event_type: str
    user_id: Optional[str]
    resource: Optional[str]
    action: Optional[str]
    details: Dict[str, Any]
    ip_address: Optional[str]
    user_agent: Optional[str]
    timestamp: datetime
    severity: str = "info"  # info, warning, error, critical


class SecurityManager:
    """
    Comprehensive security and credential management for ETL framework.
    Handles encryption, authentication, authorization, and secret management.
    """

    def __init__(self, db_connection: Any = None, master_key: Optional[str] = None):
        self.db = db_connection
        self.logger = logging.getLogger(self.__class__.__name__)

        # Master encryption key
        self.master_key = master_key or self._generate_master_key()

        # Encryption keys cache
        self.encryption_keys: Dict[str, bytes] = {}

        # JWT secret for token signing
        self.jwt_secret = secrets.token_hex(32)

        if not CRYPTOGRAPHY_AVAILABLE:
            self.logger.warning("cryptography not available - encryption features limited")

    def _generate_master_key(self) -> str:
        """Generate a new master encryption key."""
        if CRYPTOGRAPHY_AVAILABLE:
            key = Fernet.generate_key()
            return key.decode()
        else:
            return secrets.token_hex(32)

    def encrypt_credential(self, plaintext: str, encryption_method: str = "fernet") -> str:
        """Encrypt a credential using specified method."""
        if not CRYPTOGRAPHY_AVAILABLE:
            raise ImportError("cryptography required for encryption")

        try:
            if encryption_method == "fernet":
                key = self._get_or_create_key("fernet")
                f = Fernet(key)
                encrypted = f.encrypt(plaintext.encode())
                return base64.b64encode(encrypted).decode()

            elif encryption_method == "aes":
                key = self._get_or_create_key("aes")
                iv = secrets.token_bytes(16)
                cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
                encryptor = cipher.encryptor()

                # Pad plaintext to block size
                block_size = 16
                padded_data = plaintext.encode() + b"\0" * (block_size - len(plaintext) % block_size)

                encrypted = encryptor.update(padded_data) + encryptor.finalize()
                return base64.b64encode(iv + encrypted).decode()

            else:
                raise ValueError(f"Unsupported encryption method: {encryption_method}")

        except Exception as e:
            self.logger.exception(f"Failed to encrypt credential: {e}")
            raise

    def decrypt_credential(self, encrypted_data: str, encryption_method: str = "fernet") -> str:
        """Decrypt a credential using specified method."""
        if not CRYPTOGRAPHY_AVAILABLE:
            raise ImportError("cryptography required for decryption")

        try:
            if encryption_method == "fernet":
                key = self._get_or_create_key("fernet")
                f = Fernet(key)
                encrypted_bytes = base64.b64decode(encrypted_data)
                decrypted = f.decrypt(encrypted_bytes)
                return decrypted.decode()

            elif encryption_method == "aes":
                key = self._get_or_create_key("aes")
                encrypted_bytes = base64.b64decode(encrypted_data)

                iv = encrypted_bytes[:16]
                ciphertext = encrypted_bytes[16:]

                cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
                decryptor = cipher.decryptor()

                decrypted_padded = decryptor.update(ciphertext) + decryptor.finalize()
                # Remove padding
                decrypted = decrypted_padded.rstrip(b"\0")
                return decrypted.decode()

            else:
                raise ValueError(f"Unsupported encryption method: {encryption_method}")

        except Exception as e:
            self.logger.exception(f"Failed to decrypt credential: {e}")
            raise

    def _get_or_create_key(self, method: str) -> bytes:
        """Get or create encryption key for method."""
        if method not in self.encryption_keys:
            if method == "fernet":
                # Derive key from master key
                salt = b"etl_framework_salt"
                kdf = PBKDF2HMAC(
                    algorithm=hashes.SHA256(),
                    length=32,
                    salt=salt,
                    iterations=100000,
                )
                key = base64.urlsafe_b64encode(kdf.derive(self.master_key.encode()))
                self.encryption_keys[method] = key
            elif method == "aes":
                # Use master key directly for AES
                key = self.master_key.encode()[:32].ljust(32, b'\0')
                self.encryption_keys[method] = key

        return self.encryption_keys[method]

    def store_credential(self, connection_id: str, credential_type: str,
                        credential_value: str, encryption_method: str = "fernet",
                        expires_days: Optional[int] = None) -> str:
        """Store an encrypted credential."""
        encrypted_value = self.encrypt_credential(credential_value, encryption_method)

        expires_at = None
        if expires_days:
            expires_at = datetime.utcnow() + timedelta(days=expires_days)

        credential = Credential(
            credential_id=str(uuid.uuid4()),
            connection_id=connection_id,
            credential_type=credential_type,
            encrypted_value=encrypted_value,
            encryption_method=encryption_method,
            created_at=datetime.utcnow(),
            expires_at=expires_at
        )

        # Store in database
        self._store_credential_db(credential)

        self.logger.info(f"Stored {credential_type} credential for connection {connection_id}")
        return credential.credential_id

    def retrieve_credential(self, connection_id: str, credential_type: str) -> Optional[str]:
        """Retrieve and decrypt a credential."""
        credential = self._get_credential_db(connection_id, credential_type)

        if not credential:
            return None

        # Check if expired
        if credential.expires_at and credential.expires_at < datetime.utcnow():
            self.logger.warning(f"Credential expired for {connection_id}")
            return None

        try:
            decrypted = self.decrypt_credential(credential.encrypted_value, credential.encryption_method)
            return decrypted
        except Exception as e:
            self.logger.exception(f"Failed to decrypt credential for {connection_id}: {e}")
            return None

    def rotate_credential(self, connection_id: str, credential_type: str, new_value: str) -> bool:
        """Rotate a credential with new value."""
        credential = self._get_credential_db(connection_id, credential_type)

        if not credential:
            return False

        # Encrypt new value
        encrypted_value = self.encrypt_credential(new_value, credential.encryption_method)

        # Update in database
        self._update_credential_db(credential.credential_id, encrypted_value)

        # Log rotation event
        self.log_security_event(
            "credential_rotated",
            None,
            f"connection_{connection_id}",
            "rotate",
            {"credential_type": credential_type}
        )

        self.logger.info(f"Rotated {credential_type} credential for connection {connection_id}")
        return True

    def generate_encryption_key(self, method: str = "fernet") -> str:
        """Generate a new encryption key."""
        if method == "fernet":
            if CRYPTOGRAPHY_AVAILABLE:
                key = Fernet.generate_key()
                return key.decode()
            else:
                return secrets.token_hex(32)
        elif method == "aes":
            return secrets.token_hex(32)
        else:
            raise ValueError(f"Unsupported key method: {method}")

    def validate_encryption_key(self, key: str, method: str = "fernet") -> bool:
        """Validate an encryption key."""
        try:
            if method == "fernet":
                if CRYPTOGRAPHY_AVAILABLE:
                    Fernet(key.encode())
                    return True
                else:
                    return len(key) == 64  # 32 bytes hex
            elif method == "aes":
                return len(key) == 64  # 32 bytes hex
            return False
        except Exception:
            return False

    def integrate_aws_secrets_manager(self, secret_name: str) -> Dict[str, Any]:
        """Retrieve secrets from AWS Secrets Manager."""
        if not BOTO3_AVAILABLE:
            raise ImportError("boto3 required for AWS Secrets Manager integration")

        try:
            client = boto3.client('secretsmanager')
            response = client.get_secret_value(SecretName=secret_name)

            if 'SecretString' in response:
                secret = json.loads(response['SecretString'])
                return secret
            else:
                # Binary secret
                return {"binary_secret": base64.b64encode(response['SecretBinary']).decode()}

        except Exception as e:
            self.logger.exception(f"Failed to retrieve AWS secret {secret_name}: {e}")
            raise

    def integrate_azure_key_vault(self, vault_url: str, secret_name: str) -> str:
        """Retrieve secret from Azure Key Vault."""
        if not AZURE_AVAILABLE:
            raise ImportError("azure-identity and azure-keyvault-secrets required for Azure integration")

        try:
            credential = DefaultAzureCredential()
            client = SecretClient(vault_url=vault_url, credential=credential)

            secret = client.get_secret(secret_name)
            return secret.value

        except Exception as e:
            self.logger.exception(f"Failed to retrieve Azure secret {secret_name}: {e}")
            raise

    def integrate_hashicorp_vault(self, vault_addr: str, secret_path: str, token: Optional[str] = None) -> Dict[str, Any]:
        """Retrieve secrets from HashiCorp Vault."""
        if not HVAC_AVAILABLE:
            raise ImportError("hvac required for HashiCorp Vault integration")

        try:
            client = hvac.Client(url=vault_addr, token=token)

            if not client.is_authenticated():
                raise ValueError("Vault authentication failed")

            response = client.secrets.kv.v2.read_secret_version(path=secret_path)
            return response['data']['data']

        except Exception as e:
            self.logger.exception(f"Failed to retrieve Vault secret {secret_path}: {e}")
            raise

    def authenticate_user(self, username: str, password: str) -> Optional[User]:
        """Authenticate a user with username and password."""
        user = self._get_user_db(username)

        if not user or not user.is_active:
            return None

        # Verify password
        if self._verify_password(password, user.hashed_password):
            # Update last login
            self._update_user_login(user.user_id)

            # Log authentication event
            self.log_security_event("user_login", user.user_id, None, "authenticate", {})

            return user

        # Log failed authentication
        self.log_security_event("authentication_failed", None, None, "authenticate",
                              {"username": username})

        return None

    def authorize_action(self, user_id: str, resource: str, action: str) -> bool:
        """Check if user is authorized for an action on a resource."""
        user = self._get_user_by_id(user_id)

        if not user or not user.is_active:
            return False

        # Simple role-based authorization (can be extended)
        role_permissions = {
            "admin": ["*"],  # All permissions
            "developer": ["read", "write", "execute"],
            "analyst": ["read"],
            "viewer": ["read"]
        }

        user_permissions = role_permissions.get(user.role, [])

        # Check if user has permission
        authorized = "*" in user_permissions or action in user_permissions

        # Log authorization check
        self.log_security_event(
            "authorization_check",
            user_id,
            resource,
            action,
            {"authorized": authorized, "role": user.role}
        )

        return authorized

    def generate_api_token(self, user_id: str, expiry_days: int = 30) -> str:
        """Generate a JWT API token for a user."""
        if not JWT_AVAILABLE:
            raise ImportError("PyJWT required for token generation")

        expires_at = datetime.utcnow() + timedelta(days=expiry_days)

        payload = {
            "user_id": user_id,
            "exp": expires_at.timestamp(),
            "iat": datetime.utcnow().timestamp(),
            "iss": "etl_framework"
        }

        token = jwt.encode(payload, self.jwt_secret, algorithm="HS256")

        # Store token hash for revocation
        token_hash = hashlib.sha256(token.encode()).hexdigest()

        api_token = APIToken(
            token_id=str(uuid.uuid4()),
            user_id=user_id,
            token_hash=token_hash,
            expires_at=expires_at
        )

        self._store_api_token_db(api_token)

        self.logger.info(f"Generated API token for user {user_id}")
        return token

    def validate_api_token(self, token: str) -> Optional[str]:
        """Validate an API token and return user_id if valid."""
        if not JWT_AVAILABLE:
            raise ImportError("PyJWT required for token validation")

        try:
            # Decode token
            payload = jwt.decode(token, self.jwt_secret, algorithms=["HS256"])
            user_id = payload["user_id"]

            # Check if token is revoked
            token_hash = hashlib.sha256(token.encode()).hexdigest()
            if self._is_token_revoked(token_hash):
                return None

            # Check expiry
            exp_timestamp = payload.get("exp")
            if exp_timestamp and datetime.fromtimestamp(exp_timestamp) < datetime.utcnow():
                return None

            return user_id

        except jwt.ExpiredSignatureError:
            return None
        except jwt.InvalidTokenError:
            return None
        except Exception as e:
            self.logger.exception(f"Token validation error: {e}")
            return None

    def revoke_api_token(self, token: str) -> bool:
        """Revoke an API token."""
        try:
            token_hash = hashlib.sha256(token.encode()).hexdigest()
            return self._revoke_token_db(token_hash)
        except Exception as e:
            self.logger.exception(f"Token revocation error: {e}")
            return False

    def log_security_event(self, event_type: str, user_id: Optional[str],
                          resource: Optional[str], action: Optional[str],
                          details: Dict[str, Any], severity: str = "info") -> None:
        """Log a security audit event."""
        event = SecurityEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            user_id=user_id,
            resource=resource,
            action=action,
            details=details,
            timestamp=datetime.utcnow(),
            severity=severity
        )

        # Store in database
        self._store_security_event_db(event)

        # Log to application logger
        log_message = f"Security event: {event_type}"
        if user_id:
            log_message += f" user={user_id}"
        if resource:
            log_message += f" resource={resource}"
        if action:
            log_message += f" action={action}"

        if severity == "error" or severity == "critical":
            self.logger.error(log_message)
        elif severity == "warning":
            self.logger.warning(log_message)
        else:
            self.logger.info(log_message)

    def check_ssl_certificate(self, host: str, port: int = 443) -> Dict[str, Any]:
        """Check SSL certificate validity for a host."""
        if not SSL_AVAILABLE:
            raise ImportError("ssl module required for certificate checking")

        try:
            context = ssl.create_default_context()
            with socket.create_connection((host, port)) as sock:
                with context.wrap_socket(sock, server_hostname=host) as ssock:
                    cert = ssock.getpeercert()

                    # Extract certificate info
                    cert_info = {
                        "subject": dict(x[0] for x in cert.get("subject", [])),
                        "issuer": dict(x[0] for x in cert.get("issuer", [])),
                        "version": cert.get("version"),
                        "serial_number": str(cert.get("serialNumber", "")),
                        "not_before": cert.get("notBefore"),
                        "not_after": cert.get("notAfter"),
                        "valid": True
                    }

                    # Check expiry
                    not_after = ssl.cert_time_to_seconds(cert.get("notAfter"))
                    current_time = datetime.utcnow().timestamp()

                    if current_time > not_after:
                        cert_info["valid"] = False
                        cert_info["status"] = "expired"
                    elif current_time > not_after - (30 * 24 * 60 * 60):  # 30 days
                        cert_info["status"] = "expiring_soon"
                    else:
                        cert_info["status"] = "valid"

                    return cert_info

        except Exception as e:
            self.logger.exception(f"SSL certificate check failed for {host}:{port}: {e}")
            return {"valid": False, "error": str(e)}

    def get_secure_connection_string(self, conn_config: Dict[str, Any]) -> str:
        """Build a secure connection string with retrieved credentials."""
        conn_type = conn_config.get("type", "database")

        if conn_type == "database":
            # Retrieve password
            password = self.retrieve_credential(conn_config["connection_id"], "password")

            if not password:
                raise ValueError(f"No password found for connection {conn_config['connection_id']}")

            # Build connection string
            if conn_config.get("driver") == "postgresql":
                conn_str = f"postgresql://{conn_config['username']}:{password}@{conn_config['host']}:{conn_config.get('port', 5432)}/{conn_config['database']}"
            elif conn_config.get("driver") == "mysql":
                conn_str = f"mysql://{conn_config['username']}:{password}@{conn_config['host']}:{conn_config.get('port', 3306)}/{conn_config['database']}"
            elif conn_config.get("driver") == "oracle":
                conn_str = f"oracle://{conn_config['username']}:{password}@{conn_config['host']}:{conn_config.get('port', 1521)}/{conn_config['database']}"
            else:
                conn_str = conn_config.get("connection_string", "").replace("{password}", password)

            return conn_str

        elif conn_type == "api":
            # Retrieve API key
            api_key = self.retrieve_credential(conn_config["connection_id"], "api_key")

            if not api_key:
                raise ValueError(f"No API key found for connection {conn_config['connection_id']}")

            # Add to headers or query params
            base_url = conn_config["base_url"]
            auth_method = conn_config.get("auth_method", "header")

            if auth_method == "header":
                # Return URL and separate auth header
                return base_url  # Auth header would be added by caller
            elif auth_method == "query":
                separator = "&" if "?" in base_url else "?"
                return f"{base_url}{separator}api_key={api_key}"

        return conn_config.get("connection_string", "")

    def _hash_password(self, password: str) -> str:
        """Hash a password using PBKDF2."""
        salt = secrets.token_hex(16)
        hashed = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
        return f"{salt}:{hashed.hex()}"

    def _verify_password(self, password: str, hashed_password: str) -> bool:
        """Verify a password against its hash."""
        try:
            salt, hash_value = hashed_password.split(':')
            hashed = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
            return hmac.compare_digest(hashed.hex(), hash_value)
        except Exception:
            return False

    def _store_credential_db(self, credential: Credential) -> None:
        """Store credential in database."""
        if not self.db:
            return

        try:
            sql = """
                INSERT INTO CREDENTIALS (
                    credential_id, connection_id, credential_type, encrypted_value,
                    encryption_method, created_at, expires_at, last_rotated_at, rotation_policy
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            params = (
                credential.credential_id,
                credential.connection_id,
                credential.credential_type,
                credential.encrypted_value,
                credential.encryption_method,
                credential.created_at,
                credential.expires_at,
                credential.last_rotated_at,
                credential.rotation_policy
            )

            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Failed to store credential: {e}")

    def _get_credential_db(self, connection_id: str, credential_type: str) -> Optional[Credential]:
        """Retrieve credential from database."""
        if not self.db:
            return None

        try:
            sql = """
                SELECT credential_id, connection_id, credential_type, encrypted_value,
                       encryption_method, created_at, expires_at, last_rotated_at, rotation_policy
                FROM CREDENTIALS
                WHERE connection_id = %s AND credential_type = %s
                ORDER BY created_at DESC LIMIT 1
            """

            self.db.execute(sql, (connection_id, credential_type))
            row = self.db.fetchone()

            if row:
                return Credential(
                    credential_id=row[0],
                    connection_id=row[1],
                    credential_type=row[2],
                    encrypted_value=row[3],
                    encryption_method=row[4],
                    created_at=row[5],
                    expires_at=row[6],
                    last_rotated_at=row[7],
                    rotation_policy=row[8]
                )

        except Exception as e:
            self.logger.exception(f"Failed to get credential: {e}")

        return None

    def _update_credential_db(self, credential_id: str, encrypted_value: str) -> None:
        """Update credential in database."""
        if not self.db:
            return

        try:
            sql = """
                UPDATE CREDENTIALS
                SET encrypted_value = %s, last_rotated_at = %s
                WHERE credential_id = %s
            """

            params = (encrypted_value, datetime.utcnow(), credential_id)

            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Failed to update credential: {e}")

    def _get_user_db(self, username: str) -> Optional[User]:
        """Get user from database."""
        if not self.db:
            return None

        try:
            sql = "SELECT user_id, username, hashed_password, role, is_active, created_at, last_login FROM USERS WHERE username = %s"

            self.db.execute(sql, (username,))
            row = self.db.fetchone()

            if row:
                return User(
                    user_id=row[0],
                    username=row[1],
                    hashed_password=row[2],
                    role=row[3],
                    is_active=row[4],
                    created_at=row[5],
                    last_login=row[6]
                )

        except Exception as e:
            self.logger.exception(f"Failed to get user: {e}")

        return None

    def _get_user_by_id(self, user_id: str) -> Optional[User]:
        """Get user by ID from database."""
        if not self.db:
            return None

        try:
            sql = "SELECT user_id, username, hashed_password, role, is_active, created_at, last_login FROM USERS WHERE user_id = %s"

            self.db.execute(sql, (user_id,))
            row = self.db.fetchone()

            if row:
                return User(
                    user_id=row[0],
                    username=row[1],
                    hashed_password=row[2],
                    role=row[3],
                    is_active=row[4],
                    created_at=row[5],
                    last_login=row[6]
                )

        except Exception as e:
            self.logger.exception(f"Failed to get user by ID: {e}")

        return None

    def _update_user_login(self, user_id: str) -> None:
        """Update user's last login time."""
        if not self.db:
            return

        try:
            sql = "UPDATE USERS SET last_login = %s WHERE user_id = %s"

            self.db.begin()
            self.db.execute(sql, (datetime.utcnow(), user_id))
            self.db.commit()

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Failed to update user login: {e}")

    def _store_api_token_db(self, token: APIToken) -> None:
        """Store API token in database."""
        if not self.db:
            return

        try:
            sql = """
                INSERT INTO API_TOKENS (token_id, user_id, token_hash, expires_at, is_revoked, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """

            params = (
                token.token_id,
                token.user_id,
                token.token_hash,
                token.expires_at,
                token.is_revoked,
                token.created_at
            )

            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Failed to store API token: {e}")

    def _is_token_revoked(self, token_hash: str) -> bool:
        """Check if token is revoked."""
        if not self.db:
            return False

        try:
            sql = "SELECT is_revoked FROM API_TOKENS WHERE token_hash = %s"

            self.db.execute(sql, (token_hash,))
            row = self.db.fetchone()

            return row[0] if row else True

        except Exception:
            return True

    def _revoke_token_db(self, token_hash: str) -> bool:
        """Revoke token in database."""
        if not self.db:
            return False

        try:
            sql = "UPDATE API_TOKENS SET is_revoked = TRUE WHERE token_hash = %s"

            self.db.begin()
            self.db.execute(sql, (token_hash,))
            self.db.commit()

            return True

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Failed to revoke token: {e}")
            return False

    def _store_security_event_db(self, event: SecurityEvent) -> None:
        """Store security event in database."""
        if not self.db:
            return

        try:
            sql = """
                INSERT INTO SECURITY_EVENTS (
                    event_id, event_type, user_id, resource, action, details,
                    ip_address, user_agent, timestamp, severity
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            params = (
                event.event_id,
                event.event_type,
                event.user_id,
                event.resource,
                event.action,
                json.dumps(event.details),
                event.ip_address,
                event.user_agent,
                event.timestamp,
                event.severity
            )

            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Failed to store security event: {e}")