#!/usr/bin/env python3
"""
Security Manager Example
Demonstrates the usage of the SecurityManager class for credential management,
encryption, authentication, and security features.
"""

import logging
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from utils.security_manager import SecurityManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def demonstrate_encryption():
    """Demonstrate encryption/decryption functionality."""
    print("\n=== Encryption/Decryption Demo ===")

    security = SecurityManager()

    # Test Fernet encryption
    plaintext = "my_secret_password_123"
    print(f"Original: {plaintext}")

    encrypted = security.encrypt_credential(plaintext, "fernet")
    print(f"Encrypted (Fernet): {encrypted}")

    decrypted = security.decrypt_credential(encrypted, "fernet")
    print(f"Decrypted: {decrypted}")
    print(f"Match: {plaintext == decrypted}")

    # Test AES encryption
    encrypted_aes = security.encrypt_credential(plaintext, "aes")
    print(f"Encrypted (AES): {encrypted_aes}")

    decrypted_aes = security.decrypt_credential(encrypted_aes, "aes")
    print(f"Decrypted (AES): {decrypted_aes}")
    print(f"Match: {plaintext == decrypted_aes}")


def demonstrate_credential_management():
    """Demonstrate credential storage and retrieval."""
    print("\n=== Credential Management Demo ===")

    # Note: This would normally use a real database connection
    # For demo purposes, we'll show the interface
    security = SecurityManager(db_connection=None)

    connection_id = "test_db_connection"
    credential_value = "super_secret_db_password"

    # Store credential
    print(f"Storing credential for connection: {connection_id}")
    cred_id = security.store_credential(
        connection_id=connection_id,
        credential_type="password",
        credential_value=credential_value,
        encryption_method="fernet",
        expires_days=90
    )
    print(f"Credential stored with ID: {cred_id}")

    # Retrieve credential
    print("Retrieving credential...")
    retrieved = security.retrieve_credential(connection_id, "password")
    if retrieved:
        print(f"Retrieved credential: {retrieved}")
        print(f"Match: {credential_value == retrieved}")
    else:
        print("Credential not found (expected in demo without DB)")

    # Rotate credential
    print("Rotating credential...")
    new_password = "new_super_secret_password_456"
    rotated = security.rotate_credential(connection_id, "password", new_password)
    print(f"Rotation successful: {rotated}")


def demonstrate_api_token_management():
    """Demonstrate API token generation and validation."""
    print("\n=== API Token Management Demo ===")

    security = SecurityManager()

    user_id = "user_123"

    # Check if JWT is available
    try:
        # Generate token
        print(f"Generating API token for user: {user_id}")
        token = security.generate_api_token(user_id, expiry_days=7)
        print(f"Generated token: {token[:50]}...")

        # Validate token
        print("Validating token...")
        validated_user = security.validate_api_token(token)
        print(f"Token valid for user: {validated_user}")
        print(f"Match: {user_id == validated_user}")

        # Revoke token
        print("Revoking token...")
        revoked = security.revoke_api_token(token)
        print(f"Revocation successful: {revoked}")

        # Try to validate revoked token
        print("Validating revoked token...")
        validated_after_revoke = security.validate_api_token(token)
        print(f"Token still valid after revoke: {validated_after_revoke is not None}")

    except ImportError as e:
        print(f"JWT functionality not available: {e}")
        print("Install PyJWT to enable API token management")


def demonstrate_authentication():
    """Demonstrate user authentication."""
    print("\n=== Authentication Demo ===")

    security = SecurityManager()

    # Note: This would normally create users in the database
    # For demo, we'll show the interface
    print("Authentication requires database setup for full demo")
    print("Interface methods available:")
    print("- authenticate_user(username, password)")
    print("- authorize_action(user_id, resource, action)")


def demonstrate_ssl_certificate_check():
    """Demonstrate SSL certificate validation."""
    print("\n=== SSL Certificate Check Demo ===")

    security = SecurityManager()

    # Check SSL certificate for a well-known site
    host = "www.google.com"
    port = 443

    print(f"Checking SSL certificate for {host}:{port}")
    cert_info = security.check_ssl_certificate(host, port)

    print(f"Certificate valid: {cert_info.get('valid', False)}")
    print(f"Status: {cert_info.get('status', 'unknown')}")
    print(f"Subject: {cert_info.get('subject', {})}")
    print(f"Issuer: {cert_info.get('issuer', {})}")
    print(f"Valid until: {cert_info.get('not_after', 'unknown')}")


def demonstrate_connection_string_building():
    """Demonstrate secure connection string building."""
    print("\n=== Secure Connection String Demo ===")

    security = SecurityManager()

    # Example database configuration
    db_config = {
        "type": "database",
        "connection_id": "demo_db",
        "driver": "postgresql",
        "host": "localhost",
        "port": 5432,
        "database": "etl_db",
        "username": "etl_user"
    }

    print("Building connection string...")
    print("Note: This would retrieve the actual password from secure storage")
    print("Config:", db_config)

    # This would normally work with stored credentials
    try:
        conn_str = security.get_secure_connection_string(db_config)
        print(f"Connection string: {conn_str}")
    except ValueError as e:
        print(f"Expected error (no stored credential): {e}")


def demonstrate_key_generation():
    """Demonstrate encryption key generation and validation."""
    print("\n=== Key Generation Demo ===")

    security = SecurityManager()

    # Generate Fernet key
    fernet_key = security.generate_encryption_key("fernet")
    print(f"Generated Fernet key: {fernet_key[:20]}...")

    # Validate key
    is_valid = security.validate_encryption_key(fernet_key, "fernet")
    print(f"Fernet key valid: {is_valid}")

    # Generate AES key
    aes_key = security.generate_encryption_key("aes")
    print(f"Generated AES key: {aes_key[:20]}...")

    # Validate AES key
    is_valid_aes = security.validate_encryption_key(aes_key, "aes")
    print(f"AES key valid: {is_valid_aes}")


def main():
    """Run all security manager demonstrations."""
    print("Security Manager Demonstration")
    print("=" * 50)

    try:
        demonstrate_encryption()
        demonstrate_credential_management()
        demonstrate_api_token_management()
        demonstrate_authentication()
        demonstrate_ssl_certificate_check()
        demonstrate_connection_string_building()
        demonstrate_key_generation()

        print("\n=== Demo Complete ===")
        print("SecurityManager provides comprehensive security features:")
        print("✓ Multiple encryption methods (Fernet, AES)")
        print("✓ Secure credential storage and rotation")
        print("✓ API token management with JWT")
        print("✓ User authentication and authorization")
        print("✓ SSL certificate validation")
        print("✓ Security event auditing")
        print("✓ Integration with cloud secret managers")

    except Exception as e:
        logger.exception(f"Demo failed: {e}")
        print(f"Demo failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())