import secrets
from base64 import urlsafe_b64decode as b64d
from base64 import urlsafe_b64encode as b64e
from typing import Union

from cryptography.fernet import Fernet  # type: ignore
from cryptography.hazmat.backends import default_backend  # type: ignore
from cryptography.hazmat.primitives import hashes  # type: ignore
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC  # type: ignore

from app.core.config import settings


def _derive_key(
    secret_token: bytes,
    salt: bytes,
    iterations: int = settings.ENCRYPT_ITERATIONS,
) -> bytes:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=iterations,
        backend=default_backend(),
    )
    return b64e(kdf.derive(secret_token))


def password_encrypt(
    password: Union[bytes, str],
    secret_token: Union[bytes, str] = settings.SECRET_TOKEN,
    iterations: int = settings.ENCRYPT_ITERATIONS,
) -> bytes:
    if isinstance(password, str):
        password = password.encode(settings.DEFAULT_ENCODING)
    salt = secrets.token_bytes(16)
    secret_token = secret_token.encode(settings.DEFAULT_ENCODING)  # type: ignore
    key = _derive_key(secret_token, salt, iterations)
    return b64e(
        b'%b%b%b'
        % (
            salt,
            iterations.to_bytes(4, 'big'),
            b64d(Fernet(key).encrypt(password)),
        )
    )


def password_decrypt_to_bytes(
    encrypted_password: Union[bytes, str],
    secret_token: Union[bytes, str] = settings.SECRET_TOKEN,
) -> bytes:
    if isinstance(encrypted_password, str):
        encrypted_password = encrypted_password.encode(settings.DEFAULT_ENCODING)
    decoded = b64d(encrypted_password)
    salt, _iter, token = decoded[:16], decoded[16:20], b64e(decoded[20:])
    iterations = int.from_bytes(_iter, 'big')
    key = _derive_key(secret_token.encode(), salt, iterations)  # type: ignore
    return Fernet(key).decrypt(token)  # type: ignore


def password_decrypt_to_str(
    encrypted_password: Union[bytes, str],
    secret_token: Union[bytes, str] = settings.SECRET_TOKEN,
) -> str:
    return password_decrypt_to_bytes(encrypted_password, secret_token).decode(settings.DEFAULT_ENCODING)


def deserialize_private_key(private_key_file_data: bytes) -> bytes:
    p_key = serialization.load_pem_private_key(private_key_file_data, password=None, backend=default_backend())
    return p_key.private_bytes(  # type: ignore
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def convert_private_key(private_key: str) -> bytes:
    if 'BEGIN PRIVATE KEY' not in private_key or 'END PRIVATE KEY' not in private_key:
        raise ValueError(
            'Private key must start with "-----BEGIN PRIVATE KEY-----" sentence '
            'and end with "-----END PRIVATE KEY-----"'
        )
    private_key = private_key.replace('\n', '')
    private_key = private_key[:27] + '\n' + private_key[27:-25] + '\n' + private_key[-25:]
    return deserialize_private_key(private_key.encode(settings.DEFAULT_ENCODING))
