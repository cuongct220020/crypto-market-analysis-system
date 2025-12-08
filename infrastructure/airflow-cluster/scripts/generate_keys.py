import secrets

from cryptography.fernet import Fernet

print("FERNET_KEY=" + Fernet.generate_key().decode())
print("SECRET_KEY=" + secrets.token_hex(32))
