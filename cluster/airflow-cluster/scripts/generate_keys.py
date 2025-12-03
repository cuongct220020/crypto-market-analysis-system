from cryptography.fernet import Fernet
import secrets

print("FERNET_KEY=" + Fernet.generate_key().decode())
print("SECRET_KEY=" + secrets.token_hex(32))
