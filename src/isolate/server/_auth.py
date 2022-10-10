import jwt

ALGORITHM = 'HS256'

def create_auth_token(user_key: str, secret_key: str) -> str:
    # Encode user_key in a JWT
    return jwt.encode({'key': user_key}, secret_key, ALGORITHM)

def validate_auth_token(token: str, user_key: str, secret_key: str) -> bool:
    # Decode user_key from JWT
    try:
        decoded = jwt.decode(token, secret_key, algorithms=[ALGORITHM])
    except jwt.PyJWTError:
        return False
    else:
        return user_key == decoded.get('key')
