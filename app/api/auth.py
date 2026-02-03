from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from jose import JWTError, jwt
from typing import Optional
import secrets

from app.db.session import get_db
from app.config import settings
from app.models.user import User
from app.schemas.user import UserCreate, UserResponse, TokenResponse

router = APIRouter()

# OAuth2 scheme for token extraction
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/token", auto_error=False)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Create JWT access token"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt


def get_or_create_user(email: str, name: Optional[str], is_guest: bool, db: Session) -> User:
    """Get existing user or create new one"""
    user = db.query(User).filter(User.email == email).first()
    if not user:
        user = User(email=email, name=name, is_guest=is_guest)
        db.add(user)
        db.commit()
        db.refresh(user)
    return user


# Dependency for authentication
async def get_current_user(
    db: Session = Depends(get_db),
    token: str = Depends(oauth2_scheme)
) -> User:
    """
    Validate JWT token and return current user.
    Works for both OAuth and guest users.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        user_id: str = payload.get("sub")
        if user_id is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    
    user = db.query(User).filter(User.id == user_id).first()
    if user is None:
        raise credentials_exception
    
    return user


@router.post("/guest", response_model=TokenResponse)
async def create_guest_session(db: Session = Depends(get_db)):
    """
    Create a guest user session without authentication.
    Guest email format: guest_{timestamp}_{random}@c2l.com
    """
    timestamp = int(datetime.utcnow().timestamp())
    random_suffix = secrets.token_hex(4)
    guest_email = f"guest_{timestamp}_{random_suffix}@c2l.com"
    
    user = get_or_create_user(
        email=guest_email,
        name="Guest User",
        is_guest=True,
        db=db
    )
    
    # Create access token
    access_token = create_access_token(data={"sub": str(user.id), "email": user.email})
    
    return TokenResponse(
        access_token=access_token,
        user=UserResponse.model_validate(user)
    )


@router.get("/google")
async def google_login():
    """
    Redirect to Google OAuth login.
    TODO: Implement OAuth flow in Phase 2.
    """
    if not settings.GOOGLE_CLIENT_ID:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Google OAuth not configured. Use guest mode instead."
        )
    
    # OAuth flow will be implemented here
    return {
        "message": "Google OAuth login",
        "redirect_url": "https://accounts.google.com/o/oauth2/v2/auth",
        "client_id": settings.GOOGLE_CLIENT_ID
    }


@router.get("/callback")
async def google_callback(code: str, db: Session = Depends(get_db)):
    """
    Google OAuth callback handler.
    TODO: Implement OAuth flow in Phase 2.
    """
    if not settings.GOOGLE_CLIENT_ID:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Google OAuth not configured"
        )
    
    # TODO: Exchange code for tokens
    # TODO: Get user info from Google
    # TODO: Create or update user
    # TODO: Generate JWT token
    
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Google OAuth callback not yet implemented"
    )


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(
    current_user: User = Depends(get_current_user)
):
    """Get current authenticated user"""
    return UserResponse.model_validate(current_user)
