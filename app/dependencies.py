"""FastAPI dependencies for authentication and authorization."""
from fastapi import Header, HTTPException, status
from typing import Annotated
import jwt
from app.schemas import JWTData
from app.utils.logger import logger


async def get_jwt_data(
    authorization: Annotated[str | None, Header()] = None
) -> JWTData:
    """
    Extract JWT data from Authorization header without validation.
    Gateway handles validation, we just extract the data.
    
    Args:
        authorization: Authorization header value
        
    Returns:
        JWTData: Extracted JWT data
        
    Raises:
        HTTPException: If token is missing or malformed
    """
    if not authorization:
        logger.warning("Missing Authorization header")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header"
        )
    
    # Extract token from "Bearer <token>"
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        logger.warning(f"Invalid Authorization header format: {authorization[:20]}...")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Authorization header format"
        )
    
    token = parts[1]
    
    try:
        # Decode without verification (gateway already validated)
        payload = jwt.decode(
            token,
            options={"verify_signature": False, "verify_exp": False}
        )
        
        jwt_data = JWTData(
            sub=payload.get("sub"),
            nickname=payload.get("nickname"),
            role=payload.get("role"),
            exp=payload.get("exp")
        )
        
        logger.info(f"JWT extracted for user: {jwt_data.sub} ({jwt_data.nickname})")
        return jwt_data
        
    except jwt.DecodeError as e:
        logger.error(f"Failed to decode JWT: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token format"
        )
    except Exception as e:
        logger.error(f"Unexpected error extracting JWT: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Failed to process token"
        )
