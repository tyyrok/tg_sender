from fastapi import HTTPException, status

from configs.config import app_settings


async def verify_user(api_token: str) -> None:
    if api_token != app_settings.DUMMY_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized"
        ) from None
