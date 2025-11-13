import requests  # type: ignore[import]
from loguru import logger

from app.core.config import settings


# todo: use aiohttp instead of request lib
def refresh_shared_secret() -> float | int | None:  # type: ignore
    tenant, stack, secret = settings.SHARED_SECRET.split("::")  # type: ignore[union-attr]
    url = f'https://{stack}.{settings.SERVER_DOMAIN}/sso/realms/{tenant}/protocol/openid-connect/token'
    data = {'client_id': 'offline', 'client_secret': secret, 'grant_type': 'client_credentials'}
    try:
        token_response = requests.post(
            url=url,
            data=data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
        ).json()
        settings.CUSTOMER_ACCESS_TOKEN = token_response.get('access_token')

        logger.info(f"Access Token for {tenant} is refreshed.")

        return token_response.get('expires_in') * 2 / 3  # type: ignore[no-any-return]
    except Exception as e:
        logger.error(e)
