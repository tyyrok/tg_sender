import asyncio
import time

from redis import RedisError

from configs.logger import logger
from configs.config import redis_settings, telegram_settings as tg_settings
from services.global_limiter import global_limiter
from utils.redis import add_to_redis, get_from_redis, redis_conn


class TelegramRateLimiter:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()

    async def acquire_lock(
        self, chat_id: int | str, bot_id: int | str
    ) -> bool:
        if str(chat_id).startswith("-"):
            return await self._acquire_group_lock(
                chat_id=chat_id, bot_id=bot_id
            )
        return await self._acquire_lock(chat_id=chat_id, bot_id=bot_id)

    async def _acquire_lock(
        self, chat_id: int | str, bot_id: int | str
    ) -> bool:
        await global_limiter.acquire_lock(bot_id)
        async with self.lock:
            now = time.monotonic()
            redis_key = f"{redis_settings.CHAT_SEND_PREFIX}{chat_id}:{bot_id}"
            try:
                if last_chat_send := await get_from_redis(
                    redis_conn=redis_conn, key=redis_key
                ):
                    time_since_last_send = now - float(last_chat_send)
                    required_to_wait = (
                        tg_settings.PER_CHAT_DELAY - time_since_last_send
                    )
                    if required_to_wait > 0:
                        await asyncio.sleep(required_to_wait)
                await add_to_redis(
                    redis_conn=redis_conn,
                    key=redis_key,
                    value=time.monotonic(),
                    ttl=int(tg_settings.PER_CHAT_DELAY),
                )
            except RedisError as ex:
                logger.exception(f"Redis connection error: {ex.args}")
                raise
        return True

    async def acquire_edit_lock(
        self, chat_id: int | str, bot_id: int | str
    ) -> bool:
        await global_limiter.acquire_lock(bot_id)
        async with self.lock:
            now = time.monotonic()
            redis_key = f"{redis_settings.CHAT_EDIT_PREFIX}{chat_id}:{bot_id}"
            try:
                if last_chat_send := await get_from_redis(
                    redis_conn=redis_conn, key=redis_key
                ):
                    time_since_last_send = now - float(last_chat_send)
                    required_to_wait = (
                        tg_settings.PER_CHAT_EDIT_DELAY - time_since_last_send
                    )
                    if required_to_wait > 0:
                        await asyncio.sleep(required_to_wait)
                await add_to_redis(
                    redis_conn=redis_conn,
                    key=redis_key,
                    value=time.monotonic(),
                    ttl=int(tg_settings.PER_CHAT_EDIT_DELAY),
                )
            except RedisError as ex:
                logger.exception(f"Redis connection error: {ex.args}")
                raise
        return True

    async def _acquire_group_lock(
        self, chat_id: int | str, bot_id: int | str
    ) -> bool:
        await global_limiter.acquire_lock(bot_id)
        async with self.lock:
            now = time.monotonic()
            redis_key = f"{redis_settings.GROUP_SEND_PREFIX}{chat_id}:{bot_id}"
            try:
                if last_chat_send := await get_from_redis(
                    redis_conn=redis_conn, key=redis_key
                ):
                    time_since_last_send = now - float(last_chat_send)
                    required_to_wait = (
                        tg_settings.PER_GROUP_MSG_DELAY - time_since_last_send
                    )
                    if required_to_wait > 0:
                        await asyncio.sleep(required_to_wait)
                await add_to_redis(
                    redis_conn=redis_conn,
                    key=redis_key,
                    value=time.monotonic(),
                    ttl=int(tg_settings.PER_GROUP_MSG_DELAY),
                )
            except RedisError as ex:
                logger.exception(f"Redis connection error: {ex.args}")
                raise
        return True


rate_limiter = TelegramRateLimiter()
