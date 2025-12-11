from typing import Optional, Union

from redis import asyncio as aioredis
from redis.asyncio import RedisError

from configs.config import redis_settings
from configs.logger import logger

background_tasks = {}


redis_conn: aioredis.Redis = aioredis.Redis(
    host=redis_settings.REDIS_HOST,
    port=redis_settings.REDIS_PORT,
    auto_close_connection_pool=False,
    decode_responses=True,
)


async def setup_stream(
    redis_conn: aioredis.Redis,
    stream_name: str,
    group_name: str,
    consumer_id: str | None = "0",
) -> None:
    try:
        await redis_conn.xgroup_create(
            name=stream_name,
            groupname=group_name,
            id=consumer_id,
            mkstream=True,
        )
        logger.info(f"BOT: Initialized Redis Stream: {stream_name}")
    except aioredis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logger.info(f"BOT GROUP: {group_name} already exists")
        else:
            logger.exception(e)
            raise


async def add_to_redis(
    redis_conn: aioredis.Redis, key: str, value: str, ttl: int | None = None
) -> None:
    try:
        await redis_conn.set(key, value, ex=ttl)
    except RedisError as ex:
        logger.error(ex)
        raise


async def get_from_redis(
    redis_conn: aioredis.Redis, key: str
) -> Union[str, None]:
    try:
        return await redis_conn.get(key)
    except RedisError as ex:
        logger.error(ex)
        raise


async def remove_from_redis(
    redis_conn: aioredis.Redis, key: str
) -> Union[str, None]:
    try:
        return await redis_conn.delete(key)
    except RedisError as ex:
        logger.error(ex)
        raise


async def get_keys_by_prefix(
    redis_conn: aioredis.Redis,
    prefix: Optional[str] = None,
    suffix: Optional[str] = None,
) -> list[str]:
    keys = []
    if suffix:
        async for key in redis_conn.scan_iter(match=f"*{suffix}"):
            keys.append(key)  # noqa: PERF401
    if prefix:
        async for key in redis_conn.scan_iter(match=f"{prefix}*"):
            keys.append(key)  # noqa: PERF401
    return keys
