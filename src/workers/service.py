import asyncio
import time
from typing import Optional

from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from redis import Redis, RedisError

from configs.logger import logger
from configs.config import redis_settings
from schemas.message import Message, ServiceMessage, MessageType, TaskMessage
from services.bots import send_msg, edit_msg, del_msg
from utils.redis import (
    redis_conn,
    setup_stream,
    background_tasks,
    add_to_redis,
    get_from_redis,
    remove_from_redis,
    get_keys_by_prefix,
)

NUMBER_TO_READ_FROM_STREAM = 10
BLOCK_TIME = 2000

bot_commands = {
    MessageType.send_msg: send_msg,
    MessageType.edit_msg: edit_msg,
    MessageType.del_msg: del_msg,
}


async def restore_bot_consumers(
    redis_conn: Redis, exclude_keys: Optional[set[str]] = None
) -> None:
    bot_keys_restored = set()
    try:
        keys = await get_keys_by_prefix(
            redis_conn=redis_conn, prefix=redis_settings.TG_KEY_PREFIX
        )
        if not keys:
            return None
        for key in keys:
            if exclude_keys and key in exclude_keys:
                continue
            raw_token = await get_from_redis(redis_conn=redis_conn, key=key)
            if not raw_token:
                logger.exception(f"Cannot get bot token for key: {key}")
            token, is_sent_logs = raw_token.split(":LOGS:")
            await _add_bot(
                bot_id=key.split(redis_settings.TG_KEY_PREFIX)[1],
                token=token,
                bot_key=key,
                is_sent_logs=is_sent_logs == "True"
            )
            bot_keys_restored.add(key)
    except RedisError as ex:
        logger.info(f"Redis Error connection: {ex.args}, retrying..")
        if exclude_keys is None:
            exclude_keys = set(bot_keys_restored)
        else:
            for key in bot_keys_restored:
                exclude_keys.add(key)
        await asyncio.sleep(5)
        return await restore_bot_consumers(
            redis_conn=redis_conn, exclude_keys=exclude_keys
        )


async def add_bot(msg: Message) -> None:
    if not isinstance(msg.data, ServiceMessage):
        logger.exception(
            "Received service message in wrong format: %s",
            msg.data.model_dump_json(),
        )
        return
    msg: ServiceMessage = msg.data
    bot_key = f"{redis_settings.TG_KEY_PREFIX}{msg.bot_id}"
    try:
        if await get_from_redis(redis_conn=redis_conn, key=bot_key):
            logger.exception(
                "Bot is already activated: %s", msg.model_dump_json()
            )
            return
        await add_to_redis(
            redis_conn=redis_conn,
            key=bot_key,
            value=f"{msg.token}:LOGS:{bool(msg.is_sent_logs)}",
        )
    except RedisError as ex:
        logger.exception("Redis connection error, aborted operation %s", ex)
        return
    await _add_bot(
        bot_id=msg.bot_id,
        token=msg.token,
        bot_key=bot_key,
        is_sent_logs=msg.is_sent_logs,
    )


async def remove_bot(msg: Message) -> None:
    if not isinstance(msg.data, ServiceMessage):
        logger.exception(
            "Received service message in wrong format: %s",
            msg.data.model_dump_json(),
        )
        return
    msg: ServiceMessage = msg.data
    bot_key = f"{redis_settings.TG_KEY_PREFIX}{msg.bot_id}"
    task: asyncio.Task = background_tasks.pop(msg.bot_id, None)
    if task is None:
        logger.exception(f"Cannot find task for bot_id: {msg.bot_id}")
    else:
        task.cancel()
        logger.info(f"Cancelled task for bot_id: {msg.bot_id}")
    try:
        await remove_from_redis(redis_conn=redis_conn, key=bot_key)
    except RedisError as ex:
        logger.exception("Redis connection error, aborted operation %s", ex)
        return


async def _add_bot(
    bot_id: int, token: str, bot_key: str, is_sent_logs: Optional[bool] = None
) -> None:
    primary_stream = f"{redis_settings.TG_STREAM_PREFIX}{bot_id}"
    broadcast_stream = f"{redis_settings.TG_BROADCAST_STREAM_PREFIX}{bot_id}"
    logs_stream = None
    if is_sent_logs:
        logs_stream = f"{redis_settings.TG_BOT_LOG_STREAM_PREFIX}{bot_id}"
    bot = Bot(
        token=token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    try:
        await bot.get_me()
        task = asyncio.create_task(
            consume_bot(
                redis_conn=redis_conn,
                primary_stream=primary_stream,
                broadcast_stream=broadcast_stream,
                group_name=redis_settings.GROUP_NAME,
                consumer_name=str(bot_id),
                bot=bot,
                logs_stream=logs_stream,
            )
        )
        background_tasks[bot_id] = task

    except Exception as ex:
        logger.exception(
            "Cannot start bot with token %s: error: %s", token, ex
        )
        try:
            await remove_from_redis(redis_conn=redis_conn, key=bot_key)
        except RedisError as ex:
            logger.exception(
                "Redis connection error, aborted operation %s", ex
            )


async def consume_bot(
    redis_conn: Redis,
    primary_stream: str,
    broadcast_stream: str,
    group_name: str,
    consumer_name: str,
    bot: Bot,
    logs_stream: Optional[str] = None,
) -> None:
    await setup_stream(
        redis_conn=redis_conn,
        stream_name=primary_stream,
        group_name=group_name,
        consumer_id=consumer_name,
    )
    logger.info(f"Consumer for Bot Stream: {primary_stream} started")
    await setup_stream(
        redis_conn=redis_conn,
        stream_name=broadcast_stream,
        group_name=group_name,
        consumer_id=consumer_name,
    )
    logger.info(f"Consumer for Bot Stream: {broadcast_stream} started")
    if logs_stream:
        await setup_stream(
            redis_conn=redis_conn,
            stream_name=logs_stream,
            group_name=group_name,
            consumer_id=consumer_name,
        )
        logger.info(f"Consumer for Bot Stream: {logs_stream} started")
    last_reclaim_check = time.monotonic()
    while True:
        try:
            last_reclaim_check = await handle_pending_messages(
                redis_conn=redis_conn,
                primary_stream=primary_stream,
                broadcast_stream=broadcast_stream,
                logs_stream=logs_stream,
                group_name=group_name,
                consumer_name=consumer_name,
                bot=bot,
                last_reclaim_check=last_reclaim_check,
            )
            await handle_new_messages(
                redis_conn=redis_conn,
                stream_name=primary_stream,
                logs_stream=logs_stream,
                group_name=group_name,
                consumer_name=consumer_name,
                bot=bot,
                is_blocked=True,
            )
            await handle_new_messages(
                redis_conn=redis_conn,
                stream_name=broadcast_stream,
                logs_stream=logs_stream,
                group_name=group_name,
                consumer_name=consumer_name,
                bot=bot,
            )
        except asyncio.CancelledError:
            logger.info(f"Consumer {consumer_name} shutting down...")
            return
        except Exception as e:
            logger.exception(f"Error in {consumer_name}: {e}")
            await asyncio.sleep(1)


async def handle_pending_messages(
    redis_conn: Redis,
    primary_stream: str,
    broadcast_stream: str,
    group_name: str,
    consumer_name: str,
    bot: Bot,
    last_reclaim_check: float,
    logs_stream: Optional[str] = None,
) -> None:
    now = time.monotonic()
    if now - last_reclaim_check >= redis_settings.RECLAIM_INTERVAL_SECONDS:
        logger.info(f"Consumer: {consumer_name} Running reclaim check")
        last_reclaim_check = now
        await handle_pending_messages_for_stream(
            redis_conn=redis_conn,
            stream_name=primary_stream,
            group_name=group_name,
            consumer_name=consumer_name,
            bot=bot,
            logs_stream=logs_stream,
        )
        await handle_pending_messages_for_stream(
            redis_conn=redis_conn,
            stream_name=broadcast_stream,
            group_name=group_name,
            consumer_name=consumer_name,
            bot=bot,
            logs_stream=logs_stream,
        )

    return last_reclaim_check


async def handle_pending_messages_for_stream(
    redis_conn: Redis,
    stream_name: str,
    group_name: str,
    consumer_name: str,
    bot: Bot,
    logs_stream: Optional[str] = None,
) -> None:
    pending_messages = await redis_conn.xpending_range(
        name=stream_name,
        groupname=group_name,
        min="-",
        max="+",
        count=redis_settings.MAX_PENDING_TO_SCAN,
    )
    stuck_ids_to_claim = []
    for message in pending_messages:
        logger.info(pending_messages)
        if message["time_since_delivered"] > redis_settings.IDLE_THRESHOLD_MS:
            logger.info(
                f"[{consumer_name}] - found stuck message"
                f" {message['message_id']} "
                f"(Idle: {message['time_since_delivered']})"
            )
            stuck_ids_to_claim.append(message["message_id"])
    if stuck_ids_to_claim:
        claimed = await redis_conn.xclaim(
            name=stream_name,
            groupname=group_name,
            consumername=consumer_name,
            min_idle_time=redis_settings.IDLE_THRESHOLD_MS,
            message_ids=stuck_ids_to_claim,
            idle=0,
            justid=True,
        )
        logger.info(
            f"[{consumer_name}] - claimed stuck messages: {len(claimed)}"
        )
        messages = await redis_conn.xreadgroup(
            groupname=group_name,
            consumername=consumer_name,
            streams={stream_name: "0"},
            count=10,
        )
        for stream, entries in messages:
            for message_id, data in entries:
                if isinstance(data, dict):
                    logger.info(
                        f"[{stream}] {consumer_name} got: {str(data)[:90]}"
                    )
                    await handle_bot_message(
                        msg=data, bot=bot, logs_stream=logs_stream
                    )
                else:
                    logger.exception(f"[{stream}] {consumer_name} got: {data}")
                await redis_conn.xack(stream_name, group_name, message_id)


async def handle_new_messages(
    redis_conn: Redis,
    stream_name: str,
    group_name: str,
    consumer_name: str,
    bot: Bot,
    is_blocked: bool | None = False,
    logs_stream: Optional[str] = None,
) -> None:
    messages = await redis_conn.xreadgroup(
        groupname=group_name,
        consumername=consumer_name,
        streams={stream_name: ">"},
        count=NUMBER_TO_READ_FROM_STREAM,
        block=BLOCK_TIME if is_blocked else None,
    )
    for stream, entries in messages:
        for message_id, data in entries:
            if isinstance(data, dict):
                logger.info(f"[{stream}] {consumer_name} got: {data}")
                await handle_bot_message(
                    msg=data, bot=bot, logs_stream=logs_stream
                )
            else:
                logger.exception(f"[{stream}] {consumer_name} got: {data}")
            await redis_conn.xack(stream_name, group_name, message_id)


async def handle_bot_message(
    msg: dict,
    bot: Bot,
    logs_stream: Optional[str] = None,
) -> None:
    try:
        msg: Message = Message(**msg)
    except TypeError as ex:
        logger.exception(
            f"Bot Stream received unsupported message: {msg} with {ex}"
        )
        return None
    if not isinstance(msg.data, TaskMessage):
        logger.exception(
            f"Bot Stream received unsupported message data: {msg}"
        )
        return None
    if msg.type not in bot_commands:
        logger.exception(f"Bot Stream received unsupported message: {msg}")
    else:
        if not await validate_task_message(msg):
            logger.exception(f"Bot received unsupported message: {msg}")
            return None
        return await bot_commands[msg.type](
            msg=msg, bot=bot, logs_stream=logs_stream
        )


async def validate_task_message(msg: Message) -> bool:
    if (  # noqa: SIM103
        msg.type in (MessageType.del_msg, MessageType.edit_msg)
        and msg.data.message_id is None
    ):
        return False
    return True
