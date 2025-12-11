import asyncio
import time

from redis import asyncio as aioredis
from pydantic import ValidationError

from configs.config import redis_settings
from configs.logger import logger
from schemas.message import Message, MessageType
from workers.service import add_bot, remove_bot, restore_bot_consumers
from utils.redis import setup_stream, redis_conn

MAX_MSG_TO_PROCESS = 2
MAX_READ_BLOCK_TIME = 2000
CONSUMER_NAME = "CONTROLLER"

background_tasks = set()

service_commands = {
    "add_bot": add_bot,
    "remove_bot": remove_bot,
}


async def run_consumers() -> None:
    await setup_stream(
        redis_conn=redis_conn,
        stream_name=redis_settings.CONTROL_STREAM_NAME,
        group_name=redis_settings.GROUP_NAME,
    )
    await add_consumer(
        redis_conn=redis_conn,
        stream_name=redis_settings.CONTROL_STREAM_NAME,
        group_name=redis_settings.GROUP_NAME,
        consumer_name=CONSUMER_NAME,
    )
    await restore_bot_consumers(
        redis_conn=redis_conn,
    )
    try:
        await asyncio.Event().wait()
    except Exception as ex:
        logger.exception(ex)
    finally:
        for t in background_tasks:
            t.cancel()
        await asyncio.gather(*background_tasks, return_exceptions=True)


async def add_consumer(
    redis_conn: aioredis.Redis,
    stream_name: str,
    group_name: str,
    consumer_name: str,
) -> None:
    task = asyncio.create_task(
        consume_service(
            redis_conn=redis_conn,
            stream_name=stream_name,
            group_name=group_name,
            consumer_name=consumer_name,
        )
    )
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)


async def consume_service(
    redis_conn: aioredis.Redis,
    stream_name: str,
    group_name: str,
    consumer_name: str,
) -> None:
    logger.info(f"Consumer: {consumer_name} for stream: {stream_name} started")
    last_reclaim_check = time.monotonic()
    while True:
        try:
            last_reclaim_check = await handle_pending_messages(
                redis_conn=redis_conn,
                stream_name=stream_name,
                group_name=group_name,
                consumer_name=consumer_name,
                last_reclaim_check=last_reclaim_check,
            )
            messages = await redis_conn.xreadgroup(
                groupname=group_name,
                consumername=consumer_name,
                streams={stream_name: ">"},
                count=MAX_MSG_TO_PROCESS,
                block=MAX_READ_BLOCK_TIME,
            )
            for stream, entries in messages:
                for message_id, data in entries:
                    if isinstance(data, dict):
                        logger.info(
                            f"[{stream}] {consumer_name} got: {str(data)[:90]}"
                        )
                        await handle_incoming_service_message(data)
                    else:
                        logger.exception(
                            f"[{stream}] {consumer_name} got: {data}"
                        )
                    await redis_conn.xack(stream_name, group_name, message_id)
        except asyncio.CancelledError:
            logger.info(f"{consumer_name} shutting down...")
            break
        except Exception as e:
            logger.exception(f"Error in {consumer_name}: {e}")
            await asyncio.sleep(1)


async def handle_incoming_service_message(msg: dict) -> None:
    try:
        msg: Message = Message.model_validate(msg)
    except ValidationError:
        logger.exception(
            f"OUTCOME STREAM - CONSUMER: {CONSUMER_NAME}"
            f" received unsupported message: {msg}"
        )
        return

    if msg.type == MessageType.pulse:
        logger.info(f"Ping message received: {CONSUMER_NAME}")
        return
    if msg.type.value in service_commands:
        await service_commands[msg.type.value](msg)
    else:
        logger.exception(
            f"OUTCOME STREAM - CONSUMER: {CONSUMER_NAME}"
            f" received unsupported command: {msg}"
        )


async def handle_pending_messages(
    redis_conn: aioredis.Redis,
    stream_name: str,
    group_name: str,
    consumer_name: str,
    last_reclaim_check: float,
) -> float:
    now = time.monotonic()
    if now - last_reclaim_check >= redis_settings.RECLAIM_INTERVAL_SECONDS:
        logger.info(f"Consumer: {consumer_name} Running reclaim check")
        last_reclaim_check = now
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
            if (
                message["time_since_delivered"]
                > redis_settings.IDLE_THRESHOLD_MS
            ):
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
                        await handle_incoming_service_message(data)
                    else:
                        logger.exception(
                            f"[{stream}] {consumer_name} got: {data}"
                        )
                    await redis_conn.xack(stream_name, group_name, message_id)

    return last_reclaim_check
