import json

from configs.logger import logger
from schemas.message import Message, LogMessage
from utils.redis import redis_conn


async def send_to_queueu(
    msg: Message | LogMessage | dict,
    stream_name: str,
    w_raise: bool | None = False,
) -> None:
    try:
        if isinstance(msg, Message):
            msg = msg.model_dump(exclude_unset=True)
            msg["data"] = json.dumps(msg["data"])
        if isinstance(msg, LogMessage):
            msg = msg.model_dump(exclude_unset=True, exclude_none=True)
            if "reply_markup" in msg:
                msg["reply_markup"] = json.dumps(msg["reply_markup"])
        logger.info(msg)
        await redis_conn.xadd(
            name=stream_name,
            fields=msg,
        )
        logger.info(f"[TASK] MSG Sent:{msg}")
    except Exception as ex:
        logger.exception(ex)
        if w_raise:
            raise
