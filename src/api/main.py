from typing import Optional

from fastapi import FastAPI, status, Depends

from api.dependencies import verify_user
from configs.config import redis_settings
from schemas.message import (
    Message,
    MessageType,
    ServiceMessage,
    TaskMessage,
    ReplyMarkup,
)
from workers.producers import send_to_queueu


app = FastAPI()


@app.post(
    "/add",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_user)],
)
async def add_bot(bot_id: int, token: str, is_sent_logs: bool = False):
    await send_to_queueu(
        msg=Message(
            type=MessageType.add_bot,
            data=ServiceMessage(
                bot_id=bot_id, token=token, is_sent_logs=is_sent_logs
            ),
        ),
        stream_name=redis_settings.CONTROL_STREAM_NAME,
    )


@app.delete(
    "/remove",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_user)],
)
async def remove_bot(bot_id: int, token: str):
    await send_to_queueu(
        msg=Message(
            type=MessageType.remove_bot,
            data=ServiceMessage(bot_id=bot_id, token=token),
        ),
        stream_name=redis_settings.CONTROL_STREAM_NAME,
    )


@app.post(
    "/send_msg",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_user)],
)
async def send_msg(
    bot_id: int,
    chat_id: int,
    text: str,
    reply_markup: ReplyMarkup | None = None,
    reply_to_message_id: int | str | None = None,
):
    await send_to_queueu(
        msg=Message(
            type=MessageType.send_msg,
            data=TaskMessage(
                bot_id=bot_id,
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup,
                reply_to_message_id=reply_to_message_id,
            ),
        ),
        stream_name=f"{redis_settings.TG_STREAM_PREFIX}{bot_id}",
    )


@app.post(
    "/send_multi_msg",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_user)],
)
async def send_mass_msg(
    bot_id: int,
    chat_id: int,
    text: str,
    reply_markup: ReplyMarkup | None = None,
    reply_to_message_id: int | str | None = None,
):
    for i in range(30):
        await send_to_queueu(
            msg=Message(
                type=MessageType.send_msg,
                data=TaskMessage(
                    bot_id=bot_id,
                    chat_id=chat_id,
                    text=f"Report N:{i} - {text}",
                    reply_markup=reply_markup,
                    reply_to_message_id=reply_to_message_id,
                ),
            ),
            stream_name=f"{redis_settings.TG_STREAM_PREFIX}{bot_id}",
        )


@app.post(
    "/broadcast",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_user)],
)
async def send_broadcast(
    bot_id: int,
    chat_id: int,
    text: str,
    reply_markup: ReplyMarkup | None = None,
    reply_to_message_id: int | str | None = None,
):
    await send_to_queueu(
        msg=Message(
            type=MessageType.send_msg,
            data=TaskMessage(
                bot_id=bot_id,
                chat_id=chat_id,
                text=f"Broadcast MEssage - {text}",
                reply_markup=reply_markup,
                reply_to_message_id=reply_to_message_id,
            ),
        ),
        stream_name=f"{redis_settings.TG_BROADCAST_STREAM_PREFIX}{bot_id}",
    )


@app.delete(
    "/msg",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_user)],
)
async def remove_msg(
    bot_id: int,
    chat_id: int,
    msg_id: int,
):
    await send_to_queueu(
        msg=Message(
            type=MessageType.del_msg,
            data=TaskMessage(
                bot_id=bot_id, chat_id=chat_id, message_id=msg_id
            ),
        ),
        stream_name=f"{redis_settings.TG_BROADCAST_STREAM_PREFIX}{bot_id}",
    )


@app.patch(
    "/msg",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_user)],
)
async def update_msg(
    bot_id: int,
    chat_id: int,
    msg_id: int,
    text: Optional[str] = None,
    reply_markup: ReplyMarkup | None = None,
    reply_to_message_id: int | str | None = None,
):
    await send_to_queueu(
        msg=Message(
            type=MessageType.edit_msg,
            data=TaskMessage(
                bot_id=bot_id,
                chat_id=chat_id,
                message_id=msg_id,
                text=text,
                reply_markup=reply_markup,
                reply_to_message_id=reply_to_message_id,
            ),
        ),
        stream_name=f"{redis_settings.TG_BROADCAST_STREAM_PREFIX}{bot_id}",
    )
