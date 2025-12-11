from typing import Optional
from aiogram import Bot
from schemas.message import Message, MessageType, LogMessage
from services.rate_limiter import rate_limiter
from services.telegram import (
    send_message,
    split_message,
    delete_message,
    edit_message,
)
from workers.producers import send_to_queueu


async def send_msg(
    msg: Message,
    bot: Bot,
    logs_stream: Optional[str] = None,
) -> None:
    messages = split_message(msg.data.text)
    for text_msg in messages:
        await rate_limiter.acquire_lock(msg.data.chat_id)
        _, sent_msg_id = await send_message(
            bot=bot,
            chat_id=msg.data.chat_id,
            text=text_msg,
            reply_markup=msg.data.reply_markup,
            reply_to_message_id=msg.data.reply_to_message_id,
        )
        if logs_stream:
            await send_to_queueu(
                msg=LogMessage(
                    type=MessageType.send_msg,
                    status=1 if sent_msg_id != 0 else 0,
                    bot_id=msg.data.bot_id,
                    chat_id=msg.data.chat_id,
                    text=text_msg,
                    reply_markup=msg.data.reply_markup,
                    reply_to_message_id=msg.data.reply_to_message_id,
                    sent_msg_id=sent_msg_id,
                    external_id=msg.data.external_id,
                    details=None
                    if sent_msg_id != 0
                    else "Failed send message",
                ),
                stream_name=logs_stream,
                w_raise=True,
            )


async def edit_msg(
    msg: Message,
    bot: Bot,
    logs_stream: Optional[str] = None,
) -> None:
    await rate_limiter.acquire_edit_lock(msg.data.chat_id)
    res = await edit_message(
        bot=bot,
        chat_id=msg.data.chat_id,
        message_id=msg.data.message_id,
        text=msg.data.text,
        reply_markup=msg.data.reply_markup,
    )
    if logs_stream:
        await send_to_queueu(
            msg=LogMessage(
                type=MessageType.edit_msg,
                status=1 if res is True else 0,
                bot_id=msg.data.bot_id,
                chat_id=msg.data.chat_id,
                text=msg.data.text,
                message_id=msg.data.message_id,
                external_id=msg.data.external_id,
                reply_markup=msg.data.reply_markup,
                reply_to_message_id=msg.data.reply_to_message_id,
                details="" if res is True else "Failed to change msg",
            ),
            stream_name=logs_stream,
            w_raise=True,
        )


async def del_msg(
    msg: Message,
    bot: Bot,
    logs_stream: Optional[str] = None,
) -> None:
    await rate_limiter.acquire_lock(msg.data.chat_id)
    if await delete_message(
        bot=bot, chat_id=msg.data.chat_id, message_id=msg.data.message_id
    ):
        detail = ""
    else:
        detail = "Cannot delete message"
    if logs_stream:
        await send_to_queueu(
            msg=LogMessage(
                type=MessageType.del_msg,
                status=1 if detail == "" else 0,
                bot_id=msg.data.bot_id,
                chat_id=msg.data.chat_id,
                message_id=msg.data.message_id,
                external_id=msg.data.external_id,
                details=detail,
            ),
            stream_name=logs_stream,
            w_raise=True,
        )
