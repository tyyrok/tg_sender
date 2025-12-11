import asyncio
from typing import Optional

from aiogram import Bot
from aiogram.exceptions import (
    TelegramRetryAfter,
    TelegramForbiddenError,
    TelegramAPIError,
)
from aiogram.enums import ParseMode

from configs.logger import logger
from configs.config import telegram_settings
from schemas.message import ReplyMarkup


async def send_message(
    bot: Bot,
    chat_id: int,
    text: str,
    parse_mode: Optional[ParseMode] = ParseMode.HTML,
    reply_markup: Optional[ReplyMarkup] = None,
    reply_to_message_id: Optional[str | int] = None,
) -> tuple[int, int]:
    try:
        chat_id, msg_id = await _send_message(
            bot=bot,
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
            reply_to_message_id=reply_to_message_id,
        )
        msg = f"Sent message id: {msg_id} to user_id:{chat_id} text: {text}"
        logger.info(msg)
    except TelegramRetryAfter as ex:
        logger.debug(ex)
        await asyncio.sleep(ex.retry_after)
        try:
            chat_id, msg_id = await _send_message(
                bot=bot,
                chat_id=chat_id,
                text=text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                reply_to_message_id=reply_to_message_id,
            )
            msg = f"Sent message id: {msg_id} to user_id:{chat_id}"
            logger.info(msg)
        except TelegramRetryAfter as ex:
            logger.exception(ex)
            await asyncio.sleep(ex.retry_after)
    except TelegramForbiddenError as ex:
        logger.exception(ex)
        msg = f"Failed to sent message to user_id:{chat_id} text: {text}"
        logger.exception(msg)
        return chat_id, 0
    except TelegramAPIError as ex:
        logger.exception(ex)
        msg = f"Failed to sent message to user_id:{chat_id} text: {text}"
        logger.exception(msg)
        return chat_id, 0
    return chat_id, msg_id


async def _send_message(
    bot: Bot,
    chat_id: int,
    text: str,
    parse_mode: Optional[ParseMode] = ParseMode.HTML,
    reply_markup: Optional[ReplyMarkup] = None,
    reply_to_message_id: Optional[str | int] = None,
) -> tuple[int, int]:
    msg = await bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode=parse_mode,
        reply_markup=reply_markup.model_dump() if reply_markup else None,
        reply_to_message_id=reply_to_message_id,
    )
    return chat_id, msg.message_id


def split_message(msg: str) -> list[str]:
    """
    Splits the text into parts considering Telegram limits.
    """
    parts = []
    while msg:
        if len(msg) <= telegram_settings.TELEGRAM_MSG_LIMIT:
            parts.append(msg)
            break

        part = msg[: telegram_settings.TELEGRAM_MSG_LIMIT]
        first_ln = part.rfind("\n")

        if first_ln != -1:
            new_part = part[:first_ln]
            parts.append(new_part)
            msg = msg[first_ln + 1 :]
        else:
            first_space = part.rfind(" ")

            if first_space != -1:
                new_part = part[:first_space]
                parts.append(new_part)
                msg = msg[first_space + 1 :]
            else:
                parts.append(part)
                msg = msg[telegram_settings.TELEGRAM_MSG_LIMIT :]

    return parts


async def delete_message(
    bot: Bot, chat_id: int, message_id: int | str
) -> bool:
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        msg = f"Deleted message id: {message_id} from chat_id:{chat_id}"
        logger.info(msg)
        return True
    except TelegramRetryAfter as ex:
        logger.debug(ex)
        await asyncio.sleep(ex.retry_after)
        try:
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
            msg = f"Deleted message id: {message_id} from chat_id:{chat_id}"
            logger.info(msg)
        except TelegramRetryAfter as ex:
            logger.exception(ex)
            await asyncio.sleep(ex.retry_after)
    except TelegramForbiddenError as ex:
        logger.exception(ex)
        msg = f"Failed to delete message:{message_id} from chat_id:{chat_id}"
        logger.exception(msg)
        return False
    except TelegramAPIError as ex:
        logger.exception(ex)
        msg = f"Failed to delete message:{message_id} from chat_id:{chat_id}"
        logger.exception(msg)
        return False
    return False


async def edit_message(
    bot: Bot,
    chat_id: int,
    message_id: int | str,
    text: Optional[str] = None,
    parse_mode: Optional[ParseMode] = ParseMode.HTML,
    reply_markup: Optional[ReplyMarkup] = None,
    reply_to_message_id: Optional[str | int] = None,
) -> tuple[int, int]:
    try:
        markup = None
        if (
            reply_markup
            and len(reply_markup.inline_keyboard) > 0
            and len(reply_markup.inline_keyboard[0]) > 0
        ):
            markup = reply_markup.model_dump()
        if text:
            if len(text) > telegram_settings.TELEGRAM_MSG_LIMIT:
                text = text[:telegram_settings.TELEGRAM_MSG_LIMIT]
            res = await bot.edit_message_text(
                text=text,
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=markup,
                parse_mode=parse_mode,
            )
        else:
            res = await bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=markup,
            )

        msg = f"Edited message id: {message_id} from chat_id:{chat_id}"
        logger.info(msg)
    except TelegramRetryAfter as ex:
        logger.debug(ex)
        await asyncio.sleep(ex.retry_after)
        try:
            if text:
                res = await bot.edit_message_text(
                    text=text,
                    chat_id=chat_id,
                    message_id=message_id,
                    reply_markup=markup,
                )
            else:
                res = await bot.edit_message_reply_markup(
                    chat_id=chat_id, message_id=message_id, reply_markup=markup
                )

            msg = f"Edited message id: {message_id} from chat_id:{chat_id}"
            logger.info(msg)
        except TelegramRetryAfter as ex:
            logger.exception(ex)
            return False
    except TelegramForbiddenError as ex:
        logger.exception(ex)
        msg = f"Failed to edit message: {message_id} from chat_id:{chat_id}"
        logger.exception(msg)
        return False
    except TelegramAPIError as ex:
        logger.exception(ex)
        msg = f"Failed to edit message: {message_id} from chat_id:{chat_id}"
        logger.exception(msg)
        return False
    return bool(res)
