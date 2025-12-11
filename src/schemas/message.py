import json
from typing import Any, Optional, Union

from pydantic import BaseModel, model_validator

from constants.message import MessageType


class ServiceMessage(BaseModel):
    bot_id: int
    token: str
    is_sent_logs: Optional[bool] = False


class InlineButton(BaseModel):
    text: str
    callback_data: str


class ReplyMarkup(BaseModel):
    inline_keyboard: list[list[InlineButton]]


class TaskMessage(BaseModel):
    external_id: Optional[int] = None
    bot_id: int
    chat_id: int | str
    text: str | None = None
    message_id: Optional[int | str] = None
    reply_markup: Optional[ReplyMarkup] = None
    reply_to_message_id: int | str | None = None


class Message(BaseModel):
    type: MessageType
    data: Union[ServiceMessage, TaskMessage]

    @model_validator(mode="before")
    @classmethod
    def validate_data(cls, data: Any) -> Any:
        if (
            isinstance(data, dict)
            and data.get("data")
            and isinstance(data["data"], str)
        ):
            data["data"] = json.loads(data["data"])
        return data


class LogMessage(BaseModel):
    type: MessageType
    status: int
    bot_id: int
    chat_id: int | str
    text: str | None = None
    reply_markup: Optional[ReplyMarkup] = None
    reply_to_message_id: int | str | None = None
    message_id: Optional[int | str] = None
    sent_msg_id: Optional[int] = None
    details: Optional[str] = None
    external_id: Optional[int] = None
