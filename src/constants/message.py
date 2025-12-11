from enum import StrEnum


class MessageType(StrEnum):
    pulse = "pulse"
    add_bot = "add_bot"
    remove_bot = "remove_bot"
    send_msg = "send_msg"
    del_msg = "del_msg"
    edit_msg = "edit_msg"
