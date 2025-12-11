from pathlib import Path

from .base import BaseSetting

BASE_DIR = Path(__file__).parent.parent


class AppSettings(BaseSetting):
    SERVICE_NAME: str = "TG Sender"
    DUMMY_TOKEN: str = "dummy"


class RedisSetting(BaseSetting):
    REDIS_HOST: str
    REDIS_PORT: int
    CONTROL_STREAM_NAME: str = "stream:tg_bot:control"
    TG_KEY_PREFIX: str = "telegram_bot:"
    TG_STREAM_PREFIX: str = "stream:tg_bot:"
    TG_BROADCAST_STREAM_PREFIX: str = "stream:tg_bot:broadcast:"
    TG_BOT_LOG_STREAM_PREFIX: str = "stream:tg_bot:logs:"
    GROUP_NAME: str = "base"
    RECLAIM_INTERVAL_SECONDS: int = 60  # How often to check for stuck messages
    IDLE_THRESHOLD_MS: int = 30000
    MAX_PENDING_TO_SCAN: int = 10
    CHAT_SEND_PREFIX: str = "limiter:send:chat_id:"
    CHAT_EDIT_PREFIX: str = "limiter:edit:chat_id:"
    GROUP_SEND_PREFIX: str = "limiter:group:chat_id:"


class TelegramSetting(BaseSetting):
    GLOBAL_RPS: int = 28
    PER_CHAT_DELAY: float = 1.0
    PER_CHAT_EDIT_DELAY: float = 3.05
    PER_GROUP_MSG_DELAY: float = 3.05
    TELEGRAM_MSG_LIMIT: int = 4096


app_settings = AppSettings()
telegram_settings = TelegramSetting()
redis_settings = RedisSetting()
