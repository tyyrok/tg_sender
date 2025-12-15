import asyncio
import time

from configs.config import telegram_settings as tg_settings


class GlobalRateLimiter:
    def __init__(self) -> None:
        self.delay_interval = 1.0 / tg_settings.GLOBAL_RPS
        self.bots: list[int, tuple[asyncio.Lock, float]] = {}
        self.lock = asyncio.Lock()

    async def acquire_lock(self, bot_id: int | str) -> bool:
        lock, last_send = await self.get_bot_info(bot_id)
        async with lock:
            now = time.monotonic()
            next_time = last_send + self.delay_interval
            if now < next_time:
                await asyncio.sleep(next_time - now)
            await self.change_last_send(bot_id, time.monotonic())
            return True

    async def get_bot_info(
        self, bot_id: int | str
    ) -> tuple[asyncio.Lock, float]:
        async with self.lock:
            if bot_id not in self.bots:
                self.bots[bot_id] = [asyncio.Lock(), time.monotonic()]
            return self.bots[bot_id]

    async def change_last_send(
        self, bot_id: int | str, last_send: float
    ) -> None:
        async with self.lock:
            if bot_id in self.bots:
                self.bots[bot_id][-1] = last_send


global_limiter = GlobalRateLimiter()
