import asyncio

from workers.consumers import run_consumers


if __name__ == "__main__":
    asyncio.run(run_consumers())
