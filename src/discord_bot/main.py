import asyncio

from src import conf
from src.clients.rabbit import RabbitWorker
from src.discord_bot.discord_factory import discord_factory


async def main():
    discord_client = discord_factory(conf.ENV)
    rabbit = RabbitWorker(
        conf.RABBIT_HOST,
        conf.RABBIT_PORT,
        conf.RABBIT_USER,
        conf.RABBIT_PASSWORD,
        discord_client,
        asyncio.get_running_loop(),
    )

    await asyncio.gather(
        rabbit.consume(),
        discord_client.start(conf.DISCORD_TOKEN),
    )


if __name__ == "__main__":
    asyncio.run(main())
