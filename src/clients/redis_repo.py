import asyncio
import typing

from loguru import logger
from pydantic import BaseModel
from redis.asyncio import Redis

from src import conf
from src.clients.models.rabbit.queues import QueueName

if typing.TYPE_CHECKING:
    from src.discord_bot.client import DiscordClient

redis_client = Redis.from_url(url=conf.redis_string)
MATCHES_TTL = 60 * 60 * 24  # 24 hours


class RedisRepository:
    async def save_match(self, match_id: str, nick_elo: dict[str, int]) -> None:
        r_key = f"matches:{match_id}"
        async with redis_client:
            await redis_client.hset(r_key, mapping=nick_elo)
            await redis_client.expire(r_key, MATCHES_TTL, nx=True)

    async def get_match_elo(self, match_id: str) -> dict[str, int]:
        r_key = f"matches:{match_id}"
        async with redis_client:
            redis_data: dict[bytes:bytes] = await redis_client.hgetall(r_key)
            return {k.decode(): int(v) for k, v in redis_data.items()}

    async def publish(self, queue: QueueName, message: BaseModel) -> None:
        await redis_client.publish(queue, message.model_dump_json())

    async def _consume_match_queue(
        self,
        discord: "DiscordClient",
        queue: str = "matches",
    ):
        logger.info(f"Start consuming from REDIS. {queue = }")
        sub = redis_client.pubsub(ignore_subscribe_messages=True)
        await sub.subscribe(queue)
        async for message in sub.listen():
            # match: WebhookMatch = TypeAdapter(WebhookMatch).validate_python(
            #     json.loads(message.body.decode())
            # )
            logger.info(message)
            # match match.event:
            #     case EventEnum.READY | EventEnum.CONFIGURING:
            #         await discord.post_faceit_message_ready(match)
            #     case EventEnum.CANCELLED | EventEnum.ABORTED:
            #         await discord.post_faceit_message_aborted(match)
            #     case EventEnum.FINISHED:
            #         await discord.post_faceit_message_finished(match)

    async def _consume_score_queue(
        self, discord: "DiscordClient", queue: str = "update_score"
    ) -> None:
        logger.info(f"Start consuming from REDIS. {queue = }")
        sub = redis_client.pubsub(ignore_subscribe_messages=True)
        await sub.subscribe(queue)
        async for message in sub.listen():
            logger.info(message)

    async def consume(self, discord: "DiscordClient") -> None:
        sleep_time = 0
        while not discord.faceit_channel:
            sleep_time += 1
            logger.info(
                f"Redis waiting for discord bot to startup. Total sleep: {sleep_time} sec."
            )
            await asyncio.sleep(1)

        await asyncio.gather(
            self._consume_match_queue(discord, "matches"),
            self._consume_score_queue(discord, "update_score"),
        )


redis_repo = RedisRepository()
