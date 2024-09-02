import asyncio
import json
from asyncio import AbstractEventLoop

import aio_pika
from aio_pika.abc import AbstractRobustConnection
from aio_pika.pool import Pool
from loguru import logger
from pydantic import TypeAdapter

from src.clients.models.faceit.match_details import MatchDetails
from src.clients.models.rabbit.queues import QueueName
from src.discord_bot.client import DiscordClient
from src.utils.shared_models import DetailsMatchDict
from src.web.models.base import EventEnum
from src.web.models.events import WebhookMatch, MatchReady


class RabbitClient:
    def __init__(self, host: str, port: int, user: str, password: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.loop: AbstractEventLoop = asyncio.get_event_loop()
        self.connection_pool: Pool = Pool(
            self._get_connection, max_size=2, loop=self.loop
        )

    async def _get_connection(self) -> AbstractRobustConnection:
        return await aio_pika.connect_robust(
            host=self.host,
            port=self.port,
            login=self.user,
            password=self.password,
            loop=self.loop,
        )

    async def publish(
        self, message: str, routing_key: QueueName = QueueName.MATCHES
    ) -> None:
        async with self.connection_pool.acquire() as connection:
            channel = await connection.channel()
            await channel.default_exchange.publish(
                aio_pika.Message(body=message.encode()),
                routing_key=routing_key,
            )


class RabbitWorker:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        discord: DiscordClient,
        loop: AbstractEventLoop,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.discord = discord
        self.loop: AbstractEventLoop = loop
        self.connection_pool: Pool = Pool(self._get_connection, max_size=2, loop=loop)
        self.channel_pool: Pool = Pool(self._get_channel, max_size=2, loop=loop)

    async def _get_connection(self) -> AbstractRobustConnection:
        return await aio_pika.connect_robust(
            host=self.host,
            port=self.port,
            login=self.user,
            password=self.password,
            loop=self.loop,
        )

    async def _get_channel(self) -> aio_pika.Channel:
        async with self.connection_pool.acquire() as connection:
            return await connection.channel()

    async def _process_match(self, match: WebhookMatch) -> None:
        match match.event:
            case EventEnum.READY | EventEnum.CONFIGURING:
                await self.discord.post_faceit_message_ready(match)
            case EventEnum.CANCELLED | EventEnum.ABORTED:
                await self.discord.post_faceit_message_aborted(match)
            case EventEnum.FINISHED:
                await self.discord.post_faceit_message_finished(match)

    async def _update_score(self, match_details: MatchDetails, match_ready: MatchReady):
        await self.discord.update_score_for_match(
            match_details=match_details, match_ready=match_ready
        )

    async def _wait_discord_startup(self):
        sleep_time = 0
        while not self.discord.faceit_channel:
            sleep_time += 1
            logger.info(
                f"Rabbit waiting for discord bot to startup. Total sleep: {sleep_time} sec."
            )
            await asyncio.sleep(1)

    async def consume(self):
        await self._wait_discord_startup()
        async with self.channel_pool.acquire() as channel:  # type: aio_pika.Channel
            await asyncio.gather(
                self._consume_match_queue(channel, "matches"),
                self._consume_score_queue(channel, "update_score"),
            )

    async def _consume_match_queue(
        self, channel: aio_pika.Channel, queue_name: str = "matches"
    ):
        logger.info(f"Start consuming from RABBIT. {queue_name = }")
        queue = await channel.declare_queue(queue_name, durable=True)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:  # type: aio_pika.abc.AbstractIncomingMessage
                match: WebhookMatch = TypeAdapter(WebhookMatch).validate_python(
                    json.loads(message.body.decode())
                )
                try:
                    await self._process_match(match)
                except Exception as ex:
                    logger.error(ex)
                else:
                    await message.ack()

    async def _consume_score_queue(
        self, channel: aio_pika.Channel, queue_name: str = "update_score"
    ):
        logger.info(f"Start consuming from RABBIT. {queue_name = }")
        queue = await channel.declare_queue(queue_name, durable=True)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:  # type: aio_pika.abc.AbstractIncomingMessage
                details_and_match_dict = TypeAdapter(DetailsMatchDict).validate_json(
                    json.loads(message.body.decode())
                )
                match_details = TypeAdapter(MatchDetails).validate_python(
                    details_and_match_dict.match_details
                )
                match_ready = TypeAdapter(MatchReady).validate_python(
                    details_and_match_dict.match_ready
                )
                try:
                    await self._update_score(match_details, match_ready)
                except Exception as ex:
                    logger.error(f"{ex}, {ex.args}")
                    raise ex
                else:
                    await message.ack()
