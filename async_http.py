import asyncio
from socket import AF_INET
from typing import Iterable

from aiohttp import ClientSession, ServerDisconnectedError
from aiohttp.client_exceptions import ClientResponseError
from aiohttp.connector import TCPConnector
from failsafe import CircuitBreaker, Failsafe, RetryPolicy
from failsafe.failsafe import FailsafeError

SIZE_POOL_AIOHTTP = 100


class SingletonHTTPClient:
    sem: asyncio.Semaphore = None
    aiohttp_client: ClientSession = None
    breaker: Failsafe = None

    @classmethod
    def get_aiohttp_client(cls) -> ClientSession:
        if cls.aiohttp_client is None:
            connector = TCPConnector(family=AF_INET, limit_per_host=SIZE_POOL_AIOHTTP)
            cls.aiohttp_client = ClientSession(
                connector=connector, raise_for_status=True
            )

        return cls.aiohttp_client

    @classmethod
    def get_breaker(cls) -> Failsafe:
        if cls.breaker is None:
            cls.breaker = Failsafe(
                circuit_breaker=CircuitBreaker(
                    maximum_failures=3, reset_timeout_seconds=5
                ),
                retry_policy=RetryPolicy(
                    retriable_exceptions=(ServerDisconnectedError, ClientResponseError)
                ),
            )
        return cls.breaker

    @classmethod
    async def close_aiohttp_client(cls):
        if cls.aiohttp_client:
            await cls.aiohttp_client.close()
            cls.aiohttp_client = None
            cls.breaker = None

    @classmethod
    async def fetch(cls, url, fail_silently=True):
        async def _fetch(client, url, **kwargs):
            async with client.get(
                url,
                **kwargs,
            ) as response:
                if response.status == 200:
                    resp = await response.json()
                    return resp

        try:
            return await cls.get_breaker().run(_fetch, cls.get_aiohttp_client(), url)
        except FailsafeError:
            LOGGER.warn("Circuit Breaker. Unable to connect to remote.")
            if fail_silently:
                return None
            raise Exception(
                "Circuit breaker. Unable to connect to remote. Locking."
            )

    @classmethod
    async def fetch_urls(cls, urls: Iterable, fail_silently=True):
        tasks = []
        for url in urls:
            task = asyncio.ensure_future(cls.fetch(url, fail_silently=fail_silently))
            tasks.append(task)
        return await asyncio.gather(*tasks)
