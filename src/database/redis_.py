"""Redis client factory - the app's read cache (db 0), separate from the arq
job-queue Redis in worker.py.

`RedisConnection` hands out two clients:

  - get_redis()       : synchronous (redis-py). Quick lookups; blocks the event
                        loop if called from async code.
  - get_async_redis() : asyncio client, over a lazily-created class-level pool
                        shared by every instance. Preferred everywhere new.

Both use `decode_responses=True` (values come back as `str`; callers
`json.loads` / `int()` as needed) and short connect/socket timeouts so an
unreachable Redis fails fast with a clean ConnectionError.

`health_check_interval` is deliberately 0 (disabled): with a live periodic
health check, redis-py's `on_connect` handshake (`CLIENT SETINFO`) re-enters
`check_health` -> `connect` -> `on_connect` when the socket is dead, recursing
until `RecursionError` ("maximum recursion depth exceeded"). Disabling it turns
that into a normal connection error. The app should be reachable-Redis-only
anyway; if a stale-connection guard is needed later, wrap calls in a retry.

NOTE: get_redis() never actually caches - `pool_instance` stays None, so it
builds a fresh ConnectionPool on every call. Only the async side is pooled.
"""

import os
import redis
import redis.asyncio as aioredis

# 'localhost' for running outside docker (a local Redis, or `docker compose up
# redis` with port 6379 published). Set REDIS_HOST=redis inside docker-compose
# so it resolves to the service.
REDIS_HOST = os.getenv("REDIS_HOST", "redis")

# Fail a dead connection in seconds, not by hanging or recursing.
_SOCKET_CONNECT_TIMEOUT = 5
_SOCKET_TIMEOUT = 5


class RedisConnection:
    pool_instance = None
    pool = None
    _async_pool = None



    def get_redis(self):
        # NOTE: pool_instance is never assigned, so this branch always runs.
        if self.pool_instance is None:
            redis_pool = redis.ConnectionPool( host=REDIS_HOST,
                port=6379,
                db=0,
                max_connections=50,
                decode_responses=True,
                retry_on_timeout=True,
                socket_connect_timeout=_SOCKET_CONNECT_TIMEOUT,
                socket_timeout=_SOCKET_TIMEOUT,
                health_check_interval=0)

            pool = redis.StrictRedis(connection_pool=redis_pool)

        return pool

    def get_async_redis(self):
        """Returns an async Redis client instance using a shared connection pool."""
        if RedisConnection._async_pool is None:
            # FIX: Check and set the exact same class-level variable name
            RedisConnection._async_pool = aioredis.ConnectionPool(
                host=REDIS_HOST,
                port=6379,
                db=0,
                max_connections=50,
                decode_responses=True,
                retry_on_timeout=True,
                socket_connect_timeout=_SOCKET_CONNECT_TIMEOUT,
                socket_timeout=_SOCKET_TIMEOUT,
                health_check_interval=0,
            )

        # Returns the proper async Redis instance using the managed pool
        return aioredis.Redis(connection_pool=RedisConnection._async_pool)
    
# redis_client = RedisConnection().get_redis()
# print(redis_client.ping())