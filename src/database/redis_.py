
import os
import redis
import redis.asyncio as aioredis

# Defaults to 'redis' (the docker-compose service name) so the app works
# unchanged inside docker; set REDIS_HOST=localhost in the environment when
# running outside docker (e.g. a local Redis, or `docker compose` with the
# redis port published to the host).
REDIS_HOST = os.getenv("REDIS_HOST", "redis")


class RedisConnection:
    pool_instance = None
    pool = None
    _async_pool = None



    def get_redis(self):
        if self.pool_instance is None:
            redis_pool = redis.ConnectionPool( host=REDIS_HOST,
                port=6379,
                db=0,
                max_connections=50,
                decode_responses=True,
                retry_on_timeout=True,
                health_check_interval=30)
            
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
                health_check_interval=30
            )
        
        # Returns the proper async Redis instance using the managed pool
        return aioredis.Redis(connection_pool=RedisConnection._async_pool)
    
# redis_client = RedisConnection().get_redis()
# print(redis_client.ping())