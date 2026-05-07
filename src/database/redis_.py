
import redis


class RedisConnection:
    pool_instance = None
    pool = None
    
    
    def get_redis(self):
        if self.pool_instance is None:
            redis_pool = redis.ConnectionPool( host='localhost',
                port=6379,
                db=0,
                max_connections=50,
                decode_responses=True,
                retry_on_timeout=True,
                health_check_interval=30)
            
            pool = redis.StrictRedis(connection_pool=redis_pool)
      
        return pool
    
# redis_client = RedisConnection().get_redis()
# print(redis_client.ping())