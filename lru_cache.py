import threading
import redis

class  redisUtil(object):
    """
    docstring for  redisUtil
    """
    pool = None
    def __init__(self, host='localhost', port=6379):
        redisUtil.pool = redis.ConnectionPool(host=host, port=port)
        self.rd = redis.StrictRedis(connection_pool = redisUtil.pool)

    def __del__(self):
        del self.rd

    def getRedis(self):
        return self.rd

CACHE_STATE = "lru-stats"
CACHE_STORE = "lru-store"
CACHE_KEY   = 'lru-keys'
class LruCache(object):
    def __init__(self, item_max=5):
        if item_max <=0:
            raise ValueError('item_max must be > 0')
        self.lock = threading.Lock()
        self.item_max = item_max
        self.used = 0
        self.redis = redisUtil().getRedis()
        length = self.redis.llen(CACHE_KEY)
        self.redis.hset(CACHE_STATE, 'used', length)

    def clear(self):
        self.redis.flushall()
        self.redis.hset(CACHE_STATE, 'used', 0)

    def fn_cache(self, fn):
        def warp(*args, **kwargs):
            key = "%s%s" % (fn.func_name, repr((args, kwargs)))
            result =  self[key]
            if result == None:
                result = fn(*args, **kwargs)
                self[key] = result
            return result
        return warp


    def __getitem__(self, key):
        return self.get(key)


    def __setitem__(self, key, value):
        self.put(key, value)


    def get(self, key, default=None):
        with self.lock:
            if self.redis.hexists(CACHE_STORE, key):
                self.redis.hincrby(CACHE_STATE, 'hits')
                self.__lru_key(old_key=key, new_key=key)
                return self.redis.hget(CACHE_STORE, key)
            else:
                self.redis.hincrby(CACHE_STATE, 'miss')
                return default


    def put(self, key, val):
        with self.lock:
            if self.redis.hexists(CACHE_STORE, key):
                self.redis.hset(CACHE_STORE, key, val)
                self.__lru_key(old_key=key, new_key=key)
                return

            used = self.redis.hget(CACHE_STATE, 'used')
            if int(used) == self.item_max:
                r_key = self.redis.rpop(CACHE_KEY)
                self.redis.hdel(CACHE_STORE, r_key)

                self.redis.hset(CACHE_STORE, key, val)
                self.redis.hincrby(CACHE_STATE, 'removed')
                self.__lru_key(old_key=r_key, new_key=key)
            else:
                self.redis.hincrby(CACHE_STATE, 'used')
                self.redis.hset(CACHE_STORE, key, val)
                self.__lru_key(old_key=key, new_key=key)


    def __lru_key(self, old_key=None, new_key=None):
        self.redis.lrem(CACHE_KEY, 0, old_key)
        self.redis.lpush(CACHE_KEY, new_key)


    def status(self):
        used_status = """
             cache used status:
                max:%s
                used:%s
                key:%s
                miss:%s
                hits:%s
                remove:%s
            """ % (self.item_max, 
                self.redis.hget(CACHE_STATE, 'used'), 
                ','.join(self.redis.lrange(CACHE_KEY, 0, -1)), 
                self.redis.hget(CACHE_STATE, 'miss'), 
                self.redis.hget(CACHE_STATE, 'hits'), 
                self.redis.hget(CACHE_STATE, 'removed'))
        print used_status
        return used_status

if __name__ == '__main__':
    lru = LruCache()
    lru.clear()
    for i in range(5):
        lru['test_' + str(i)] = i
    lru['test_5'] = 5
    lru['test_1'] = 2
    lru['test_7'] = 6
    print lru['test_5']
    print lru['test_34']
    lru.status()


    @lru.fn_cache
    def add(a, b):
        return a + b

    for i in xrange(6):
        print add(3, i)
    lru.status()
