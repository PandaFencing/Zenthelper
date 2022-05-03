import redis


def RedisConnect():
    r = redis.Redis(
        host = '127.0.0.1',
        port = '6370'
    )

    return r

class RedisQueen(object):
    def __init__(self) -> None:
        redisPool = redis.ConnectionPool(
            host = '127.0.0.1',
            port = '6379',
            decode_responses=True,
            password=''
            )
        self.__db = redis.Redis(connection_pool = redisPool)
        
    def Qsize(self, key):
        return self.__db.llen(key)

    def Put(self, key, item):
        self.__db.rpush(key, item)

    def GetWait(self, key, timeout=None):
        item = self.__db.blpop(key, timeout=timeout)
        return item

    def GetNowait(self, key):
        item = self.__db.lpop(key)
        return item

    def DeleteKey(self, key):
        return self.__db.delete(key)

if __name__ == '__main__':
    RedisQueen = RedisQueen()
    rc = RedisQueen.Qsize('test')
    print(rc)
    RedisQueen.Put('test', "haha")
    RedisQueen.Put('test', "haha1")
    RedisQueen.Put('test', "haha2")
    RedisQueen.Put('test', "haha3")

    rc = RedisQueen.Qsize('test')
    print(rc)
    RedisQueen.DeleteKey('test')
    rc = RedisQueen.qsize('test')
    print(rc)

