from ReqBaoStockData import *
from common.RedisDB import *

# sync all stock to redis key all_stock


def SyncAllStock():
    redisAllStockKey = 'all_stock'
    redisQueen = RedisQueen()
    reqBaoData = ReqBaoData()

    if redisQueen.Qsize(redisAllStockKey) != 0:
        redisQueen.DeleteKey(redisAllStockKey)
    for code in reqBaoData.QueryAllStock("2022-02-28"):
        redisQueen.Put(redisAllStockKey, code)


