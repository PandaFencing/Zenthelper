from ReqBaoStockData import *
from common.RedisDB import *
from SyncAllStock import *
from common.MongoDB import *
from multiprocessing import Process


def GetAllDData(frequency):
    redisAllStockkey = 'all_stock'
    # SyncAllStock()
    redisQueen = RedisQueen()
    reqBaoData = ReqBaoData()
    dumpMongo = DumpMongo()

    while redisQueen.Qsize(redisAllStockkey):
        code = redisQueen.GetWait(redisAllStockkey)
        code = code[1]  # redis return key:value
        rs = reqBaoData.QueryHistoryKData(code, start='2016-01-01', end='2022-04-29', frequency=frequency, adjustflag='2')
        if rs != []:
            dumpMongo.DumpData(rs, frequency)

    reqBaoData.BaoStockLogout()



if __name__ == '__main__':
   #frequency = ['d','w','m','5','15','30','60']
    frequency = ['30','60']
    for type in frequency:
        SyncAllStock()
        #GetAllDData(type)
        process_list = []
        for i in range(13):
            p = Process(target=GetAllDData, args=(type,))
            p.start()
            process_list.append(p)

        for i in process_list:
            p.join()
