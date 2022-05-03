from pymongo import MongoClient


## Mongodb URL
MongodbURL = 'mongodb://localhost:27017'
def MongoConnect():
    return MongoClient(MongodbURL)

class DumpMongo(object):
    def __init__(self) -> None:
        self.__db = MongoClient(MongodbURL)
        self.db = self.__db.get_database("AStockData")

    def CloseMongo(self):
        self.__db.close()

    def DumpData(self, data, dataType):
        # １：日线　２：周线　３：月线　４：５分钟线　５：１５分钟线　６：３０分钟线　７：６０分钟线
        self.dType = {
                'd': self.DumpDWMKData,
                'w': self.DumpDWMKData,
                'm': self.DumpDWMKData,
                '5': self.DumpMinutesKdata,
                '15': self.DumpMinutesKdata,
                '30': self.DumpMinutesKdata,
                '60': self.DumpMinutesKdata,
        }

        return self.dType[dataType](data, dataType)

    def DumpDWMKData(self, data, dataType):         # dump daily weekly monthly k data
        collection = {
            'd': 'DailyKData',
            'w': 'WeeklyKData',
            'm': 'MonthlyKData'
        }
        code = data[0][1]
        item = {code : data}
        db = self.db.get_collection(collection[dataType])
        db.insert_one(item)

    def DumpMinutesKdata(self, data, dataType):     # dump 5 15 30 60 k data
        collection = {
            '5': 'FiveMKData',
            '15': 'FiftyMKData',
            '30': 'ThirtyMKData',
            '60': 'SixtyMKData',
        }
        code = data[0][1]
        item = {code : data}
        db = self.db.get_collection(collection[dataType])
        db.insert_one(item)


class QuerryMongo(object):
    def __init__(self) -> None:
        pass 