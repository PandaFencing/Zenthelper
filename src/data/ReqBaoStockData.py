import sys
sys.path.append('../')
from common.BaoStockCommon import *
import pandas as pd


class ReqBaoData(object):

    def __init__(self) -> None:
        self.bs = BaoStockLogin()

    def QueryHistoryKData(self, code, start=None, end=None, frequency='d', adjustflag='3'):
        self.queryFunc= {
            'd': self.QueryHistoryDKData,
            'w': self.QueryHistoryWMKData,
            'm': self.QueryHistoryWMKData,
            '5': self.QueryHistoryMinutesKData,
            '15': self.QueryHistoryMinutesKData,
            '30': self.QueryHistoryMinutesKData,
            '60': self.QueryHistoryMinutesKData
        }

        self.code = code
        self.start = start
        self.end = end
        self.frequency = frequency
        self.adjustflag = adjustflag

        return self.queryFunc[frequency]()

    def QueryHistoryDKData(self):
        rs = bs.query_history_k_data_plus(self.code,
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST", 
        start_date=self.start, end_date=self.end,
        frequency=self.frequency, adjustflag=self.adjustflag
        )
        print('query_history_k_data_plus query code:' + self.code)
        print('query_history_k_data_plus respond error_code:'+rs.error_code)
        print('query_history_k_data_plus respond  error_msg:'+rs.error_msg)

        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        return data_list

    def QueryHistoryWMKData(self):
        rs = bs.query_history_k_data_plus(self.code,
        "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg", 
        start_date=self.start, end_date=self.end,
        frequency=self.frequency, adjustflag=self.adjustflag
        )
        print('query_history_k_data_plus query code:' + self.code)
        print('query_history_k_data_plus respond error_code:'+rs.error_code)
        print('query_history_k_data_plus respond  error_msg:'+rs.error_msg)

        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        return data_list


    def QueryHistoryMinutesKData(self):
        rs = bs.query_history_k_data_plus(self.code, 
        "date,time,code,open,high,low,close,volume,amount,adjustflag", 
        start_date=self.start, end_date=self.end,
        frequency=self.frequency, adjustflag=self.adjustflag)
        print('query_history_k_data_plus query code:' + self.code)
        print('query_history_k_data_plus respond error_code:'+rs.error_code)
        print('query_history_k_data_plus respond  error_msg:'+rs.error_msg)


        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())

        return data_list
    
    def QueryAllStock(self, date = None):
        stock_rs = self.bs.query_all_stock(date)
        stock_df = stock_rs.get_data()
        data_list = []
        for code in stock_df["code"]:
            data_list.append(code)
        return data_list

    def BaoStockLogout(self):
        BaoStockLogout(self.bs)

if __name__ == '__main__':
    ReqBaoData = ReqBaoData()
    ReqBaoData.QueryAllStock("2022-04-28")
    pass
    # ReqBaoData = ReqBaoData()
    # ReqBaoData.QueryHistoryKData(300)
    # ReqBaoData.BaoStockLogout()