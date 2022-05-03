import baostock as bs

def BaoStockLogin():
    lg = bs.login()
    print('login respond error_code:'+lg.error_code)
    print('login respond  error_msg:'+lg.error_msg)
    return bs

def BaoStockLogout(bs):
    bs.logout()
    print('logout baostock done!')