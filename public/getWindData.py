# -*- coding:UTF-8 -*-

def get_wind_data(exeStr):
    from sqlalchemy import create_engine
    import pandas as pd
    db_info = {'user': 'query',
               'password': 'query',
               'host': '10.201.4.164',
               'port': '1433',
               'database': 'wind_fsync'
               }
    # wind_engine = create_engine('mssql+pymssql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s?charset=cp936' % db_info)
    wind_engine = create_engine('mssql+pymssql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s?charset=utf8' % db_info)
    dt = pd.read_sql(exeStr, wind_engine)
    return dt

def get_wind_data2(exeStr):
    import pymssql
    conn = pymssql.connect('10.201.4.164', 'query', 'query', 'wind_fsync')
    cursor = conn.cursor()
    cursor.execute(exeStr)
    res = cursor.fetchall()
    cursor.close()
    conn.close()
    return res

