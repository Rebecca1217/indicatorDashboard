# -*- coding:UTF-8 -*-

# import pymssql
from sqlalchemy import create_engine
import pandas as pd

def get_data_sql(exeStr, database):
    def switch(argument):
        switcher = {
            'wind': [
                'query',
                'query',
                '10.201.4.164',
                '1433',
                'wind_fsync'],
            'lhtz': [
                'lhtz',
                'Cjhx+20140801',
                '10.201.4.164',
                '1433',
                'lhtzdb']}
        res = switcher.get(argument)
        return res
    databaseInfo = switch(database.lower())
    db_info = dict(pd.DataFrame(
        {'Var': ['user', 'password', 'host', 'port', 'database'],
         'Value': databaseInfo}).values)
    db_engine = create_engine(
        'mssql+pymssql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s?charset=utf8' %
        db_info)
    dt = pd.read_sql(exeStr, db_engine)
    db_engine.dispose()
    # conn = pymssql.connect(databaseInfo[0], databaseInfo[1], databaseInfo[2], databaseInfo[3])
    # cursor = conn.cursor()
    # cursor.execute(exeStr)
    # res = cursor.fetchall()
    # cursor.close()
    # conn.close()
    return dt
