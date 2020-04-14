# -*- coding:UTF-8 -*-

import pymssql

def get_data_sql(exeStr, database):
    def switch(argument):
        switcher = {'wind': ['10.201.4.164', 'query', 'query', 'wind_fsync'],
                    'lhtz': ['10.201.4.164', 'lhtz', 'Cjhx+20140801', 'lhtzdb']}
        res = switcher.get(argument)
        return res
    databaseInfo = switch(database.lower())
    conn = pymssql.connect(databaseInfo[0], databaseInfo[1], databaseInfo[2], databaseInfo[3])
    cursor = conn.cursor()
    cursor.execute(exeStr)
    res = cursor.fetchall()
    cursor.close()
    conn.close()
    return res

