# -*- coding:UTF-8 -*-

import pandas as pd
from public.getTradingDays import get_trading_days
from public.getWindData import get_wind_data
# 获取指数历史持仓 这个指数不包含行业指数，至少不包含申万行业指数
def get_index_pos(indexCode, dateFrom, dateTo):
    exeStr = 'select  S_INFO_WINDCODE, S_CON_WINDCODE, S_CON_INDATE, S_CON_OUTDATE, CUR_SIGN ' \
             'from dbo.AINDEXMEMBERS where S_INFO_WINDCODE = \'%s\' and' \
             ' (S_CON_INDATE <= %s and ( S_CON_OUTDATE > %s or S_CON_OUTDATE is null))' % (indexCode, dateTo, dateFrom)
    indexPos = get_wind_data(exeStr)
    indexPos = pd.DataFrame(data=indexPos, columns=['Index_Code', 'Stock_Code', 'DateIn', 'DateOut', 'Cur_Label'])
    indexPos['DateIn'] = pd.to_datetime(indexPos['DateIn'])
    indexPos['DateOut'] = pd.to_datetime(indexPos['DateOut'])

    if len(indexPos) > 0:
        # 调整指数数据格式为每天持仓
        # 先repmat全集出来，再根据时间筛选符合要求的
        repNum = indexPos.shape[0]
        tradingDays = get_trading_days(dateFrom, dateTo)
        tradingDays['Date'] = tradingDays.index
        repDaysNum = tradingDays.shape[0]
        tradingDays = pd.concat([tradingDays] * repNum, ignore_index=True)
        tradingDays.sort_values('Date', inplace=True)
        indexPosRep = pd.concat([indexPos] * repDaysNum, ignore_index=True)

        tradingDays = tradingDays.reset_index(drop=True)
        indexPosRep = indexPosRep.reset_index(drop=True)
        assert all(tradingDays.index == indexPosRep.index)
        indexPosFull = pd.concat([tradingDays, indexPosRep], axis=1)

        indexPosFull['Valid'] = (indexPosFull['Date'] >= indexPosFull['DateIn']) & \
                                ((indexPosFull['Date'] < indexPosFull['DateOut']) |
                                 (indexPosFull['DateOut'].isnull())) # 当天进当天出
        indexPos = indexPosFull[indexPosFull['Valid']]
        indexPos.reset_index(drop=True, inplace=True) # 数据筛选后必须reset index 不然index还是原来indexPosFull的
        # # check NUM
        # count = indexPos.groupby(by = ['Date'])['Stock_Code'].count()
        # assert all((count <= 300) & (count >= 295))

        indexPos = indexPos[['Index_Code', 'Date', 'Stock_Code']]

    else:
        indexPos = pd.DataFrame([], columns=['Index_Code', 'Date', 'Stock_Code'])

    indexPos.set_index(['Date', 'Stock_Code'], inplace=True)
    # 这个地方index怎么设置是个问题，理论上index应该是唯一的

    return (indexPos)

