# -*- coding:UTF-8 -*-
import pandas as pd
from public.getTradingDays import get_trading_days
from public.getWindData import get_wind_data

def get_stock_fund_pos(dateFrom, dateTo):
    # 2001010201000000 偏股混合型, 2001010101000000 普通股票型
    sqlStr = 'select  a.F_INFO_WINDCODE, a.S_INFO_SECTORENTRYDT, a.S_INFO_SECTOREXITDT, a.CUR_SIGN ' \
             ' from ChinaMutualFundSector a ' \
             'left join AShareIndustriesCode b on ' \
             'a.S_INFO_SECTOR=b.INDUSTRIESCODE where a.S_INFO_SECTOR in ' \
             '(\'2001010201000000\', \'2001010101000000\') and ' \
             'a.S_INFO_SECTORENTRYDT <= {0} and (a.S_INFO_SECTOREXITDT > {1} or a.S_INFO_SECTOREXITDT is null)'.format(dateTo, dateFrom)
    #  entrydt is null and exitdt is null and cur_sign = 1这种都是只有个代码但还未成立的，没有其他数据就不用选出来了
    fundPos = get_wind_data(sqlStr)
    fundPos = pd.DataFrame(data=fundPos, columns=['Fund_Code', 'Date_In', 'Date_Out', 'Cur_Label'])
    fundPos['Date_In'] = pd.to_datetime(fundPos['Date_In'])
    fundPos['Date_Out'] = pd.to_datetime(fundPos['Date_Out'])

    if len(fundPos) > 0:
        days = get_trading_days(dateFrom, dateTo)
        days['Date'] = days.index
        repNum = fundPos.shape[0]
        repDaysNum = days.shape[0]
        daysRep = pd.concat([days] * repNum, ignore_index=True)
        daysRep.sort_values('Date', inplace=True)
        fundPosRep = pd.concat([fundPos] * repDaysNum, ignore_index=True)

        daysRep = daysRep.reset_index(drop=True)
        fundPosRep = fundPosRep.reset_index(drop=True)
        assert all(daysRep.index == fundPosRep.index)
        fundPosFull = pd.concat([daysRep, fundPosRep], axis=1)
        resPos = fundPosFull.loc[(fundPosFull['Date'] >= fundPosFull['Date_In']) &
                                 ((fundPosFull['Date'] < fundPosFull['Date_Out']) | (pd.isnull(fundPosFull['Date_Out']))), ['Date', 'Fund_Code']]
        resPos.reset_index(drop=True, inplace=True)
    else:
        resPos = pd.DataFrame([], columns=['Date', 'Fund_Code'])
    return resPos
