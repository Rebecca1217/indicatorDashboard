# -*- coding:UTF-8 -*-

# 数据来源：Wind数据库：中国A股交易日历AShareCalendar
# 本函数取的是上交所交易日期

import pandas as pd
from public.getDataSQL import get_data_sql
from datetime import datetime

def get_trading_days(dateFrom, dateTo):
    assert isinstance(dateFrom, str), 'dateFrom should be str'
    assert isinstance(dateTo, str), 'dateTo should  be str'

    exeStr = 'select TRADE_DAYS from dbo.ASHARECALENDAR where ' \
             'S_INFO_EXCHMARKET = \'SSE\' and TRADE_DAYS >= %s and TRADE_DAYS <= %s' % (dateFrom, dateTo)
    tradingDays = get_data_sql(exeStr, 'wind')
    tradingDays.columns=['Date']
    tradingDays['Date'] = pd.to_datetime(tradingDays['Date'])
    tradingDays = tradingDays.sort_values('Date')  # 等价于tradingDays.sort_value('Date', inplace=True)
    tradingDays.set_index('Date', inplace=True)

    return tradingDays


def get_trading_days2(dateFrom, dateTo, dateType):

    assert isinstance(dateFrom, str), 'dateFrom should be str'
    assert isinstance(dateTo, str), 'dateTo should  be str'

    # 因为月末涉及最后一天标签问题，所以往前往后多读一些
    dateFrom1 = pd.to_datetime(dateFrom) - pd.Timedelta(365, unit='D')
    dateTo1 = pd.to_datetime(dateTo) + pd.Timedelta(365, unit='D')
    dateFrom1 = datetime.strftime(dateFrom1, '%Y%m%d')
    dateTo1 = datetime.strftime(dateTo1, '%Y%m%d')

    dateType = dateType.lower()
    if dateType not in ('"week", "month", "quarter", "halfyear", "year"'):
        raise ValueError('wrong marktype, must be('
                         '"week", "month", "quarter", "halfyear", "year")')

    exeStr = 'select TRADE_DAYS from dbo.ASHARECALENDAR where ' \
             'S_INFO_EXCHMARKET = \'SSE\' and TRADE_DAYS >= %s and TRADE_DAYS <= %s' % (dateFrom1, dateTo1)
    tradingDays = get_data_sql(exeStr, 'wind')
    tradingDays.columns=['Date']
    tradingDays['Date'] = pd.to_datetime(tradingDays['Date'])
    tradingDays.sort_values('Date', inplace=True)
    tradingDays.reset_index(drop=True, inplace=True)

    # @2020.04.02貌似可以用x.is_year_end, x.is_quarter_end, x.is_month_end解决3个
    # 添加周末、月末、季末、年末标签
    # Week_Label需要做一个特殊处理，如果是1月份的Week标签是52，则年份需-1，12月份的Week标签是1，则年份需+1
    tradingDays['Year_Num'] = [x.year for x in tradingDays['Date']]
    tradingDays['Quarter_Num'] = [x.quarter for x in tradingDays['Date']]
    tradingDays['Month_Num'] = [x.month for x in tradingDays['Date']]
    tradingDays['Week_Num'] = [x.week for x in tradingDays['Date']]
    tradingDays.loc[tradingDays['Quarter_Num'] <= 2, 'Halfyear_Num'] = 1
    tradingDays.loc[tradingDays['Quarter_Num'] >= 3, 'Halfyear_Num'] = 2
    tradingDays['Halfyear_Num'] = tradingDays['Halfyear_Num'].astype(int)

    # 这里不用for 循环应该怎么调整：
    for i in range(len(tradingDays)):
        y = tradingDays['Year_Num'][i]
        m = tradingDays['Month_Num'][i]
        w = tradingDays['Week_Num'][i]
        if m == 1 and w == 52:
            tradingDays.loc[i, 'Year_Num'] = y - 1
        elif m == 12 and w == 1:
            tradingDays.loc[i, 'Year_Num'] = y + 1  # 这里直接调整Year_Num没关系，因为后面Year_Label的时候直接用Date.year，Year_Num后来没用了
    del y, m, w

    weekLabel = tradingDays['Year_Num'] * 100 + tradingDays['Week_Num']
    tradingDays['Week_Label'] = weekLabel
    tradingDays['Month_Label'] = [x.year * 100 + x.month for x in tradingDays['Date']]
    tradingDays['Quarter_Label'] = [x.year * 100 + x.quarter for x in tradingDays['Date']]
    tradingDays['Year_Label'] = [x.year for x in tradingDays['Date']]
    tradingDays['Halfyear_Label'] = [x.year * 100 for x in tradingDays['Date']] + tradingDays['Halfyear_Num']


    weekEnd = tradingDays.groupby('Week_Label')['Date'].last()
    monthEnd = tradingDays.groupby('Month_Label')['Date'].last()
    quarterEnd = tradingDays.groupby('Quarter_Label')['Date'].last()
    halfyearEnd = tradingDays.groupby('Halfyear_Label')['Date'].last()
    yearEnd = tradingDays.groupby('Year_Label')['Date'].last()
    tradingDays.loc[:, 'If_Week_End'] = [x in weekEnd.values for x in tradingDays['Date'].values]
    tradingDays.loc[:, 'If_Month_End'] = [x in monthEnd.values for x in tradingDays['Date'].values]
    tradingDays.loc[:, 'If_Quarter_End'] = [x in quarterEnd.values for x in tradingDays['Date'].values]
    tradingDays.loc[:, 'If_Halfyear_End'] = [x in halfyearEnd.values for x in tradingDays['Date'].values]
    tradingDays.loc[:, 'If_Year_End'] = [x in yearEnd.values for x in tradingDays['Date'].values]

    # 筛选需要的标签
    def selectData(i):
        switcher = {
            'week': tradingDays[['Date', 'Week_Label', 'If_Week_End']],
            'month': tradingDays[['Date', 'Month_Label', 'If_Month_End']],
            'quarter': tradingDays[['Date', 'Quarter_Label', 'If_Quarter_End']],
            'halfyear': tradingDays[['Date', 'Halfyear_Label', 'If_Halfyear_End']],
            'year': tradingDays[['Date', 'Year_Label', 'If_Year_End']]
        }
        return switcher.get(dateType, "Invalid day of week")

    res = selectData(dateType)
    res = res[(res['Date'] >= pd.to_datetime(dateFrom)) & (res['Date'] <= pd.to_datetime(dateTo))]
    res.set_index('Date', inplace=True)

    return res
