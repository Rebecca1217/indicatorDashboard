# -*- coding:UTF-8 -*-

import pandas as pd
from datetime import datetime
from public.getTradingDays import get_trading_days2

# 输入一个含有日期的dataframe，为其添加month_label, week_label, year_label, if_month_end, if_week_end, if_year_end
# Date 需要是InputDF的index
def attach_date_label(inputDF, dateType):
    assert 'Date' in inputDF.index.names, 'input DataFrame should contain "Date!"'
    dateType = dateType.lower()
    if dateType not in ('"week", "month", "quarter", "year"'):
        raise ValueError('wrong marktype, must be('
                         '"week", "month", "quarter", "year")')

    dateSeq = pd.to_datetime(inputDF.index.get_level_values('Date'))
    dateFrom = datetime.strftime(dateSeq.min(), '%Y%m%d')
    dateTo = datetime.strftime(dateSeq.max(), '%Y%m%d')

    attachLabel = get_trading_days2(dateFrom, dateTo, dateType)

    res = inputDF.merge(attachLabel, how='left', on='Date').set_index(inputDF.index)
    def repective_col(i):
        switcher = {
            'week': 'Week_Label',
            'month': 'Month_Label',
            'quarter': 'Quarter_Label',
            'halfyear': 'Halfyear_Label',
            'year': 'Year_Label'
        }
        return switcher.get(dateType, "Invalid day of week")
    col = repective_col(dateType)
    res[col] = res[col].astype('Int64')

    return res
