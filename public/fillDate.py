# -*- coding:UTF-8 -*-
from public.getTradingDays import get_trading_days
from datetime import datetime
import pandas as pd

def fill_date(inputData, dateF, dateT, dataKey):

    fullDate = get_trading_days(dateF, dateT)
    fullSector = inputData.index.get_level_values(dataKey).unique()
    fullSector = fullSector[~pd.isnull(fullSector)]
    # 先rempat一个全集出来
    repNum = len(fullSector)
    repDaysNum = len(fullDate)
    fullDate['Date'] = fullDate.index
    fullDate = pd.concat([fullDate] * repNum, ignore_index=True)
    fullDate.sort_values('Date', inplace=True)
    fullDate = fullDate.reset_index(drop=True)

    fullSectorRep = pd.concat([pd.Series(fullSector)] * repDaysNum, ignore_index=True)
    fullSectorRep = fullSectorRep.reset_index(drop=True)

    assert len(fullDate) == len(fullSectorRep)
    dateSectorFull = pd.concat([fullDate, fullSectorRep], axis=1)
    dateSectorFull.set_index(['Date', 'SW_Code'], inplace=True)

    return dateSectorFull