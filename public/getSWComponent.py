# -*- coding:UTF-8 -*-

# 获取申万行业和股票代码对照表(SWIndexMembers)
def get_component_SW(dateFrom, dateTo):
    from public.getWindData import get_wind_data
    from public.getTradingDays import get_trading_days
    import pandas as pd
    # 获取全集，如果需要某个单独行业再自行筛选
    exeStr = 'select s_info_windcode, S_CON_WINDCODE, S_CON_INDATE, S_CON_OUTDATE from ' \
             'SWIndexMembers where S_INFO_WINDCODE in ' \
             '(select INDUSTRIESALIAS + \'.SI\' from ASHAREINDUSTRIESCODE where ' \
             'LEVELNUM = 2 and INDUSTRIESCODE like \'61%\') and ' \
             '(S_CON_INDATE <= {} and (S_CON_OUTDATE >= {} or S_CON_OUTDATE is null))'.format(dateTo, dateFrom)
    # Note: 为什么这个地方Indate和Outdate都有等于号：纳入是当天的0点就开始，而剔除的话是截止到剔除日期的24:00结束

    swComponent = get_wind_data(exeStr)
    swComponent = pd.DataFrame(swComponent, columns=['SW_Code', 'Stock_Code', 'In_Date', 'Out_Date'])
    swComponent['In_Date'] = pd.to_datetime(swComponent['In_Date'])
    swComponent['Out_Date'] = pd.to_datetime(swComponent['Out_Date'])

    if len(swComponent) > 0:
        # 调整指数数据格式为每天持仓
        # 先repmat全集出来，再根据时间筛选符合要求的
        repNum = swComponent.shape[0]
        tradingDays = get_trading_days(dateFrom, dateTo)
        tradingDays['Date'] = tradingDays.index
        repDaysNum = tradingDays.shape[0]
        tradingDays = pd.concat([tradingDays] * repNum, ignore_index=True)
        tradingDays.sort_values('Date', inplace=True)
        swComponentRep = pd.concat([swComponent] * repDaysNum, ignore_index=True)

        tradingDays = tradingDays.reset_index(drop=True)
        swComponentRep = swComponentRep.reset_index(drop=True)
        assert all(tradingDays.index == swComponentRep.index)
        swComponentFull = pd.concat([tradingDays, swComponentRep], axis=1)

        swComponentFull['Valid'] = (swComponentFull['Date'] >= swComponentFull['In_Date']) & \
                                ((swComponentFull['Date'] <= swComponentFull['Out_Date']) |
                                 (swComponentFull['Out_Date'].isnull()))  # 当天进当天出
        swComponent = swComponentFull[swComponentFull['Valid']]
        swComponent.reset_index(drop=True, inplace=True)  # 数据筛选后必须reset index 不然index还是原来indexPosFull的
        # # check NUM
        # count = indexPos.groupby(by = ['Date'])['Stock_Code'].count()
        # assert all((count <= 300) & (count >= 295))

        swComponent = swComponent[['SW_Code', 'Date', 'Stock_Code']]
    else:
        swComponent = pd.DataFrame([], columns=['SW_Code', 'Date', 'Stock_Code'])

    swComponent.set_index(['Date', 'Stock_Code'], inplace=True)

    return swComponent


# 下面这个函数废弃了，ASWSINDEXCLOSEWEIGHT数据有很多异常缺失问题，不到万不得已不用这个表
# #  获取申万行业和股票代码对照表（ASWSINDEXCLOSEWEIGHT）
# def getSWComponentOld(dateFrom, dateTo):
#     from public.get_wind_data import get_wind_data
#     import pandas as pd
#     # 获取全集，如果需要某个单独行业再自行筛选
#
#     exeStr = 'select S_INFO_WINDCODE, S_CON_WINDCODE, TRADE_DT, I_WEIGHT ' \
#              'from dbo.ASWSINDEXCLOSEWEIGHT where ' \
#              'TRADE_DT >= {} and TRADE_DT <= {} and ' \
#              'S_INFO_WINDCODE in (select INDUSTRIESALIAS + \'.SI\' from ASHAREINDUSTRIESCODE where ' \
#              'LEVELNUM = 2 and INDUSTRIESCODE like \'61%\')'.format(dateFrom, dateTo)
#     swWeight = get_wind_data(exeStr)
#     swWeight = pd.DataFrame(data=swWeight, columns=['SW_Code', 'Stock_Code', 'Date', 'Weight'])
#
#     return(swWeight)
