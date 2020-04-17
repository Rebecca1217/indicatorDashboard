# -*- coding:UTF-8 -*-
import pandas as pd
from public.getTradingDays import get_trading_days
from public.getWindData import get_wind_data
from public.attachSTKLabel import attach_stk_label

# @2020.04.12这个函数暂时不调整，因为ChinaMutualFundNAV基金缺失一些日期


def get_stock_fund_univ(dateFrom, dateTo, ifFlexible):
    # 2001010201000000 偏股混合型, 2001010101000000 普通股票型, 2001010204000000 灵活配置型
    # ifFlexible = True表示否包含灵活配置型中的实际偏股型基金
    # 这个地方从ChinaMutualFundDescription还是从ChinaMutualFundSector读取作为table a都没关系
    # 结果是一样的，因为之前不涉及读取bchmk的数据所以直接从表ChinaMutualFundSector读的
    sqlStr = 'select a.F_INFO_WINDCODE, b.S_INFO_SECTORENTRYDT, b.S_INFO_SECTOREXITDT, ' \
             'b.S_INFO_SECTOR, a.F_INFO_BENCHMARK from ChinaMutualFundDescription a ' \
             'left join ChinaMutualFundSector b ' \
             'on a.F_INFO_WINDCODE = b.F_INFO_WINDCODE ' \
             'where b.S_INFO_SECTOR in ' \
             '(\'2001010201000000\', \'2001010101000000\', \'2001010204000000\') and ' \
             'b.S_INFO_SECTORENTRYDT <= {0} and ' \
             '(b.S_INFO_SECTOREXITDT >= {1} or b.S_INFO_SECTOREXITDT is null) ' \
             'order by F_INFO_WINDCODE, S_INFO_SECTORENTRYDT'.format(dateTo, dateFrom)
    # entrydt is null and exitdt is null and cur_sign =
    # 1这种都是只有个代码但还未成立的，没有其他数据就不用选出来了
    fundInfo = get_wind_data(sqlStr)
    fundInfo = pd.DataFrame(
        data=fundInfo,
        columns=[
            'Fund_Code',
            'Date_In',
            'Date_Out',
            'Info_Sector',
            'Bchmk'])
    fundInfo['Date_In'] = pd.to_datetime(fundInfo['Date_In'])
    fundInfo['Date_Out'] = pd.to_datetime(fundInfo['Date_Out'])

    # 对零配型基金贴偏股或非偏股标签，把零配中的非股票先排除，再进行后面的时间对应操作
    fundInfoStk = fundInfo[fundInfo['Info_Sector'] != '2001010204000000']
    if ifFlexible:
        fundInfoMix = fundInfo[fundInfo['Info_Sector'] == '2001010204000000']
        fundInfoMix = attach_stk_label(fundInfoMix)[0]
        fundInfoMix.drop('Stk_Weight', axis=1, inplace=True)
        fundInfo = pd.concat([fundInfoStk, fundInfoMix],
                             axis=0, ignore_index=True)
    else:
        fundInfo = fundInfoStk.copy()

    if len(fundInfo) > 0:
        days = get_trading_days(dateFrom, dateTo)
        days['Date'] = days.index
        repNum = fundInfo.shape[0]
        repDaysNum = days.shape[0]
        daysRep = pd.concat([days] * repNum, ignore_index=True)
        daysRep.sort_values('Date', inplace=True)
        fundPosRep = pd.concat([fundInfo] * repDaysNum, ignore_index=True)

        daysRep = daysRep.reset_index(drop=True)
        fundPosRep = fundPosRep.reset_index(drop=True)
        assert all(daysRep.index == fundPosRep.index)
        fundPosFull = pd.concat([daysRep, fundPosRep], axis=1)
        resPos = fundPosFull.loc[(fundPosFull['Date'] >= fundPosFull['Date_In']) & (
            (fundPosFull['Date'] <= fundPosFull['Date_Out']) | (pd.isnull(fundPosFull['Date_Out']))), ['Date', 'Fund_Code', 'Info_Sector']]
        resPos.reset_index(drop=True, inplace=True)
    else:
        resPos = pd.DataFrame([], columns=['Date', 'Fund_Code'])
    return resPos

# tst2 = get_wind_data('select PRICE_DATE, a.F_INFO_WINDCODE,b.S_INFO_SECTOR from ChinaMutualFundNAV a '
#                      'left join (select F_INFO_WINDCODE, S_INFO_SECTOR, S_INFO_SECTORENTRYDT, '
#                      'isnull(s_info_sectorexitdt, \'99991231\') as S_INFO_SECTOREXITDT from '
#                      'ChinaMutualFundSector) b '
#                      'on a.F_INFO_WINDCODE = b.F_INFO_WINDCODE and a.PRICE_DATE >= b.S_INFO_SECTORENTRYDT '
#                      'and a.PRICE_DATE <= b.S_INFO_SECTOREXITDT where b.S_INFO_SECTOR in '
#                      '(\'2001010201000000\', \'2001010101000000\', \'2001010204000000\') and '
#                      'a.PRICE_DATE >= 20100101 and a.PRICE_DATE <= 20200410 '
#                      'order by PRICE_DATE, F_INFO_WINDCODE')
# tst2 = pd.DataFrame(tst2, columns=['Date', 'Fund_Code', 'Sector_Code'])
