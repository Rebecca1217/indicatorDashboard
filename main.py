# -*- coding:UTF-8 -*-

# @created @2020.03.30

import pandas as pd
import numpy as np
from public.getWindData import get_wind_data
from public.getDataSQL import get_data_sql
from public.getIndexPos import get_index_pos
from public.getTradingDays import get_trading_days, get_trading_days2
from public.getStockFundUniv import get_stock_fund_univ
from public.attachSTKLabel import attach_stk_label
from public.getSWComponent import get_component_SW
from public.basicInfoSW import basic_info_SW
from public.attachDateLabel import attach_date_label
import datetime

############################################ 融资融券数据 ######################
# 目前只需要两融余额总量、融资买入额这两个数据
dateFrom = '20100331'
dateTo = datetime.datetime.strftime(datetime.datetime.today(), '%Y%m%d')
sqlStr = 'select TRADE_DT, S_MARSUM_EXCHMARKET, S_MARSUM_TRADINGBALANCE, S_MARSUM_SECLENDINGBALANCE, ' \
         'S_MARSUM_MARGINTRADEBALANCE, S_MARSUM_PURCHWITHBORROWMONEY from ' \
         'AShareMarginTradeSum where TRADE_DT >= {0} and TRADE_DT <= {1}  ' \
         'order by TRADE_DT, S_MARSUM_EXCHMARKET'.format(dateFrom, dateTo)
marginTD = get_wind_data(sqlStr)  # margin trading data
marginTD = pd.DataFrame(
    marginTD,
    columns=[
        'Date',
        'Mkt',
        'Buy_Balance',
        'Sell_Balance',
        'Margin_TD_Balance',
        'Buy'])  # Buy代表融资余额，Sell代表融券余额, Margin_TD_Balance是两融余额
marginTD['Date'] = pd.to_datetime(marginTD['Date'])
marginTD[['Buy_Balance',
          'Sell_Balance',
          'Margin_TD_Balance',
          'Buy']] = marginTD[['Buy_Balance',
                              'Sell_Balance',
                              'Margin_TD_Balance',
                              'Buy']].apply(lambda x: x.astype(float))
marginTD.sort_values(['Mkt', 'Date'], inplace=True)
marginTD['Buy_MA'] = marginTD.groupby('Mkt')['Buy'].rolling(
    window=10, min_periods=10).mean().values
marginTD.to_hdf(
    'dataForPlot/marginTDData.hdf',
    key='marginTD',
    type='w',
    format='table')

# A股每日成交量数据  注意A股成交金额单位是千元
sqlStr = 'select TRADE_DT, S_INFO_WINDCODE, right(S_INFO_WINDCODE, 2) as Mkt, S_DQ_AMOUNT * 1000 as Amount from AShareEODPrices where ' \
    'TRADE_DT >= {0} and TRADE_DT <= {1}'.format(datetime.datetime.strftime(pd.to_datetime(dateFrom) - datetime.timedelta(60), '%Y%m%d'),
                                                 dateTo)
tdAmount = get_wind_data(sqlStr)
tdAmount = pd.DataFrame(
    tdAmount,
    columns=[
        'Date',
        'Stock_Code',
        'Mkt',
        'TD_Amount'])
tdAmount['Date'] = pd.to_datetime(tdAmount['Date'])
tdAmount['TD_Amount'] = tdAmount['TD_Amount'].astype(float)
tdAmount.sort_values(['Date', 'Stock_Code'], inplace=True)
tdAmount.set_index(['Date', 'Stock_Code'], inplace=True)
tdAmount.to_hdf('tdAmount.hdf', key='tdAmount', type='w')
# tdAmount = pd.read_hdf('tdAmount.hdf')

mktAmount = pd.DataFrame(tdAmount.groupby(['Date', 'Mkt'])['TD_Amount'].sum())
mktAmount.sort_values(['Mkt', 'Date'], inplace=True)
mktAmount['TD_Amount_MA'] = mktAmount.groupby(
    'Mkt')['TD_Amount'].rolling(window=10, min_periods=10).mean().values
mktAmount = mktAmount[mktAmount.index.get_level_values(
    'Date') >= pd.to_datetime(dateFrom)].copy()

marginTDMkt = marginTD.copy()
marginTDMkt.loc[marginTDMkt['Mkt'] == 'SSE', 'Mkt'] = 'SH'
marginTDMkt.loc[marginTDMkt['Mkt'] == 'SZSE', 'Mkt'] = 'SZ'
marginTDMkt.set_index(['Date', 'Mkt'], inplace=True)
marAmountRatio = mktAmount.merge(
    marginTDMkt,
    how='left',
    left_index=True,
    right_index=True)
marAmountRatio['Ratio'] = marAmountRatio['Buy_MA'] / \
    marAmountRatio['TD_Amount_MA']  # 分市场融资交易MA占交易金额MA比重
marAmountRatio.to_hdf(
    'dataForPlot/marginTDData.hdf',
    key='marAmountRatio',
    type='w',
    format='table')
# 全市场要从头开始另算，因为不能MA_SH + MA+SZ = MA_Total
marAmountT = pd.DataFrame(tdAmount.groupby('Date')['TD_Amount'].sum())  # total
marAmountT.sort_values(['Date'], inplace=True)
marAmountT['TD_Amount_MA'] = marAmountT['TD_Amount'].rolling(
    window=10, min_periods=10).mean().values
marAmountT = marAmountT[marAmountT.index >= pd.to_datetime(dateFrom)].copy()
marAmountTRatio = marAmountT.merge(
    marginTD.groupby('Date')['Buy'].sum(),
    how='left',
    left_index=True,
    right_index=True)
marAmountTRatio['Buy_MA'] = marAmountTRatio['Buy'].rolling(
    window=10, min_periods=10).mean().values
marAmountTRatio['Ratio'] = marAmountTRatio['Buy_MA'] / \
    marAmountTRatio['TD_Amount_MA']
marAmountTRatio.to_hdf(
    'dataForPlot/marginTDData.hdf',
    key='marAmountTRatio',
    type='w',
    format='table')

# # print当前融资交易占比及历史分位数  2020.03.23, 9.14% ,历史分位数是58.62%，最新已经降下来了
# 广发的流动性跟踪这个数据是本周，以5天rolling计算的
# print('当前占比： ' + '{:.2%}'.format(marAmountTRatio['Ratio'][-1]))
# print('历史分位数： ' + '{:.2%}'.format(marAmountTRatio['Ratio'].rank()[-1] / len(mktAmountTRatio)))

############################################ 指数总交易量数据 ####################
# 从AIndexEODPrices读取的指数交易量数据，创业板指实际是创业板综，所以干脆都自己读成分股自己汇总
tdAmount.to_hdf('tdAmount.hdf', key='tdAmount', type='w', format='table')
tdDays60 = get_trading_days(
    datetime.datetime.strftime(
        pd.to_datetime(dateTo) -
        datetime.timedelta(160),
        '%Y%m%d'),
    dateTo)
dateFrom60 = datetime.datetime.strftime(
    tdDays60.index[len(tdDays60) - 60], '%Y%m%d')

# 这个不建议用Wind指数交易的表直接读取，那个数有些口径不对，创业板指提供的实际是创业板综

def get_index_amount(indexCode, dateFrom, dateTo):
    indexPos = get_index_pos(indexCode, dateFrom, dateTo)
    amount = indexPos.merge(
        tdAmount,
        how='left',
        left_index=True,
        right_index=True)
    res = amount.groupby('Date')['TD_Amount'].sum()
    res = pd.Series([int(round(x / 1e8, 0))
                     for x in res], index=res.index)
    return res

# tdAmount = pd.read_hdf('tdAmount.hdf', key='tdAmount')
# 上证综指成分股000001.SH
tdAmountSH = get_index_amount('000001.SH', dateFrom60, dateTo)
tdAmountSH = tdAmountSH[tdAmountSH != 0]  # 当天收盘前更新数据还没有
# 沪深300成分股000300.SH
tdAmount300 = get_index_amount('000300.SH', dateFrom60, dateTo)
tdAmount300 = tdAmount300[tdAmount300 != 0]
# 中证500成分股000905.SH
tdAmount500 = get_index_amount('000905.SH', dateFrom60, dateTo)
tdAmount500 = tdAmount500[tdAmount500 != 0]
# 创业板指成分股399006.SZ
tdAmountGEM = get_index_amount('399006.SZ', dateFrom60, dateTo)
tdAmountGEM = tdAmountGEM[tdAmountGEM != 0]
tdAmountSH.to_hdf('dataForPlot/indexTDAmount.hdf', key='tdAmountSH', type='w')
tdAmount300.to_hdf(
    'dataForPlot/indexTDAmount.hdf',
    key='tdAmount300',
    type='w')
tdAmount500.to_hdf(
    'dataForPlot/indexTDAmount.hdf',
    key='tdAmount500',
    type='w')
tdAmountGEM.to_hdf(
    'dataForPlot/indexTDAmount.hdf',
    key='tdAmountGEM',
    type='w')


###############################################基金仓位#######################
# 选取股票型和偏股混合型基金
# 问题：同一只基金的场内和场外是否需要只保留一个？
# 日期序列就是年中和年末
# @2020.04.03基金总股票仓位数据是每季度公布的，持股明细才是半年度，这里分开处理，股票总仓位按季度频率，分行业的按照半年度频率
dateFrom = '20100101'
dateTo = datetime.datetime.strftime(datetime.datetime.today(), '%Y%m%d')

def get_fund_pos(dateFrom, dateTo, ifFlexible):
    fundUniv = get_stock_fund_univ(
        dateFrom, dateTo, ifFlexible)  # 普通股票型+混合偏股型基金全集
    # 构造年中和年末日期序列
    dateSeqHY = get_trading_days2(dateFrom, dateTo, 'halfyear')
    dateSeqHY = dateSeqHY[dateSeqHY['If_Halfyear_End']]
    # 筛选年中和年末的基金universe  fundUnivHalfYear
    fundUnivHY = dateSeqHY.merge(
        fundUniv,
        how='left',
        on='Date',
        validate='one_to_many')
    fundUnivHY.drop(['Halfyear_Label', 'If_Halfyear_End'],
                    axis=1, inplace=True)

    # 季度日期序列
    dateSeqQ = get_trading_days2(dateFrom, dateTo, 'quarter')
    dateSeqQ = dateSeqQ[dateSeqQ['If_Quarter_End']]
    fundUnivQ = dateSeqQ.merge(
        fundUniv,
        how='left',
        on='Date',
        validate='one_to_many')
    fundUnivQ.drop(['Quarter_Label', 'If_Quarter_End'], axis=1, inplace=True)

    # 获取基金的仓位数据，join到univ上求均值即可
    sqlStr = 'select S_INFO_WINDCODE, F_PRT_ENDDATE, F_PRT_STOCKTONAV, F_PRT_NETASSET, F_PRT_STOCKVALUE from ' \
             'ChinaMutualFundAssetPortfolio where F_PRT_ENDDATE >= {0} and F_PRT_ENDDATE <= {1}' \
             'order by S_INFO_WINDCODE, F_PRT_ENDDATE'.format(dateFrom, dateTo)
    fundPosition = get_wind_data(sqlStr)
    fundPosition = pd.DataFrame(
        fundPosition,
        columns=[
            'Fund_Code',
            'Date',
            'Stock_Pos',
            'Fund_Value',
            'Stk_Value'])
    fundPosition['Date'] = pd.to_datetime(fundPosition['Date'])
    fundPosition['Stock_Pos'] = fundPosition['Stock_Pos'].astype(float)

    # Note: fundPosition的日期是报告日期，都是0630这种的，遇到0630不是交易日的就匹配不上 pd.merge_asof只能根据一个key，需要先split,比较麻烦
    # 按月份匹配，如果有多于1条的数据，则取日期最新的
    fundPosition['Month_Label'] = [
        x.year * 100 + x.month for x in fundPosition['Date']]
    fundUnivQ['Month_Label'] = [
        x.year * 100 + x.month for x in fundUnivQ['Date']]
    fundPosition.drop_duplicates(
        ['Fund_Code', 'Month_Label'], keep='last', inplace=True)
    fundPosUnivQ = fundUnivQ.merge(
        fundPosition, how='left', on=[
            'Month_Label', 'Fund_Code'], validate='one_to_one')
    #  去掉仓位低于40%的，这种一般是在建仓期
    fundPosUnivQ = fundPosUnivQ[(fundPosUnivQ['Stock_Pos'] >= 40) | (
        np.isnan(fundPosUnivQ['Stock_Pos']))].copy()
    fundPosUnivQ.sort_values(['Date_x', 'Fund_Code'], inplace=True)
    fundPosTotalQ = fundPosUnivQ.groupby('Date_x')['Stock_Pos'].mean()
    fundPosTotalQ.index.names = ['Date']  # 这个总数据没有日期问题，只要有数据的一定是全的，不会只是前10名仓位
    # 这个fundPosTotal就是所有偏股型基金总仓位的均值，是季度的，保存下来单独做一个表
    posTableT = fundPosTotalQ.iloc[(
        len(fundPosTotalQ) - 4): len(fundPosTotalQ)] / 100
    posTableT.index = [
        datetime.datetime.strftime(
            x, '%Y/%m/%d') for x in posTableT.index]
    posTableT = pd.DataFrame(posTableT).transpose()

    fundPosTotalHY = dateSeqHY.merge(fundPosTotalQ, how='left', on='Date')
    fundPosTotalHY.drop(
        ['Halfyear_Label', 'If_Halfyear_End'], axis=1, inplace=True)
    # 基金持仓的分行业仓位  季报公布的是证监会分类标准，所以这里用底仓自己计算
    # 分行业持仓，最新一期的有的基金公布年报，有的只公布的四季报，这种需要处理，处理时间？？？？？？？？？
    # 基金net asset要直接读，不能用行业仓位反除，四舍五入不准
    # 先筛选出股票型+偏股型基金的底仓，重复的只保留最新
    # 类别code和名称对应表：AShareIndustriesCode，参考用，查询数据不需要这个表
    sqlStr = 'select a.S_INFO_WINDCODE, S_INFO_STOCKWINDCODE, a.F_PRT_ENDDATE, F_PRT_STKVALUE, ' \
             'F_PRT_STKVALUETONAV, b.F_PRT_NETASSET from ' \
             'ChinaMutualFundStockPortfolio a ' \
             'left join ChinaMutualFundAssetPortfolio b ' \
             'on a.S_INFO_WINDCODE = b.S_INFO_WINDCODE and a.F_PRT_ENDDATE = b.F_PRT_ENDDATE ' \
             'where a.S_INFO_WINDCODE in (' \
             'select F_INFO_WINDCODE from ChinaMutualFundSector where S_INFO_SECTOR in ' \
             '(\'2001010201000000\', \'2001010101000000\', \'2001010204000000\')) ' \
             'and month(a.F_PRT_ENDDATE) in (6, 12) and ' \
             'a.F_PRT_ENDDATE >= {0} and a.F_PRT_ENDDATE <= {1}'.format(dateFrom, dateTo)
    # 大概筛选一下就可以，比不筛选稍快一点，也不用筛太精确，因为后面还会join到fundUniv上
    # 直接用一个sql语句不好写，因为entrydate exitdate一个基金对应多条数据，leftjoin 都给弄上了不好汇总
    # 这里面也有同一个报告期出现两次的，数据不一样，以最新日期为准，例如160512.SZ在20110623和20110630的两次只取20110630
    # 这个地方sql省不了，如果不写S_INFO_WINDCODE in (select F_INFO_WINDCODE from ChinaMutualFundSector where ' \
    #                  'S_INFO_SECTOR in (\'2001010201000000\', \'2001010101000000\', \'2001010204000000\'))只join s_info_sector的话会导致有重复数据
    # 这里选出来的数据，可能包含了一些中间符合标准，后期退出了的基金，不用担心，因为最后会再join到fundUniv上面
    fundPosDetail = get_data_sql(sqlStr, 'wind')
    fundPosDetail = pd.DataFrame(
        fundPosDetail,
        columns=[
            'Fund_Code',
            'Stock_Code',
            'Date',
            'Stk_Values',
            'Stk_Pct',
            'Fund_Value'])
    fundPosDetail['Date'] = pd.to_datetime(fundPosDetail['Date'])
    fundPosDetail[['Stk_Values', 'Stk_Pct', 'Fund_Value']] = fundPosDetail[
        ['Stk_Values', 'Stk_Pct', 'Fund_Value']].astype(float)
    # 摘出半年度的时间clear_version (不能直接fundPosDetail.drop_duplicates因为本来就有很多duplicates必须摘出时间序列再剔除)
    halfYearDate = pd.DataFrame(
        fundPosDetail['Date'].unique(),
        columns=['Date'])
    halfYearDate['Month_Label'] = [
        x.year * 100 + x.month for x in halfYearDate['Date']]
    halfYearDate.sort_values(['Date'], inplace=True)
    halfYearDate.drop_duplicates(['Month_Label'], keep='last', inplace=True)
    # tmp = fundPosDetail[[x in set(halfYearDate['Date']) for x in fundPosDetail['Date']]].copy() # 奇怪这个地方如果不加set就都是FALSE，而且这种筛选方式太慢
    # https://stackoverflow.com/questions/48553824/checking-if-a-date-falls-in-a-dataframe-pandas
    halfYearDate['Valid'] = True
    fundPosDetail = fundPosDetail.merge(
        halfYearDate[['Date', 'Valid']], on='Date', how='left')
    fundPosDetail.loc[pd.isnull(fundPosDetail['Valid']), 'Valid'] = False
    fundPosDetail = fundPosDetail[fundPosDetail['Valid']].copy()
    fundPosDetail.reset_index(drop=True, inplace=True)
    fundPosDetail['Month_Label'] = [
        x.year * 100 + x.month for x in fundPosDetail['Date']]
    fundUnivHY['Month_Label'] = [
        x.year * 100 + x.month for x in fundUnivHY['Date']]
    fundPosDetail = fundUnivHY.merge(
        fundPosDetail, how='left', on=[
            'Month_Label', 'Fund_Code'], validate='one_to_many')  # Date_x是交易日,Date_y是报告日
    fundPosDetail = fundPosDetail[[
        'Date_x', 'Fund_Code', 'Stock_Code', 'Stk_Values', 'Fund_Value']]
    fundPosDetail.columns = [
        'Date',
        'Fund_Code',
        'Stock_Code',
        'Stk_Values',
        'Fund_Value']

    #  贴行业标签计算行业权重
    swComponent = get_component_SW(dateFrom, dateTo)
    fundPosDetail = fundPosDetail.merge(
        swComponent, how='left', on=[
            'Date', 'Stock_Code'])
    uniqueNav = fundPosDetail.drop_duplicates(
        ['Date', 'Fund_Code'], keep='first')
    totalNav = pd.DataFrame(uniqueNav.groupby(
        'Date')['Fund_Value'].sum())  # 每天的基金总市值
    fundPosSector = pd.DataFrame(fundPosDetail.groupby(
        ['Date', 'SW_Code'])['Stk_Values'].sum())  # 每天分行业的总市值
    fundPosSector['SW_Code'] = fundPosSector.index.get_level_values('SW_Code')
    fundPosSector = fundPosSector.merge(totalNav, how='left', on='Date')
    fundPosSector['SW_Pct'] = fundPosSector['Stk_Values'] / \
        fundPosSector['Fund_Value']
    # 行业标签添加中文名称
    swPara = basic_info_SW()
    swPara['SW_Code'] = swPara['Wind_Code'] + '.SI'
    fundPosSector = fundPosSector.merge(
        swPara[['SW_Code', 'SW_Name']], how='left', on='SW_Code').set_index(fundPosSector.index)

    # 怎么判断当前的最新一期是否已经更新完数据，通过时间无法判断，只能通过结果反推，如果底仓汇总的结果和总数差异过大(>3%)，就说明还没更新完
    # 汇总的时候不要用行业汇总，直接用底仓汇总(事实上就算<3%了，一段时间应该也会继续更新缩小，diff最终应该小于1%)
    diff = pd.Series(fundPosDetail.groupby(['Date'])['Stk_Values'].sum() / totalNav['Fund_Value']) * 100 -\
           fundPosTotalHY['Stock_Pos']
    if abs(diff[-1]) > 3:
        fundPosSector = fundPosSector[fundPosSector.index <=
                                      fundPosSector.index.unique()[-2]]
        fundPosTotal = fundPosTotalHY[fundPosTotalHY.index <=
                                      fundPosTotalHY.index.unique()[-2]]
    assert all(fundPosSector.index.unique() == fundPosTotal.index.unique())
    # 总仓位和各行业配置比例变动，输出表格结果
    currPeriod = fundPosSector.index.unique()[-1]
    lastPeriod = fundPosSector.index.unique()[-2]
    sectorPctTable = fundPosSector.pivot_table(
        index='SW_Name',
        columns=fundPosSector.index,
        values='SW_Pct',
        aggfunc='first')
    # 本期 、本期相对上期变化，最终只显示当前还在用的行业
    sectorPctTable = sectorPctTable.merge(swPara.loc[swPara['Used_Label'] == 1, [
                                          'SW_Name', 'Used_Label']], how='left', on='SW_Name')
    sectorPctTable.sort_values(
        ['Used_Label', 'SW_Name'], ascending=False, inplace=True)  # 把已经不用的行业放在最下面
    sectorPctTable.reset_index(drop=True, inplace=True)  # 历史仓位变化图

    sectorPctTableCurr = sectorPctTable[[
        'SW_Name', lastPeriod, currPeriod, 'Used_Label']].copy()
    sectorPctTableCurr = sectorPctTableCurr[sectorPctTableCurr['Used_Label'] == 1]
    del sectorPctTableCurr['Used_Label']
    # 把总仓位加在最上面一排一起计算  本期和上期仓位变化这个表就不显示已经废弃的行业了
    totalPctCurr = pd.DataFrame(fundPosTotal).transpose()[
        [lastPeriod, currPeriod]] / 100
    totalPctCurr['SW_Name'] = '总仓位'
    totalPctCurr = totalPctCurr[['SW_Name', lastPeriod, currPeriod]]
    posTable = pd.concat([totalPctCurr, sectorPctTableCurr],
                         axis=0).reset_index(drop=True)
    posTable['Delta'] = posTable[currPeriod] - posTable[lastPeriod]
    del sectorPctTable['Used_Label']
    return [posTableT, posTable, sectorPctTable]

fundPosData = get_fund_pos(dateFrom, dateTo, False)  # 目前暂时还不需要跑True
posTableT = fundPosData[0]
posTable = fundPosData[1]
sectorPctTable = fundPosData[2]
posTableT.to_hdf('dataForPlot/fundPosData.hdf', key='posTableT', type='w', format='table')
posTable.to_hdf('dataForPlot/fundPosData.hdf', key='posTable', type='w', format='table')
sectorPctTable.to_hdf('dataForPlot/fundPosData.hdf', key='sectorPctTable', type='w', format='table')

######################################## 新发基金规模 ############################
dateFrom = '20150101'  # 最终只取过去3年数据
dateTo = datetime.datetime.strftime(datetime.datetime.today(), '%Y%m%d')

def get_new_fund_amount(dateFrom, dateTo, ifFlexible):
    # 因为要求MA，所以时间往前错一点
    dateFromTemp = datetime.datetime.strftime(pd.to_datetime(dateFrom) - datetime.timedelta(50), '%Y%m%d')
    dateSeq = get_trading_days(dateFromTemp, dateTo)
    dateSeq = attach_date_label(pd.DataFrame(dateSeq), 'month')
    dateSeq = attach_date_label(dateSeq, 'week')
    weekSeq = dateSeq[dateSeq['If_Week_End']]
    # 这是全部的时间，后面把新发基金的数据merge到这个时间上
    # 这个周度和月度是自然周和自然月，只在计算MA的分位数时候作为基准用
    monthSeq = dateSeq[dateSeq['If_Month_End']]

    sqlStr = 'select a.F_INFO_WINDCODE, b.S_INFO_SECTOR, F_INFO_SETUPDATE, ' \
             'a.F_ISSUE_TOTALUNIT * a.F_INFO_PARVALUE as Collection, ' \
             'F_INFO_BENCHMARK  from ChinaMutualFundDescription a ' \
             'left join ChinaMutualFundSector b ' \
             'on a.F_INFO_WINDCODE = b.F_INFO_WINDCODE ' \
             'where b.S_INFO_SECTOR in ' \
             '(\'2001010201000000\', \'2001010101000000\', \'2001010204000000\')'

    stkFund = get_wind_data(sqlStr)
    stkFund = pd.DataFrame(
        stkFund,
        columns=['Fund_Code', 'Info_Sector', 'Setup_Date', 'Collection', 'Bchmk'])
    stkFund['Setup_Date'] = pd.to_datetime(stkFund['Setup_Date'])
    stkFund['Collection'] = stkFund['Collection'].astype(float)

    stkFundStk = stkFund[stkFund['Info_Sector'] != '2001010204000000']
    stkFundMix = stkFund[stkFund['Info_Sector'] == '2001010204000000']
    if ifFlexible:
        stkFundMix = attach_stk_label(stkFundMix)[0]
        stkFundMix.drop('Stk_Weight', axis=1, inplace=True)
        stkFund = pd.concat([stkFundStk, stkFundMix],
                            axis=0, ignore_index=True)
        del stkFundStk, stkFundMix
    else:
        stkFund = stkFundStk.copy()
        del stkFundStk

    stkFund.sort_values('Setup_Date', inplace=True)
    stkFund['Date'] = stkFund['Setup_Date']
    stkFund.set_index('Date', inplace=True)
    # 07年之前的有些基金setup date是非交易日，不影响最近3年可不处理
    stkFundWeek = attach_date_label(stkFund, 'week')
    stkFundMonth = attach_date_label(stkFund, 'month')
    weekCount = stkFundWeek.groupby('Week_Label')['Collection'].sum()
    monthCount = stkFundMonth.groupby('Month_Label')['Collection'].sum()

    weekCount = weekSeq.merge(
        pd.DataFrame(weekCount),
        how='left',
        on='Week_Label').set_index(
        weekSeq.index)  # 做这一步merge因为有的周没有数据，应该取0处理，月度也需要（虽然月度还没有0的情况）
    # 这里merge完了Week_Label不知怎么就变成object了，需要调整一下，不然存hdf会报警告
    # weekCount['Week_Label'] = weekCount['Week_Label'].astype(int)
    weekCount = weekCount[['Week_Label', 'Collection']]
    monthCount = monthSeq.merge(
        pd.DataFrame(monthCount),
        how='left',
        on='Month_Label').set_index(
        monthSeq.index)
    # monthCount['Month_Label'] = monthCount['Month_Label'].astype(int)
    monthCount = monthCount[['Month_Label', 'Collection']]
    weekCount['Collection'] = weekCount['Collection'].fillna(0)
    monthCount['Collection'] = monthCount['Collection'].fillna(0)

    # 筛选出过去3年的部分
    dateFrom3Year = datetime.datetime.strftime(
        pd.to_datetime(dateTo) - datetime.timedelta(365 * 3), '%Y%m%d')
    weekCount = weekCount[weekCount.index >= pd.to_datetime(dateFrom3Year)]
    monthCount = monthCount[monthCount.index >= pd.to_datetime(dateFrom3Year)] # 求MA所处分位数的基准序列 画图（除最后一跟MA柱以外的部分）
    # 下面求5天MA和20天MA
    stkFundSum = stkFund.groupby('Date')['Collection'].sum()
    stkFundSum = dateSeq.merge(stkFundSum, how='left', on='Date')
    stkFundSum = pd.DataFrame(stkFundSum['Collection'])
    stkFundSum['Collection'] = stkFundSum['Collection'].fillna(0)

    stkFundSum['MA_5'] = stkFundSum['Collection'].rolling(window=5, min_periods=5).sum()
    stkFundSum['MA_20'] = stkFundSum['Collection'].rolling(window=20, min_periods=20).sum()

    # 用来画柱状图的序列
    if stkFundSum.index[-1] > weekCount.index[-1]:
        weekCount = pd.concat([weekCount[['Collection']],
                                pd.DataFrame(stkFundSum.iloc[len(stkFundSum) - 1, :]).transpose()[['MA_5']].rename(columns={'MA_5':'Collection'})], axis=0)
    else:
        weekCount = weekCount[['Collection']]
    if stkFundSum.index[-1] > monthCount.index[-1]:
        monthCount = pd.concat([monthCount[['Collection']],
                                pd.DataFrame(stkFundSum.iloc[len(stkFundSum) - 1, :]).transpose()[['MA_20']].rename(columns={'MA_20':'Collection'})], axis=0)
    else:
        monthCount = monthCount[['Collection']]

    # 输出4个数值：过去一周和一月新发规模数据（两个数值）和对应在过去三年上的历史分位数（两个数值）
    res1 = stkFundSum['MA_5'][-1]
    res2 = stkFundSum['MA_20'][-1]
    res3 = weekCount['Collection'].rank(pct=True)[-1]
    res4 = monthCount['Collection'].rank(pct=True)[-1]
    fundRes = pd.DataFrame({'过去一周新发规模': '{:.1f}亿'.format(res1),
                            '过去一月新发规模': '{:.1f}亿'.format(res2),
                            '过去一周新发规模分位数': '{:.1%}'.format(res3),
                            '过去一月新发规模分位数': '{:.1%}'.format(res4)},
                           index=['value'])
    return [fundRes, weekCount, monthCount]

newFundDataExcl = get_new_fund_amount(dateFrom, dateTo, False)
newFundDataIncl = get_new_fund_amount(dateFrom, dateTo, True)
fundResExcl = newFundDataExcl[0]
weekCountExcl = newFundDataExcl[1]
monthCountExcl = newFundDataExcl[2]
fundResIncl = newFundDataIncl[0]
weekCountIncl = newFundDataIncl[1]
monthCountIncl = newFundDataIncl[2]
fundResExcl.to_hdf('dataForPlot/newFundData.hdf', key='fundResExcl', type='w')
weekCountExcl.to_hdf('dataForPlot/newFundData.hdf', key='weekCountExcl', type='w')
monthCountExcl.to_hdf('dataForPlot/newFundData.hdf', key='monthCountExcl', type='w')
fundResIncl.to_hdf('dataForPlot/newFundData.hdf', key='fundResIncl', type='w')
weekCountIncl.to_hdf('dataForPlot/newFundData.hdf', key='weekCountIncl', type='w')
monthCountIncl.to_hdf('dataForPlot/newFundData.hdf', key='monthCountIncl', type='w')
###############################################IPO规模###############################

dateFrom = '20150101'  # 最终只取过去3年数据
dateTo = datetime.datetime.strftime(datetime.datetime.today(), '%Y%m%d')
dateSeq = get_trading_days(dateFrom, dateTo)
dateSeq = attach_date_label(pd.DataFrame(dateSeq), 'month')
# 当月显示月初至今，需要对日期的最后一个If_Month_End调整成True
dateSeq.iloc[-1, dateSeq.columns.get_loc('If_Month_End')] = True

# 这是全部的时间，后面把新发基金的数据merge到这个时间上
monthSeq = dateSeq[dateSeq['If_Month_End']].copy()

sqlStr = 'select S_INFO_WINDCODE, S_IPO_COLLECTION * 10000 as Collection, S_IPO_LISTDATE from AShareIPO ' \
         'where S_IPO_LISTDATE >= {0} ' \
         'order by S_IPO_LISTDATE'.format(dateFrom)
stkUniv = get_wind_data(sqlStr)
stkUniv = pd.DataFrame(
    stkUniv,
    columns=[
        'Stock_Code',
        'Collection',
        'List_Date'])
stkUniv['List_Date'] = pd.to_datetime(stkUniv['List_Date'])
stkUniv['Collection'] = stkUniv['Collection'].astype(float)
stkUniv['Date'] = stkUniv['List_Date']
stkUniv.set_index('Date', inplace=True)
stkMonth = attach_date_label(stkUniv, 'month')
monthCountIPO = stkMonth.groupby('Month_Label')['Collection'].sum()

monthCountIPO = monthSeq.merge(
    pd.DataFrame(monthCountIPO),
    how='left',
    on='Month_Label').set_index(
        monthSeq.index)
monthCountIPO = monthCountIPO[['Month_Label', 'Collection']]
monthCountIPO['Collection'] = monthCountIPO['Collection'].fillna(0)

# 筛选出过去3年的部分
dateFrom3Year = datetime.datetime.strftime(
    pd.to_datetime(dateTo) - datetime.timedelta(365 * 3), '%Y%m%d')
monthCountIPO = monthCountIPO[monthCountIPO.index >=
                              pd.to_datetime(dateFrom3Year)]
monthCountIPO.drop('Month_Label', axis=1, inplace=True)
monthCountIPO.to_hdf(
    'dataForPlot/monthCountIPO.hdf',
    key='monthCountIPO',
    type='w',
    format='table')

###############################################A/H溢价######################
dateFrom = '20060101'  # 从有数据开始
dateTo = datetime.datetime.strftime(datetime.datetime.today(), '%Y%m%d')
sqlStr = 'select  S_INFO_WINDCODE, TRADE_DT, S_DQ_CLOSE from HKIndexEODPrices ' \
         'where S_INFO_WINDCODE = \'HSAHP.HI\' ' \
         'and TRADE_DT >= {0} and TRADE_DT <= {1} ' \
         'order by TRADE_DT'.format(dateFrom, dateTo)
dataAH = pd.DataFrame(
    get_wind_data(sqlStr),
    columns=[
        'Index_Code',
        'Date',
        'Close'])
dataAH['Date'] = pd.to_datetime(dataAH['Date'])
dataAH['Close'] = dataAH['Close'].astype(float)
dataAH.set_index('Date', inplace=True)
dataAH['Close_Pct'] = dataAH['Close'].expanding(min_periods=1).apply(
    lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=True)
dataAH.to_hdf('dataForPlot/dataAH.hdf', key='dataAH', type='w')
