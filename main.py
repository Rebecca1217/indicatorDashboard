# -*- coding:UTF-8 -*-

# @created @2020.03.30

import pandas as pd
import numpy as np
from public.getWindData import get_wind_data
from public.getIndexPos import get_index_pos
from public.getTradingDays import get_trading_days, get_trading_days2
from public.getStockFundPos import get_stock_fund_pos
from public.getSWComponent import get_component_SW
from public.basicInfoSW import basic_info_SW
from public.attachDateLabel import attach_date_label
import datetime


############################################ 融资融券数据 ###########################################
# 目前只需要两融余额总量、融资买入额这两个数据
dateFrom = '20100331'
dateTo = datetime.datetime.strftime(datetime.datetime.today(), '%Y%m%d')
sqlStr = 'select TRADE_DT, S_MARSUM_EXCHMARKET, S_MARSUM_TRADINGBALANCE, S_MARSUM_SECLENDINGBALANCE, ' \
         'S_MARSUM_MARGINTRADEBALANCE, S_MARSUM_PURCHWITHBORROWMONEY from ' \
         'AShareMarginTradeSum where TRADE_DT >= {0} and TRADE_DT <= {1}  ' \
         'order by TRADE_DT, S_MARSUM_EXCHMARKET'.format(dateFrom, dateTo)
marginTD = get_wind_data(sqlStr)  # margin trading data
marginTD = pd.DataFrame(marginTD, columns=['Date', 'Mkt', 'Buy_Balance', 'Sell_Balance', 'Margin_TD_Balance', 'Buy'])  # Buy代表融资余额，Sell代表融券余额, Margin_TD_Balance是两融余额
marginTD['Date'] = pd.to_datetime(marginTD['Date'])
marginTD[['Buy_Balance', 'Sell_Balance', 'Margin_TD_Balance', 'Buy']] = marginTD[['Buy_Balance', 'Sell_Balance', 'Margin_TD_Balance', 'Buy']].apply(lambda x: x.astype(float))
marginTD.sort_values(['Mkt', 'Date'], inplace=True)
marginTD['Buy_MA'] = marginTD.groupby('Mkt')['Buy'].rolling(window=10, min_periods=10).mean().values
marginTD.to_hdf('dataForPlot/marginTD.hdf', key='marginTD', type='w')

# A股每日成交量数据  注意A股成交金额单位是千元
sqlStr = 'select TRADE_DT, S_INFO_WINDCODE, right(S_INFO_WINDCODE, 2) as Mkt, S_DQ_AMOUNT * 1000 as Amount from AShareEODPrices where ' \
        'TRADE_DT >= {0} and TRADE_DT <= {1}'.format(datetime.datetime.strftime(pd.to_datetime(dateFrom) - datetime.timedelta(60), '%Y%m%d'),
                                                     dateTo)
tdAmount = get_wind_data(sqlStr)
tdAmount = pd.DataFrame(tdAmount, columns=['Date', 'Stock_Code', 'Mkt', 'TD_Amount'])
tdAmount['Date'] = pd.to_datetime(tdAmount['Date'])
tdAmount['TD_Amount'] = tdAmount['TD_Amount'].astype(float)
tdAmount.sort_values(['Date', 'Stock_Code'], inplace=True)
tdAmount.set_index(['Date', 'Stock_Code'], inplace=True)
tdAmount.to_hdf('tdAmount.hdf', key='tdAmount', type='w')
# tdAmount = pd.read_hdf('tdAmount.hdf')

mktAmount = pd.DataFrame(tdAmount.groupby(['Date','Mkt'])['TD_Amount'].sum())
mktAmount.sort_values(['Mkt', 'Date'], inplace=True)
mktAmount['TD_Amount_MA'] = mktAmount.groupby('Mkt')['TD_Amount'].rolling(window=10, min_periods=10).mean().values
mktAmount = mktAmount[mktAmount.index.get_level_values('Date') >= pd.to_datetime(dateFrom)].copy()

marginTDMkt = marginTD.copy()
marginTDMkt.loc[marginTDMkt['Mkt'] == 'SSE', 'Mkt'] = 'SH'
marginTDMkt.loc[marginTDMkt['Mkt'] == 'SZSE', 'Mkt'] = 'SZ'
marginTDMkt.set_index(['Date', 'Mkt'], inplace=True)
marAmountRatio = mktAmount.merge(marginTDMkt, how='left', left_index=True, right_index=True)
marAmountRatio['Ratio'] = marAmountRatio['Buy_MA'] / marAmountRatio['TD_Amount_MA'] # 分市场融资交易MA占交易金额MA比重
marAmountRatio.to_hdf('dataForPlot/marAmountRatio.hdf', key='marAmountRatio', type='w')
# 全市场要从头开始另算，因为不能MA_SH + MA+SZ = MA_Total
marAmountT = pd.DataFrame(tdAmount.groupby('Date')['TD_Amount'].sum())  # total
marAmountT.sort_values(['Date'], inplace=True)
marAmountT['TD_Amount_MA'] = marAmountT['TD_Amount'].rolling(window=10, min_periods=10).mean().values
marAmountT = marAmountT[marAmountT.index >= pd.to_datetime(dateFrom)].copy()
marAmountTRatio = marAmountT.merge(marginTD.groupby('Date')['Buy'].sum(), how='left', left_index=True, right_index=True)
marAmountTRatio['Buy_MA'] = marAmountTRatio['Buy'].rolling(window=10, min_periods=10).mean().values
marAmountTRatio['Ratio'] = marAmountTRatio['Buy_MA'] / marAmountTRatio['TD_Amount_MA']
marAmountTRatio.to_hdf('dataForPlot/marAmountTRatio.hdf', key='marAmountTRatio', type='w')

# # print当前融资交易占比及历史分位数  2020.03.23, 9.14% ,历史分位数是58.62%，最新已经降下来了
## 广发的流动性跟踪这个数据是本周，以5天rolling计算的
# print('当前占比： ' + '{:.2%}'.format(marAmountTRatio['Ratio'][-1]))
# print('历史分位数： ' + '{:.2%}'.format(marAmountTRatio['Ratio'].rank()[-1] / len(mktAmountTRatio)))


############################################ 指数总交易量数据 ########################################
# 从AIndexEODPrices读取的指数交易量数据，创业板指实际是创业板综，所以干脆都自己读成分股自己汇总

tdDays20 = get_trading_days(datetime.datetime.strftime(pd.to_datetime(dateTo) - datetime.timedelta(60), '%Y%m%d'), dateTo)
dateFrom20 = datetime.datetime.strftime(tdDays20.index[len(tdDays20) - 20], '%Y%m%d')

# 这个可以直接读，不用自己算
def get_index_amount(index_code, dateFrom, dateTo):
    indexPos = get_index_pos(index_code, dateFrom, dateTo)
    amount = indexPos.merge(tdAmount, how='left', left_index=True, right_index=True)
    res = amount.groupby('Date')['TD_Amount'].sum()
    res = pd.Series([int(round(x / 100000000, 0)) for x in res], index=res.index)
    return res

# 上证综指成分股000001.SH
tdAmountSH = get_index_amount('000001.SH', dateFrom20, dateTo)
tdAmountSH.to_hdf('dataForPlot/tdAmountSH.hdf', key='tdAmountSH', type='w')
# 沪深300成分股000300.SH
tdAmount300 = get_index_amount('000300.SH', dateFrom20, dateTo)
tdAmount300.to_hdf('dataForPlot/tdAmount300.hdf', key='tdAmount300', type='w')
# 中证500成分股000905.SH
tdAmount500 = get_index_amount('000905.SH', dateFrom20, dateTo)
tdAmount500.to_hdf('dataForPlot/tdAmount500.hdf', key='tdAmount500', type='w')
# 创业板指成分股399006.SZ
tdAmountGEM = get_index_amount('399006.SZ', dateFrom20, dateTo)
tdAmountGEM.to_hdf('dataForPlot/tdAmountGEM.hdf', key='tdAmountGEM', type='w')

###############################################基金仓位###############################################
# 选取股票型和偏股混合型基金
# 问题：同一只基金的场内和场外是否需要只保留一个？
# 日期序列就是年中和年末
# @2020.04.03基金总股票仓位数据是每季度公布的，持股明细才是半年度，这里需要修改一下
dateFrom = '20100101'
dateTo = datetime.datetime.strftime(datetime.datetime.today(), '%Y%m%d')
dateSeq = get_trading_days(dateFrom, dateTo)
dateSeq['Year_Month'] = dateSeq.index.year * 100 + dateSeq.index.month
fundUniv = get_stock_fund_pos(dateFrom, dateTo)   # 普通股票型+混合偏股型基金全集
# 构造年中和年末日期序列
dateSeq = get_trading_days2(dateFrom, dateTo, 'halfyear')
dateSeq = dateSeq[dateSeq['If_Halfyear_End']]
# 筛选年中和年末的基金universe  fundUnivHalfYear
fundUnivHY = fundUniv[[x in dateSeq.index for x in fundUniv['Date']]].copy()
fundUnivHY.reset_index(drop=True, inplace=True)
# 获取基金的仓位数据，join到univ上求均值即可
sqlStr = 'select S_INFO_WINDCODE, F_PRT_ENDDATE, F_PRT_STOCKTONAV, F_PRT_NETASSET, F_PRT_STOCKVALUE from ' \
         'ChinaMutualFundAssetPortfolio where F_PRT_ENDDATE >= {0} and F_PRT_ENDDATE <= {1}'.format(dateFrom, dateTo)
fundPosition = get_wind_data(sqlStr)
fundPosition = pd.DataFrame(fundPosition, columns=['Fund_Code', 'Date', 'Stock_Pos', 'Fund_Value', 'Stk_Value'])
fundPosition['Date'] = pd.to_datetime(fundPosition['Date'])
fundPosition['Stock_Pos'] = fundPosition['Stock_Pos'].astype(float)

# Note: fundPosition的日期是报告日期，都是0630这种的，遇到0630不是交易日的就匹配不上 pd.merge_asof只能根据一个key，需要先split,比较麻烦
# 按月份匹配，如果有多于1条的数据，则取日期最新的
fundPosition['Month_Label'] = [x.year * 100 + x.month for x in fundPosition['Date']]
fundUnivHY['Month_Label'] = [x.year * 100 + x.month for x in fundUnivHY['Date']]
fundPosition.drop_duplicates(['Fund_Code', 'Month_Label'], keep='last', inplace=True)
fundPosUniv = fundUnivHY.merge(fundPosition, how='left', on=['Month_Label', 'Fund_Code'], validate='one_to_one')
#  去掉仓位低于40%的，这种一般是在建仓期
fundPosUniv = fundPosUniv[(fundPosUniv['Stock_Pos'] >= 40) | (np.isnan(fundPosUniv['Stock_Pos']))].copy()
fundPosUniv.sort_values(['Date_x', 'Fund_Code'], inplace=True)
fundPosTotal = fundPosUniv.groupby('Date_x')['Stock_Pos'].mean()
fundPosTotal.index.names = ['Date']  # 这个总数据没有日期问题，只要有数据的一定是全的，不会只是前10名仓位

# 基金持仓的分行业仓位  季报公布的是证监会分类标准，所以这里用底仓自己计算
# 分行业持仓，最新一期的有的基金公布年报，有的只公布的四季报，这种需要处理，处理时间？？？？？？？？？
# 基金net asset要直接读，不能用行业仓位反除，四舍五入不准
# 先筛选出股票型+偏股型基金的底仓，重复的只保留最新
sqlStr = 'select c.S_INFO_WINDCODE, S_INFO_STOCKWINDCODE, c.F_PRT_ENDDATE, F_PRT_STKVALUE, F_PRT_STKVALUETONAV, ' \
         'd.f_prt_netasset from ChinaMutualFundStockPortfolio c ' \
         'left join ChinaMutualFundAssetPortfolio d ' \
         'on c.S_INFO_WINDCODE = d.S_INFO_WINDCODE and c.F_PRT_ENDDATE = d.F_PRT_ENDDATE ' \
         'where c.S_INFO_WINDCODE in (' \
         'select  a.F_INFO_WINDCODE from ChinaMutualFundSector a ' \
         'inner join AShareIndustriesCode b ' \
         'on a.S_INFO_SECTOR=b.INDUSTRIESCODE ' \
         'where a.S_INFO_SECTOR in (\'2001010201000000\', \'2001010101000000\')) and ' \
         'month(c.f_prt_enddate) in (6, 12) ' \
         'and c.F_PRT_ENDDATE >= {0} and c.F_PRT_ENDDATE <= {1}'.format(dateFrom, dateTo)
fundPosDetail = get_wind_data(sqlStr)  # 这样筛出来的是整个过程中在股票型基金中出现过的基金，没有对应时间，需要再对应一次
# 直接用一个sql语句不好写，因为entrydate exitdate一个基金对应多条数据，leftjoin 都给弄上了不好汇总
# 这里面也有同一个报告期出现两次的，数据不一样，以最新日期为准，例如160512.SZ在20110623和20110630的两次只取20110630
fundPosDetail = pd.DataFrame(fundPosDetail, columns=['Fund_Code', 'Stock_Code', 'Date', 'Stk_Values', 'Stk_Pct', 'Fund_Value'])
fundPosDetail['Date'] = pd.to_datetime(fundPosDetail['Date'])
fundPosDetail[['Stk_Values', 'Stk_Pct', 'Fund_Value']] = fundPosDetail[['Stk_Values', 'Stk_Pct', 'Fund_Value']].astype(float)
# 摘出半年度的时间clear_version (不能直接fundPosDetail.drop_duplicates因为本来就有很多duplicates必须摘出时间序列再剔除)
halfYearDate = pd.DataFrame(fundPosDetail['Date'].unique(), columns=['Date'])
halfYearDate['Month_Label'] = [x.year * 100 + x.month for x in halfYearDate['Date']]
halfYearDate.sort_values(['Date'], inplace=True)
halfYearDate.drop_duplicates(['Month_Label'], keep='last', inplace=True)
# tmp = fundPosDetail[[x in set(halfYearDate['Date']) for x in fundPosDetail['Date']]].copy() # 奇怪这个地方如果不加set就都是FALSE
# https://stackoverflow.com/questions/48553824/checking-if-a-date-falls-in-a-dataframe-pandas
# 不要这么筛选，太慢了。。。日期格式尤其慢。。
halfYearDate['Valid'] = True
fundPosDetail = fundPosDetail.merge(halfYearDate[['Date', 'Valid']], on = 'Date', how='left')
fundPosDetail.loc[pd.isnull(fundPosDetail['Valid']), 'Valid'] = False
fundPosDetail = fundPosDetail[fundPosDetail['Valid']].copy()
fundPosDetail.reset_index(drop=True, inplace=True)
fundPosDetail['Month_Label'] = [x.year * 100 + x.month for x in fundPosDetail['Date']]
fundPosDetail = fundUnivHY.merge(fundPosDetail, how='left', on=['Month_Label', 'Fund_Code'], validate='one_to_many') # Date_x是交易日,Date_y是报告日
fundPosDetail = fundPosDetail[['Date_x', 'Fund_Code', 'Stock_Code', 'Stk_Values', 'Fund_Value']]
fundPosDetail.columns = ['Date', 'Fund_Code', 'Stock_Code', 'Stk_Values', 'Fund_Value']
#   到目前为止，还没有剔除过程中才进来或者中途出去的基金，需要再match一步
#  为了严谨做这一步，事实上这部分stockPortfolio表绝大部分也没数据，对结果应该没影响


#  贴行业标签计算行业权重
swComponent = get_component_SW(dateFrom, dateTo)
fundPosDetail = fundPosDetail.merge(swComponent, how='left', on=['Date', 'Stock_Code'])
uniqueNav = fundPosDetail.drop_duplicates(['Date', 'Fund_Code'], keep='first')
totalNav = pd.DataFrame(uniqueNav.groupby('Date')['Fund_Value'].sum())  # 每天的基金总市值
fundPosSector = pd.DataFrame(fundPosDetail.groupby(['Date', 'SW_Code'])['Stk_Values'].sum())   # 每天分行业的总市值
fundPosSector['SW_Code'] = fundPosSector.index.get_level_values('SW_Code')
fundPosSector = fundPosSector.merge(totalNav, how='left', on='Date')
fundPosSector['SW_Pct'] = fundPosSector['Stk_Values'] / fundPosSector['Fund_Value']
# 行业标签添加中文名称
swPara = basic_info_SW()
swPara['SW_Code'] = swPara['Wind_Code'] + '.SI'
fundPosSector = fundPosSector.merge(swPara[['SW_Code', 'SW_Name']], how='left', on='SW_Code').set_index(fundPosSector.index)

# 总仓位和各行业配置比例变动，输出表格结果
currPeriod = fundPosSector.index.unique()[-1]
lastPeriod = fundPosSector.index.unique()[-2]
sectorPctTable = fundPosSector.pivot_table(index='SW_Name', columns=fundPosSector.index, values='SW_Pct', aggfunc='first')
# 本期 、本期相对上期变化，最终只显示当前还在用的行业
sectorPctTable = sectorPctTable.merge(swPara.loc[swPara['Used_Label'] == 1, ['SW_Name', 'Used_Label']], how='left', on='SW_Name')
sectorPctTable.sort_values(['Used_Label', 'SW_Name'], ascending=False, inplace=True) # 把已经不用的行业放在最下面
sectorPctTable.reset_index(drop=True, inplace=True)  # 历史仓位变化图

sectorPctTableCurr = sectorPctTable[['SW_Name', lastPeriod, currPeriod]]
# 把总仓位加在最上面一排一起计算
totalPctCurr = pd.DataFrame(fundPosTotal).transpose()[[lastPeriod, currPeriod]] / 100
totalPctCurr['SW_Name'] = '总仓位'
totalPctCurr = totalPctCurr[['SW_Name', lastPeriod, currPeriod]]
posTable = pd.concat([totalPctCurr, sectorPctTableCurr], axis=0).reset_index(drop=True)
posTable['Delta'] = posTable[currPeriod] - posTable[lastPeriod]


# # 核对计算明细后的仓位和直接读取的仓位差异在哪  直接读取的仓位是各基金的仓位均值，不是sum(A)/sum(B) 而是 mean(A/B)
# tmp = fundPosSector.groupby('Date')['SW_Pct'].sum()
# tmp.values * 100 - fundPosTotal.values
# # 以20191231为例，计算如果sum(A)/sum(B)的总仓位应该是多少,是80吗？基金仓位的平均是88左右,不是80， 是88左右，说明分行业的数不对
# #
#
# tstDate = pd.to_datetime('20191231')
# total20170630 = fundPosUniv[fundPosUniv['Date_x'] == tstDate]  #
# total20170630['Stock_Pos'].mean() # 86.33208927259359
# total20170630['Stk_Value'].sum() / total20170630['Fund_Value'].sum()  # 0.872698 # 这种计算不合适，因为nan/sum会排除，但分开计算就没有排除
# print(total20170630['Stk_Value'].sum()) # 1316306935113
# print(total20170630['Fund_Value'].sum()) # 1508318851764
# stkDetail = fundPosDetail[fundPosDetail['Date'] == tstDate]
# stkDetail['Stk_Values'].sum()  # 千亿级别 差137万，可以忽略
# fundValue = stkDetail.drop_duplicates(['Date', 'Fund_Code'])
# fundValue['Fund_Value'].sum()  # 总数也有差别，为什么会少一些？fundPosDetail的总市值比fundPosUniv(106)的少
# # 因为fundPosDetail有空值，一些新发基金，有总市值，但没有明细底仓数据
# print(stkDetail['Stk_Values'].sum() / fundValue['Fund_Value'].sum() ) # 84.10 本身没问题，看分行业汇总是不是少很多
# # 是的
# stkDetail[pd.isnull(stkDetail['SW_Code'])]['Stk_Values'].sum() / fundValue['Fund_Value'].sum()   # 5.04% 差异在于一些港股，一些股票换行业过渡期缺失行业标签
# stkDetail[~pd.isnull(stkDetail['SW_Code'])]['Stk_Values'].sum() / fundValue['Fund_Value'].sum()  # 79.06%
#
#
#
#############################################新发基金#################################################
dateFrom = '20150101'  # 最终只取过去3年数据
dateTo = datetime.datetime.strftime(datetime.datetime.today(), '%Y%m%d')
dateSeq = get_trading_days(dateFrom, dateTo)
dateSeq = attach_date_label(pd.DataFrame(dateSeq), 'month')
dateSeq = attach_date_label(dateSeq, 'week')
weekSeq = dateSeq[dateSeq['If_Week_End']]
monthSeq = dateSeq[dateSeq['If_Month_End']]  # 这是全部的时间，后面把新发基金的数据merge到这个时间上

sqlStr = 'select  a.F_INFO_WINDCODE, F_INFO_SETUPDATE, c.F_ISSUE_TOTALUNIT * c.F_INFO_PARVALUE * 100000000 as ' \
         'Collection from ChinaMutualFundSector a ' \
         'inner join AShareIndustriesCode b on a.S_INFO_SECTOR=b.INDUSTRIESCODE ' \
         'left join ChinaMutualFundDescription c ' \
         'on a.F_INFO_WINDCODE = c.F_INFO_WINDCODE ' \
         'where a.S_INFO_SECTOR in (\'2001010201000000\', \'2001010101000000\')'

stkFund = get_wind_data(sqlStr)
stkFund = pd.DataFrame(stkFund, columns=['Fund_Code', 'Setup_Date', 'Collection'])
stkFund['Setup_Date'] = pd.to_datetime(stkFund['Setup_Date'])
stkFund['Collection'] = stkFund['Collection'].astype(float)
stkFund.sort_values('Setup_Date', inplace=True)
stkFund['Date'] = stkFund['Setup_Date']
stkFund.set_index('Date', inplace=True)
stkFundWeek = attach_date_label(stkFund, 'week')  # 07年之前的有些基金setup date是非交易日，不影响最近3年可不处理
stkFundMonth = attach_date_label(stkFund, 'month')
weekCount = stkFundWeek.groupby('Week_Label')['Collection'].sum()
monthCount = stkFundMonth.groupby('Month_Label')['Collection'].sum()

weekCount = weekSeq.merge(pd.DataFrame(weekCount), how='left', on='Week_Label').set_index(weekSeq.index)
# 这里merge完了Week_Label不知怎么就变成object了，需要调整一下，不然存hdf会报警告
weekCount['Week_Label'] = weekCount['Week_Label'].astype(int)
weekCount = weekCount[['Week_Label', 'Collection']]
monthCount = monthSeq.merge(pd.DataFrame(monthCount), how='left', on='Month_Label').set_index(monthSeq.index)
monthCount['Month_Label'] = monthCount['Month_Label'].astype(int)
monthCount = monthCount[['Month_Label', 'Collection']]
weekCount['Collection'] = weekCount['Collection'].fillna(0)
monthCount['Collection'] = monthCount['Collection'].fillna(0)

# 筛选出过去3年的部分
dateFrom3Year = datetime.datetime.strftime(pd.to_datetime(dateTo) - datetime.timedelta(365*3), '%Y%m%d')
weekCount = weekCount[weekCount.index >= pd.to_datetime(dateFrom3Year)]
monthCount = monthCount[monthCount.index >= pd.to_datetime(dateFrom3Year)]

# 输出4个数值：过去一周和一月新发规模数据（两个数值）和对应在过去三年上的历史分位数（两个数值）
res1 = weekCount['Collection'][-1]
res2 = monthCount['Collection'][-1]
res3 = weekCount['Collection'].rank()[-1] / len(weekCount)
res4 = monthCount['Collection'].rank()[-1] / len(monthCount)
fundRes = pd.DataFrame({'上周新发规模': '{:.1f}亿'.format(res1/100000000),
                        '上月新发规模': '{:.1f}亿'.format(res2/100000000),
                        '上周新发规模分位数': '{:.1%}'.format(res3),
                        '上月新发规模分位数': '{:.1%}'.format(res4)},
                        index=['value'])
weekCount.to_hdf('dataForPlot/newFundData.hdf', key='weekCount', type='w')
monthCount.to_hdf('dataForPlot/newFundData.hdf', key='monthCount', type='w')
fundRes.to_hdf('dataForPlot/newFundData.hdf', key='fundRes', type='w')

###############################################A/H溢价#################################################
dateFrom = '20060101'   #从有数据开始
dateTo = datetime.datetime.strftime(datetime.datetime.today(), '%Y%m%d')
sqlStr = 'select  S_INFO_WINDCODE, TRADE_DT, S_DQ_CLOSE from HKIndexEODPrices ' \
         'where S_INFO_WINDCODE = \'HSAHP.HI\' ' \
         'and TRADE_DT >= {0} and TRADE_DT <= {1} ' \
         'order by TRADE_DT'.format(dateFrom, dateTo)
dataAH = pd.DataFrame(get_wind_data(sqlStr), columns=['Index_Code', 'Date', 'Close'])
dataAH['Date'] = pd.to_datetime(dataAH['Date'])
dataAH['Close'] = dataAH['Close'].astype(float)
dataAH.set_index('Date', inplace=True)
dataAH['Close_Pct'] = dataAH['Close'].expanding(min_periods=1).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=True)
dataAH.to_hdf('dataForPlot/dataAH.hdf', key='dataAH', type='w')



###############################################IPO规模####################################################

dateFrom = '20150101'  # 最终只取过去3年数据
dateTo = datetime.datetime.strftime(datetime.datetime.today(), '%Y%m%d')
dateSeq = get_trading_days(dateFrom, dateTo)
dateSeq = attach_date_label(pd.DataFrame(dateSeq), 'month')
dateSeq.iloc[-1, dateSeq.columns.get_loc('If_Month_End')] = True  #  当月显示月初至今，需要对日期的最后一个If_Month_End调整成True

monthSeq = dateSeq[dateSeq['If_Month_End']].copy()  # 这是全部的时间，后面把新发基金的数据merge到这个时间上

sqlStr = 'select S_INFO_WINDCODE, S_IPO_COLLECTION * 10000 as Collection, S_IPO_LISTDATE from AShareIPO ' \
         'where S_IPO_LISTDATE >= {0} ' \
         'order by S_IPO_LISTDATE'.format(dateFrom)
stkUniv = get_wind_data(sqlStr)
stkUniv = pd.DataFrame(stkUniv, columns=['Stock_Code', 'Collection', 'List_Date'])
stkUniv['List_Date'] = pd.to_datetime(stkUniv['List_Date'])
stkUniv['Collection'] = stkUniv['Collection'].astype(float)
stkUniv['Date'] = stkUniv['List_Date']
stkUniv.set_index('Date', inplace=True)
stkMonth = attach_date_label(stkUniv, 'month')
monthCountIPO = stkMonth.groupby('Month_Label')['Collection'].sum()

monthCountIPO = monthSeq.merge(pd.DataFrame(monthCountIPO), how='left', on='Month_Label').set_index(monthSeq.index)
monthCountIPO = monthCountIPO[['Month_Label', 'Collection']]
monthCountIPO['Collection'] = monthCountIPO['Collection'].fillna(0)

# 筛选出过去3年的部分
dateFrom3Year = datetime.datetime.strftime(pd.to_datetime(dateTo) - datetime.timedelta(365*3), '%Y%m%d')
monthCountIPO = monthCountIPO[monthCountIPO.index >= pd.to_datetime(dateFrom3Year)]
monthCountIPO.to_hdf('dataForPlot/monthCountIPO.hdf', key='monthCountIPO', type='w')













