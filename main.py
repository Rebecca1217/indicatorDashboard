# -*- coding:UTF-8 -*-

# @created @2020.03.30

import pandas as pd
from public.getWindData import get_wind_data
from public.getIndexPos import get_index_pos
from public.getTradingDays import get_trading_days, get_trading_days2
from public.getStockFundPos import get_stock_fund_pos
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
dateFrom = '20100101'
dateTo = datetime.datetime.strftime(datetime.datetime.today(), '%Y%m%d')
dateSeq = get_trading_days(dateFrom, dateTo)
dateSeq['Year_Month'] = dateSeq.index.year * 100 + dateSeq.index.month
fundUniv = get_stock_fund_pos(dateFrom, dateTo)   # 普通股票型+混合偏股型基金全集
# 构造年中和年末日期序列
dateSeq = get_trading_days2(dateFrom, dateTo, 'halfyear')
dateSeq = dateSeq[dateSeq['If_Halfyear_End']]
# 筛选年中和年末的基金universe  fundUnivHalfYear
fundUnivHY = fundUniv[[x in dateSeq.index for x in fundUniv['Date']]]
# 获取基金的仓位数据，join到univ上求均值即可
sqlStr = 'select S_INFO_WINDCODE, F_PRT_ENDDATE, F_PRT_STOCKTONAV from ' \
         'ChinaMutualFundAssetPortfolio where F_PRT_ENDDATE >= {0} and F_PRT_ENDDATE <= {1}'.format(dateFrom, dateTo)
fundPosition = get_wind_data(sqlStr)
fundPosition = pd.DataFrame(fundPosition, columns=['Fund_Code', 'Date', 'Stock_Pos'])
fundPosition['Date'] = pd.to_datetime(fundPosition['Date'])
fundPosition['Stock_Pos'] = fundPosition['Stock_Pos'].astype(float)
# Note: fundPosition的日期是报告日期，都是0630这种的，遇到0630不是交易日的就匹配不上 pd.merge_asof只能根据一个key，需要先split,比较麻烦
tmp = fundUnivHY.merge(fundPosition, how='left', on=['Date', 'Fund_Code'])








