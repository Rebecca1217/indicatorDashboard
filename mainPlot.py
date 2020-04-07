# -*- coding:UTF-8 -*-
import matplotlib.pyplot as plt
# import matplotlib
# import matplotlib.ticker as mticker
# import matplotlib.dates as mdates
import pandas as pd
import numpy as np
import seaborn as sns
import datetime


# @created @2020.03.30
marginTDPlot = pd.read_hdf('dataForPlot/marginTDData.hdf', key='marginTD')
marginTDPlot['Margin_TD_Balance'] = marginTDPlot['Margin_TD_Balance'] / 100000000
marginTDSH = marginTDPlot.loc[marginTDPlot['Mkt'] == 'SSE', ['Date','Margin_TD_Balance']].copy()
marginTDSZ = marginTDPlot.loc[marginTDPlot['Mkt'] == 'SZSE', ['Date', 'Margin_TD_Balance']].copy()
marginTDSH.set_index('Date', inplace=True)
marginTDSZ.set_index('Date', inplace=True)
marginTDTotal = pd.DataFrame(marginTDPlot.groupby('Date')['Margin_TD_Balance'].sum())
assert len(marginTDSH) == len(marginTDSZ) == len(marginTDTotal)


###################################################  箱线图  ######################################################
marginData = {'沪市': marginTDSH['Margin_TD_Balance'], '深市': marginTDSZ['Margin_TD_Balance'], '全A': marginTDTotal['Margin_TD_Balance']}
fig, ax = plt.subplots()
# ax.boxplot(marginData.values(), showfliers=False, widths=0.001)
ax.boxplot(marginData.values(), whis=3, showfliers=False)  # 定义outliers |Q3-Q1| * whis
ax.set_xticklabels(marginData.keys())
mSH = marginTDSH['Margin_TD_Balance'].median()
mSZ = marginTDSZ['Margin_TD_Balance'].median()
mTotal = marginTDTotal['Margin_TD_Balance'].median()
latestSH = marginTDSH['Margin_TD_Balance'][-1]
latestSZ = marginTDSZ['Margin_TD_Balance'][-1]
latestTotal = marginTDTotal['Margin_TD_Balance'][-1]
ax.scatter(1, latestSH, c='grey', label='SH')
ax.text(1 - 0.3, mSH, int(round(mSH, 0)))
quantileSH = '({0}%)'.format(round(marginTDSH['Margin_TD_Balance'].rank()[-1] / len(marginTDSH) * 100, 1))
ax.text(1.15, latestSH, str(int(round(latestSH, 0))) + '\n' + quantileSH, ha='center', va='center')
ax.scatter(2, latestSZ, c='grey', label='SH')
ax.text(2 - 0.3, mSZ, int(round(mSZ, 0)))
quantileSZ = '({0}%)'.format(round(marginTDSZ['Margin_TD_Balance'].rank()[-1] / len(marginTDSZ) * 100, 1))
ax.text(2.15, latestSZ, str(int(round(latestSZ, 0))) + '\n' +  quantileSZ, ha='center', va='center')
ax.scatter(3, latestTotal, c='grey', label='SH')
ax.text(3 - 0.3, mTotal, int(round(mTotal, 0)))
quantileTotal = '({0}%)'.format(round(marginTDTotal['Margin_TD_Balance'].rank()[-1] / len(marginTDTotal) * 100, 1))
ax.text(3.15, latestTotal, str(int(round(latestTotal, 0))) + '\n' + quantileTotal, ha='center', va='center')
plt.title('A股两融余额历史分位(亿元)')
plt.show()


# 箱线图2
marAmountRatio = pd.read_hdf('dataForPlot/marginTDData.hdf', key='marAmountRatio')
marAmountTRatio = pd.read_hdf('dataForPlot/marginTDData.hdf', key='marAmountTRatio')
marAmountRatio = marAmountRatio[~np.isnan(marAmountRatio['Ratio'])]
marAmountTRatio = marAmountTRatio[~np.isnan(marAmountTRatio['Ratio'])]
marRatioSH = marAmountRatio[marAmountRatio.index.get_level_values('Mkt') == 'SH'].copy()
marRatioSZ = marAmountRatio[marAmountRatio.index.get_level_values('Mkt') == 'SZ'].copy()
marRatioSH = marRatioSH.droplevel(level='Mkt')
marRatioSZ = marRatioSZ.droplevel(level='Mkt')
marRatioData = {'沪市': marRatioSH['Ratio'], '深市': marRatioSZ['Ratio'], '全A': marAmountTRatio['Ratio']}
fig, ax = plt.subplots()
ax.boxplot(marRatioData.values(), whis=3, showfliers=False, widths=0.3, medianprops=dict(linestyle='-', color='orange'))
ax.set_xticklabels(marRatioData.keys())
# ax.yaxis.set_major_formatter(mticker.PercentFormatter())  # 这种format会直接把数字加上% 而不是*100%
ax.set_yticklabels(['{:,.1%}'.format(x) for x in ax.get_yticks()])
mSH = marRatioSH['Ratio'].median()
mSZ = marRatioSZ['Ratio'].median()
mA = marAmountTRatio['Ratio'].median()
latestSH = marRatioSH['Ratio'][-1]
latestSZ = marRatioSZ['Ratio'][-1]
latestA = marAmountTRatio['Ratio'][-1]
# 沪市
ax.scatter(1, latestSH, c='grey', label='SH')
ax.text(1-0.32, mSH, '{:.1%}'.format(mSH))
quantileSH = '({0}%)'.format(round(marRatioSH['Ratio'].rank()[-1] / len(marRatioSH) * 100, 1))
ax.text(1.15, latestSH, str('{:.1%}'.format(latestSH)) + '\n' + quantileSH, ha='center', va='center')
# 深市
ax.scatter(2, latestSZ, c='grey', label='SZ')
ax.text(2-0.32, mSZ, '{:.1%}'.format(mSZ))
quantileSZ = '({0}%)'.format(round(marRatioSZ['Ratio'].rank()[-1] / len(marRatioSZ) * 100, 1))
ax.text(2.15, latestSZ, str('{:.1%}'.format(latestSZ)) + '\n' + quantileSZ, va='center', ha='center')
# 全A
ax.scatter(3, latestA, c='grey', label='全A')
ax.text(3-0.32, mA, '{:.1%}'.format(mA))
quantileA = '({0}%)'.format(round(marAmountTRatio['Ratio'].rank()[-1] / len(marAmountTRatio) * 100, 1))
ax.text(3.15, latestA, str('{:.1%}'.format(latestA)) + '\n' + quantileA, ha='center', va='center')
plt.title('融资交易对市场交易量占比')
plt.show()


############################################### 成交量饼图 ############################################
pieData = pd.DataFrame({'Margin_TD_Balance': [marginTDSH['Margin_TD_Balance'][-1],
                                              marginTDSZ['Margin_TD_Balance'][-1]]},
                       index=['沪市', '深市'])

def pie_plot(plotData, plotTitle):
    sns.set(font='SimHei', style='white')
    flatui = ['#e74c3c', '#8b8b8b', '#6e97c8', '#edbe8b', '#9b93c8', '#2e8b78', '#696969', '#f08080']  # 红灰蓝橙紫绿
    sns.set_palette(flatui)
    sns.palplot(sns.color_palette())
    l1 = [int(round(x,0)) for x in plotData['Margin_TD_Balance'].values]
    l2 = ['({:.1f}'.format(x*100) + '%)' for x in np.array(l1) / sum(l1)]
    showLabels = [str(l1[0]) + l2[0], str(l1[1]) + l2[1]]
    plotData.plot.pie(y='Margin_TD_Balance', title=plotTitle,
                      legend=True, labels=showLabels, labeldistance=0.4, fontsize=12)
    plt.legend(labels=plotData.index, loc='upper right')
    # sns.despine()  # 去掉边框
    plt.axis('equal')
    plt.ylabel('')
    plt.show()
    return
pie_plot(pieData, '两市融资融券余额占比')

################################################# 指数成交量柱状图  ##############################################
# fig, ax = plt.subplots()
tdAmountSH = pd.read_hdf('dataForPlot/indexTDAmount.hdf', key='tdAmountSH')
tdAmountSH.index = [pd.to_datetime(str(x)).strftime('%Y/%m/%d') for x in tdAmountSH.index.get_level_values('Date')]
tdAmountSH.plot.bar(color='#e74c3c')
plt.title('上证综指近20交易日成交量（亿）')
plt.show()

tdAmount300 = pd.read_hdf('dataForPlot/indexTDAmount.hdf', key='tdAmount300')
tdAmount300.index = [pd.to_datetime(str(x)).strftime('%Y/%m/%d') for x in tdAmount300.index.get_level_values('Date')]
tdAmount300.plot.bar(color='#e74c3c')
plt.title('沪深300近20交易日成交量（亿）')
plt.show()

tdAmount500 = pd.read_hdf('dataForPlot/indexTDAmount.hdf', key='tdAmount500')
tdAmount500.index = [pd.to_datetime(str(x)).strftime('%Y/%m/%d') for x in tdAmount500.index.get_level_values('Date')]
tdAmount500.plot.bar(color='#e74c3c')
plt.title('中证500近20交易日成交量（亿）')
plt.show()

tdAmountGEM = pd.read_hdf('dataForPlot/indexTDAmount.hdf', key='tdAmountGEM')
tdAmountGEM.index = [pd.to_datetime(str(x)).strftime('%Y/%m/%d') for x in tdAmountGEM.index.get_level_values('Date')]
tdAmountGEM.plot.bar(color='#e74c3c')
plt.title('创业板指近20交易日成交量（亿）')
plt.show()


# # import matplotlib.dates as mdates
# # 横轴输入时间格式的话会默认把非交易日补齐，不需要，所以干脆不输入时间
# fig, ax = plt.subplots()
# ax.xaxis.set_major_formatter(mdates.DateFormatter('%m%d'))
# ax.bar(tdAmountGEMPlot.index, tdAmountGEMPlot['Amount'], color='#e74c3c', align='center')
# plt.title('创业板指近20交易日成交量（亿）')
# plt.xlabel(' ')
# plt.show()


############################################   基金仓位表格  ########################################
posTable = pd.read_hdf('dataForPlot/fundPosData.hdf', key='posTable')
sectorPctTable = pd.read_hdf('dataForPlot/fundPosData.hdf', key='sectorPctTable')
posTable.iloc[:, 1:4] = posTable.iloc[:, 1:4].apply(lambda x: ['{:.1%}'.format(e) for e in x])
fig = plt.figure(dpi=120)
ax = fig.add_subplot(1,1,1)
table_data=posTable.values
table = ax.table(cellText=table_data,
                 colLabels=['行业分类', '上期仓位', '本期仓位', '本期相对上期变化'],
                 loc='center')
table.set_fontsize(10)
table.scale(1,0.8)
ax.axis('off')
plt.show()

# 分行业画历史仓位热力图
heatTable = sectorPctTable.fillna(0)
heatTable.set_index('SW_Name', inplace=True)
heatTable.columns = [datetime.datetime.strftime(x, '%Y/%m')for x in heatTable.columns]
fig, ax = plt.subplots()
sns.heatmap(heatTable * 100, annot=False, cmap='OrRd', cbar_kws={'format': '%.0f%%'},
            xticklabels=True, yticklabels=True)
ax.set_yticklabels(ax.get_yticklabels(), size = 8)
plt.ylabel('')
plt.title('权益类基金行业仓位历史分布图')
plt.show()

################################################ 新发基金 ########################################
weekCount = pd.read_hdf('dataForPlot/newFundData.hdf', key='weekCount')
monthCount = pd.read_hdf('dataForPlot/newFundData.hdf', key='monthCount')
fundRes = pd.read_hdf('dataForPlot/newFundData.hdf', key='fundRes')
#  输出四个值的表格
fig = plt.figure(dpi=80)
ax = fig.add_subplot(1,1,1)
table_data=fundRes.values
table = ax.table(cellText=table_data, colLabels=fundRes.columns, loc='center')
table.set_fontsize(14)
table.scale(1,4)
ax.axis('off')
plt.show()
# 柱状图
del weekCount['Week_Label'], monthCount['Month_Label']
weekCount.index = [pd.to_datetime(str(x)).strftime('%Y/%m/%d') for x in weekCount.index.get_level_values('Date')]
monthCount.index = [pd.to_datetime(str(x)).strftime('%Y/%m') for x in monthCount.index.get_level_values('Date')]
weekCount['Collection'] = weekCount['Collection'] / 1e8
monthCount['Collection'] = monthCount['Collection'] / 1e8
weekColor = ['#8b8b8b'] * (len(weekCount) - 1) + ['#e74c3c']
monthColor = ['#8b8b8b'] * (len(monthCount) - 1) + ['#e74c3c']

# 新发基金周度柱状图
fig, ax = plt.subplots()
plt.bar(list(range(len(weekCount))), weekCount['Collection'].values,
        tick_label=weekCount.index, color=weekColor, width=0.5)
ax.set_xticklabels(weekCount.index, rotation = 90, ha="right")
marker = weekCount.iloc[-1, weekCount.columns.get_loc('Collection')]
ax.text(len(weekCount) - 1.5, marker + 5, '{:.1f}'.format(marker))
tickNum = 10
xTicks = [int(round(x, 0)) for x in np.linspace(0,len(weekCount.index) - 1, tickNum, endpoint=True)]
xTicksLabel = weekCount.index[xTicks]
ax.set_xticks(xTicks)
# xTicksLabel = [pd.to_datetime(str(x)).strftime('%Y/%m/%d') for x in xTicks]
ax.set_xticklabels(xTicksLabel, rotation=90, ha="right")
plt.title('近3年周度新发基金规模（亿）')
plt.show()

# 新发基金月度柱状图
fig, ax = plt.subplots()
plt.bar(list(range(len(monthCount))), monthCount['Collection'].values,
        tick_label=monthCount.index, color=monthColor, width=0.5)
ax.set_xticklabels(monthCount.index, rotation = 90, ha="right")
marker = monthCount.iloc[-1, monthCount.columns.get_loc('Collection')]
ax.text(len(monthCount) - 1.5, marker + 5, '{:.1f}'.format(marker))
tickNum = 10
xTicks = [int(round(x, 0)) for x in np.linspace(0,len(monthCount.index) - 1, tickNum, endpoint=True)]
xTicksLabel = monthCount.index[xTicks]
ax.set_xticks(xTicks)
# xTicksLabel = [pd.to_datetime(str(x)).strftime('%Y/%m/%d') for x in xTicks]
ax.set_xticklabels(xTicksLabel, rotation=90, ha="right")
plt.title('近3年月度新发基金规模（亿）')
plt.show()


############################################ 月度IPO柱状图 ###################################################
monthCountIPO = pd.read_hdf('dataForPlot/monthCountIPO.hdf', key='monthCountIPO')
monthCountIPO['Collection'] = round(monthCountIPO['Collection'] / 100000000, 0).astype(int) # 亿元
del monthCountIPO['Month_Label']
monthCountIPO.index = [pd.to_datetime(str(x)).strftime('%Y/%m') for x in monthCountIPO.index.get_level_values('Date')]
color = ['#8b8b8b'] * (len(monthCountIPO) - 1) + ['#e74c3c']
# 最后一列柱突出颜色并标记数字
fig, ax = plt.subplots()
plt.bar(list(range(len(monthCountIPO))), monthCountIPO['Collection'].values,
        tick_label=monthCountIPO.index, color=color, width=0.5)
ax.set_xticklabels(monthCountIPO.index, rotation = 90, ha="right")
marker = monthCountIPO['Collection'][-1]
# ax.scatter(len(monthCountIPO), marker, c='black')
ax.text(len(monthCountIPO) - 1.5, marker + 5, str(marker))
# fig.tight_layout()
plt.title('近3年月度IPO规模（亿）')
plt.show()

###################################################  A/H溢价####################################################
dataAH = pd.read_hdf('dataForPlot/dataAH.hdf', key='dataAH')
closeMean = dataAH['Close'].mean()
closeSigma = dataAH['Close'].std()
closeS1Plus = closeMean + closeSigma
closeS1Minus = closeMean - closeSigma
closeS2Plus = closeMean + 2 * closeSigma
closeS2Minus = closeMean - 2 * closeSigma

# 画两张折线图

# sns.set(font='SimHei', style='ticks')  # deep
# flatui = ['#e74c3c', '#8b8b8b', '#6e97c8', '#edbe8b', '#9b93c8', '#2e8b78', '#696969', '#f08080']  # 红灰蓝橙紫绿
# sns.set_palette(flatui)
# sns.palplot(sns.color_palette())
def plot_line(plotData, sig1Plus, sig1Minus, sig2Plus, sig2Minus, title):
    fig, ax = plt.subplots()
    plotData.plot(color='#e74c3c', title=title, legend=True)
    plt.hlines(sig1Plus, plotData.index.min(), plotData.index.max(), colors='grey', linestyles='dashed')
    plt.hlines(sig2Plus, plotData.index.min(), plotData.index.max(), colors='grey', linestyles='dashdot')
    plt.hlines(sig1Minus, plotData.index.min(), plotData.index.max(), colors='grey', linestyles='dashed')
    plt.hlines(sig2Minus, plotData.index.min(), plotData.index.max(), colors='grey', linestyles='dashdot')
    plt.legend(labels=['指数价格', '一倍标准差', '两倍标准差'], loc='upper right')
    plt.xlabel('')
    # plt.tight_layout()
    # plt.autoscale()
    #  这个地方设置横坐标轴需要调整
    tickNum = 10
    xTicks = [int(round(x, 0)) for x in np.linspace(0,len(plotData.index) - 1, tickNum, endpoint=True)]
    xTicks = plotData.index[xTicks]
    ax.set_xticks(xTicks)
    xTicksLabel = [pd.to_datetime(str(x)).strftime('%Y/%m/%d') for x in xTicks]
    ax.set_xticklabels(xTicksLabel, rotation=90, ha="right")
    plt.show()
    return
plot_line(dataAH['Close'], closeS1Plus, closeS1Minus, closeS2Plus, closeS2Minus, 'A/H溢价指数历史走势')

fig, ax = plt.subplots()
ax = dataAH['Close_Pct'].plot(color='#e74c3c', title='A/H溢价指数历史分位数走势', legend=False)
plt.xlabel('')
tickNum = 10
xTicks = [int(round(x, 0)) for x in np.linspace(0,len(dataAH['Close_Pct'].index) - 1, tickNum, endpoint=True)]
xTicks = dataAH['Close_Pct'].index[xTicks]
ax.set_xticks(xTicks)
xTicksLabel = [pd.to_datetime(str(x)).strftime('%Y/%m/%d') for x in xTicks]
ax.set_xticklabels(xTicksLabel, rotation=90, ha="right")
ax.set_yticklabels(['{:,.0%}'.format(x) for x in ax.get_yticks()])
marker = dataAH['Close_Pct'][-1]
plt.annotate('{:,.1%}'.format(marker), xy=(dataAH.index[-1], marker),
             xytext=(dataAH.index[-500], marker + 0.3),  # 横坐标是日期，不是数字
            arrowprops=dict(facecolor='black', shrink=1, width=0.3, headlength=11,headwidth=3)
            )
plt.show()