# -*- coding:UTF-8 -*-
import matplotlib.pyplot as plt
# import matplotlib
# import matplotlib.ticker as mticker
# import matplotlib.dates as mdates
import pandas as pd
import numpy as np
import seaborn as sns


# @created @2020.03.30
marginTDPlot = pd.read_hdf('dataForPlot/marginTD.hdf', key='marginTD')
marginTDPlot['Margin_TD_Balance'] = marginTDPlot['Margin_TD_Balance'] / 100000000
marginTDSH = marginTDPlot.loc[marginTDPlot['Mkt'] == 'SSE', ['Date','Margin_TD_Balance']].copy()
marginTDSZ = marginTDPlot.loc[marginTDPlot['Mkt'] == 'SZSE', ['Date', 'Margin_TD_Balance']].copy()
marginTDSH.set_index('Date', inplace=True)
marginTDSZ.set_index('Date', inplace=True)
marginTDTotal = pd.DataFrame(marginTDPlot.groupby('Date')['Margin_TD_Balance'].sum())
assert len(marginTDSH) == len(marginTDSZ) == len(marginTDTotal)


#  箱线图
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
marAmountRatio = pd.read_hdf('dataForPlot/marAmountRatio.hdf')
marAmountTRatio = pd.read_hdf('dataForPlot/marAmountTRatio.hdf')
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


# 饼图
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

# 指数成交量柱状图
# fig, ax = plt.subplots()
tdAmountSH = pd.read_hdf('dataForPlot/tdAmountSH.hdf')
tdAmountSH.index = [pd.to_datetime(str(x)).strftime('%Y/%m/%d') for x in tdAmountSH.index.get_level_values('Date')]
tdAmountSH = tdAmountSH.droplevel('Index_Code')
tdAmountSH.plot.bar(color='#e74c3c')
plt.title('上证综指近20交易日成交量（亿）')
plt.show()

tdAmount300 = pd.read_hdf('dataForPlot/tdAmoun300.hdf')
tdAmount300.index = [pd.to_datetime(str(x)).strftime('%Y/%m/%d') for x in tdAmount300.index.get_level_values('Date')]
tdAmount300 = tdAmount300.droplevel('Index_Code')
tdAmount300.plot.bar(color='#e74c3c')
plt.title('沪深300近20交易日成交量（亿）')
plt.show()

tdAmount500 = pd.read_hdf('dataForPlot/tdAmount500.hdf')
tdAmount500.index = [pd.to_datetime(str(x)).strftime('%Y/%m/%d') for x in tdAmount500.index.get_level_values('Date')]
tdAmount500 = tdAmount500.droplevel('Index_Code')
tdAmount500.plot.bar(color='#e74c3c')
plt.title('中证500近20交易日成交量（亿）')
plt.show()

tdAmountGEM = pd.read_hdf('dataForPlot/tdAmountGEM.hdf')
tdAmountGEM.index = [pd.to_datetime(str(x)).strftime('%Y/%m/%d') for x in tdAmountGEM.index.get_level_values('Date')]
tdAmountGEM = tdAmountGEM.droplevel('Index_Code')
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

