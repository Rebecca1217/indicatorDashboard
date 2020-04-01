# -*- coding:UTF-8 -*-

import numpy as np
import pandas as pd

def get_IC(factorValues, indexPriceValues, winIC, ifRank):
    # 修改cumReturn,从一天一天cumprod改成直接priceValues 对应日期相除
    assert factorValues.shape == indexPriceValues.shape, 'Check the dimension of inputs!'

    factorValues = factorValues.astype(np.float)
    indexPriceValues = indexPriceValues.astype(np.float)
    returnValues = pd.DataFrame(np.nan, index=indexPriceValues.index, columns=indexPriceValues.columns)
    returnValues.iloc[:, winIC:returnValues.shape[1]] = (indexPriceValues.iloc[:, winIC:(indexPriceValues.shape[1])].values / \
                                                        indexPriceValues.iloc[:, 0:(indexPriceValues.shape[1] - winIC)].values) - 1

    if ifRank:
        factorValues = factorValues.rank(axis=0, method='dense')
        returnValues = returnValues.rank(axis=0, method='dense')

    ICSeries = pd.Series([np.nan] * factorValues.shape[1], index=factorValues.columns)
    if factorValues.shape[1] >= winIC:
        for i in range(winIC, factorValues.shape[1]):  # i指代收益率的index，i之前的factorValues对应的IC是nan
            factorI = factorValues.iloc[:, i - winIC]
            returnI = returnValues.iloc[:, i]  # 用到i-winIC的Close_Price，没有用到i-winIC的收益率
            # 筛选出都是非nan的再计算相关系数
            labelValid = (~np.isnan(factorI)) & (~np.isnan(returnI))
            iIC = np.corrcoef(factorI[labelValid], returnI[labelValid])[0, 1]
            ICSeries[i] = iIC

    return ICSeries


# def get_IC_Olc(factorValues, returnValues, winIC, ifRank):
#     # 需要考虑nan对相关系数的影响
#     # @2020.03.13修正IC收益率的错位从dr变为cumR
#     # priceValues自己构造returnValues
#     assert factorValues.shape == returnValues.shape, 'Check the dimension of inputs!'
#
#     factorValues = factorValues.astype(np.float)
#     returnValues = returnValues.astype(np.float)
#     # 这个地方出现了一个错误，不能上来就对returnValues求秩，因为这时候returnValues还不是cumReturn
#     if ifRank:
#         factorValues = factorValues.rank(axis=0, method='dense')
#         # returnValues = returnValues.rank(axis=0, method='dense')
#
#     ICSeries = pd.Series([np.nan] * factorValues.shape[1], index=factorValues.columns)
#     if factorValues.shape[1] >= winIC:
#         for i in range(winIC, factorValues.shape[1]):
#             factorI = factorValues.iloc[:, i - winIC]
#             # returnI = returnValues.iloc[:, i]
#             # returnI需要的一段时间的cumReturn
#             returnIDR = returnValues.iloc[:, (i - winIC + 1) : (i + 1)]
#             returnI = np.cumprod((returnIDR + 1), axis=1) - 1
#             returnI = returnI.iloc[:, -1]
#             if ifRank:
#                 returnI = returnI.rank(method='dense')
#             # 筛选出都是非nan的再计算相关系数
#
#             labelValid = (~np.isnan(factorI)) & (~np.isnan(returnI))
#             iIC = np.corrcoef(factorI[labelValid], returnI[labelValid])[0, 1]
#             ICSeries[i] = iIC
#
#     return ICSeries
