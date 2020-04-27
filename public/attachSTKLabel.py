# -*- coding:UTF-8 -*-
import pandas as pd
import numpy as np
import re
from public.getDataSQL import get_data_sql
import datetime
from sqlalchemy import create_engine



# 根据基金的bchmk文字描述，按照50%股票类指数作为分界点，贴偏股类标签（1）和非偏股类标签（0）


def attach_stk_label(inputTable):
    assert all([x in inputTable.columns for x in ['Fund_Code', 'Bchmk']])
    bchmkList = [x if pd.isnull(x) else x.split('+')
                 for x in inputTable['Bchmk']]

    bchmkDict = []
    for iRow in range(len(bchmkList)):
        bchmkStringI = bchmkList[iRow]
        obj = {}
        if bchmkStringI is not None:
            for item in bchmkStringI:
                if len(re.findall(r'^([^*%]*)\*?(?:([1-9]\d?)%)?$', item)) > 0:
                    x, y = re.findall(
                        r'^([^*%]*)\*?(?:([1-9]\d?)%)?$', item)[0]
                    obj[x] = y
        bchmkDict.append(obj)

    # 读取指数标签参数表：
    sqlStr = 'select Bchmk_Name, If_STK from paraBchmkType'
    indexLabel = get_data_sql(sqlStr, 'lhtz')
    indexLabel.columns=['Bchmk_Name', 'If_STK']
    indexLabel = indexLabel.set_index('Bchmk_Name')['If_STK'].to_dict()

    assert len(inputTable) == len(bchmkDict)
    stkWeight = []
    newIndexName = []
    sqlTable = pd.DataFrame()
    for iCode in range(len(bchmkDict)):
        bchmkI = bchmkDict[iCode]
        stkLabelI = [indexLabel.get(key) for key in bchmkI.keys()]
        weightI = [0.0 if w == '' else float(w) for w in bchmkI.values()]
        if None not in stkLabelI:
            stkWeightI = sum(np.array(stkLabelI) * np.array(weightI))
        else:
            # 把None先粗略补齐，再把新指数名称保存下来，便于print到日志检查
            newIndexName = np.array(list(bchmkI.keys()))[
                [x is None for x in stkLabelI]]
            addLabel = []
            for i in range(len(newIndexName)):
                newIndexNameI = newIndexName[i]
                addLabelI = not(
                    '债' in newIndexNameI or '存款' in newIndexNameI or '年化' in newIndexNameI)
                addLabel.append(addLabelI)
                # write to sql
                sqlTableI = pd.DataFrame(np.array([newIndexName, addLabel,
                                                  [datetime.datetime.strftime(datetime.datetime.today(), '%Y-%m-%d')] * len(addLabel)]
                                                 ).transpose(), columns=['Bchmk_Name', 'If_STK', 'Update_Time'])
                sqlTable = pd.concat([sqlTable, sqlTableI], axis=0, ignore_index=True)


            addIndex = np.where([x is None for x in stkLabelI])[0]
            newIndex = list(range(len(addLabel)))
            for i, j in zip(addIndex, newIndex):
                stkLabelI[i] = addLabel[j]
            stkWeightI = sum(np.array(stkLabelI) * np.array(weightI))

        stkWeight.append(stkWeightI)
    sqlTable.drop_duplicates(inplace=True)
    if len(sqlTable) > 0:
        engine = create_engine("mssql+pymssql://lhtz:Cjhx+20140801@10.201.4.164:1433/lhtzdb")
        conn = engine.connect()
        sqlTable.to_sql('paraBchmkType', conn, index=False, if_exists='append')
        conn.close()
        print('新添加Bchmk_Name:{0} ...'.format(sqlTable))
    else:
        print('没有需要新添加参数的Bchmk_Name...')

    res = inputTable.copy()
    res.loc[:, 'Stk_Weight'] = stkWeight
    res = res[res['Stk_Weight'] >= 50]

    return res, newIndexName
