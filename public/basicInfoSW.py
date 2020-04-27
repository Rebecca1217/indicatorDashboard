# -*- coding:UTF-8 -*-

#  获取申万一级行业名称、代码对照表
import pandas as pd
from public.getDataSQL import get_data_sql
import numpy as np
def basic_info_SW():

    # 61开头是申万，levelnum=2表示一级

#     exeStr = 'select INDUSTRIESCODE, INDUSTRIESNAME, INDUSTRIESALIAS from dbo.ASHAREINDUSTRIESCODE where  LEVELNUM = 2 and INDUSTRIESCODE like \'61%\' and USED = 1'
    exeStr = 'select INDUSTRIESCODE, INDUSTRIESNAME, INDUSTRIESALIAS, USED from dbo.ASHAREINDUSTRIESCODE where LEVELNUM = 2 and INDUSTRIESCODE like \'61%\''
    codePara = get_data_sql(exeStr, 'wind')
    codePara.columns=['SW_Code', 'SW_Name', 'Wind_Code', 'Used_Label']
    # codePara['Wind_Code_New'] = pd.Series(codePara['Wind_Code'] + '.SI')
    codePara.sort_values(['Used_Label', 'Wind_Code'], inplace=True)
    codePara.reset_index(drop=True, inplace=True)
    sectorName = [np.nan] * 6 + ['消费', '周期上游', '周期中游', '周期中游', '周期上游', '成长', '消费',
                                 '消费', '消费', '消费', '消费', '周期中游', '周期中游', '大金融',
                                 '消费', '消费', '其他', '周期下游', '周期下游', '成长', '其他', '成长',
                                 '成长', '成长', '大金融', '大金融', '周期下游', '周期下游']
    codePara['Sector_Name'] = sectorName

    return(codePara)
