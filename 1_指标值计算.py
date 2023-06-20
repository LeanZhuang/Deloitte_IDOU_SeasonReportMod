# _*_ coding utf-8 _*_
# Author: You Tian
# Date: 24/7/2022 5:19 pm
# File name: 1_指标值计算

############################################################################
######################### 代码说明 #####################################
# 该步骤通常要求计算三年时间跨度的指标，即使后续触发判断只需要计算最新单季的触发，但判断过程仍需要用到三年前的数据，
# 因此该文件输出的指标数据应包含三年时间跨度的数据，才能保证后续触发结果正确。


import pandas as pd
import numpy as np
import os
import re
from datetime import date
import sys

############################################################################
######################### 数据读取 #####################################


# input_df = sys.argv[1]
# output_df = sys.argv[2]
# start_year = int(sys.argv[3])
# start_quarter = int(sys.argv[4])
# end_year = int(sys.argv[5])
# end_quarter = int(sys.argv[6])
input_df = '/Users/zhuangyuhao/PycharmProjects/SeasonRepMod/整合季报代码模型/dfs/原始科目数据_20230104.csv'
output_df = '/Users/zhuangyuhao/PycharmProjects/SeasonRepMod/整合季报代码模型/dfs/指标计算结果_20230116.csv'
start_year = int(2019)
start_quarter = int(1)
end_year = int(2022)
end_quarter = int(3)

path_dfs = r"dfs/"   # 在此更改数据输入文件夹
path_input = r"input/"   # 在此更改字典输入文件夹
today = date.today().strftime("%Y%m%d")


df = pd.read_csv(input_df)  # 输入全量科目数据
df_formulae = pd.read_excel(path_input+"指标值计算数据字典.xlsx")  # 倘若数据字典的名称有修改，在这里更改


############################################################################
######################### 相关functions #####################################

# 将字典中的公式等信息以variable的形式表示
def gen_variable(my_formulae):
    tmp = []
    for equation in my_formulae:
        lst_variables1 = re.findall(r"([A-Z]{2}_\d{4}[_]?[1]?)", equation)
        lst_variables3 = re.findall(r"([a-z]{1,}_[A-Za-z]{1,}_\d*)", equation)
        lst_variables = lst_variables1 + lst_variables3
        for variable in lst_variables:
            tmp.append(variable)
    tmp = set(tmp)
    return tmp

# 单独计算byear特殊科目
def byear_indicator(df_name):
    temp_df = eval(df_name + ".copy()")
    temp_df['byear(PF_0102)'] = ''
    for index, row in temp_df.iterrows():
        idx = np.where((temp_df['BASIC_entity_name'] == row['BASIC_entity_name']) &
                       (temp_df['BASIC_year'] == row['BASIC_year'] - 1) &
                       (temp_df['BASIC_quarter'] == 4))
        try:
            value = temp_df.loc[idx]['PF_0102'].values[0]
            temp_df.at[index, 'byear(PF_0102)'] = value
        except:
            temp_df.at[index, 'byear(PF_0102)'] = np.nan

    temp_df['QW_PERFORMANCE_RATIO'] = temp_df['PF_0102'] / temp_df['byear(PF_0102)']
    temp_df['QW_RANGE_PERFORMANCE_RATIO'] = (temp_df['PF_0102'] - gq.last(temp_df['PF_0102'])) / temp_df[
        'byear(PF_0102)']
    return temp_df

# 单独计算特殊指标
def QW_NONRECURRENT_NETPROFIT_DELTA(df_name):
    temp_df = eval(df_name + ".copy()")
    temp_df['QW_NONRECURRENT_NETPROFIT_DELTA_1'] = temp_df['stmnote_Eoitems_24'] - temp_df['PF_0132']
    temp_df['QW_NONRECURRENT_NETPROFIT_DELTA_2'] = temp_df['stmnote_Eoitems_24'] - 0.5 * temp_df['PF_0132']
    temp_df['QW_NONRECURRENT_NETPROFIT_DELTA'] = temp_df.apply(lambda x: list([x['QW_NONRECURRENT_NETPROFIT_DELTA_1'],
                                                                               x['QW_NONRECURRENT_NETPROFIT_DELTA_2']]),
                                                               axis=1)
    temp_df2 = temp_df.drop(labels=['QW_NONRECURRENT_NETPROFIT_DELTA_1', 'QW_NONRECURRENT_NETPROFIT_DELTA_2'], axis=1)
    return temp_df2

# 调用general_quarterly里的函数，对指标进行计算
def calculate_indicator(df_name, equation_list):
    tmp_df = eval(df_name + ".copy()")
    for key, value in equation_list.items():
        check_comp = gq.check_components(value[0], tmp_df)
        if check_comp == 1:
            tmp_df[key] = eval(gq.parse_equation('tmp_df', value[0]))
        else:
            print("Error: can't calculate", key)
    return tmp_df

##########################################################################
######################### 数据预处理 #######################################

# 筛选所需年份与季度之后的数据（这里的年份和季度根据筛计算的指标值年份调整，提前9个季度）
df_after = df[df.BASIC_year >= start_year]
df_after_2 = df_after[(df_after.BASIC_year > start_year) | (df_after.BASIC_quarter >= start_quarter)]

# Reorder the dataframe
df2 = df_after_2.sort_values(
    by=["BASIC_entity_name", "BASIC_code", "BASIC_industry", "BASIC_year", "BASIC_quarter", "code", "end_value"])

# Replace duplicate observations by using the observation before
df3 = df2.drop_duplicates(
    subset=["BASIC_entity_name", "BASIC_code", "BASIC_industry", "BASIC_year", "BASIC_quarter", "code"], keep='first') #保留存在的值

# Long to wide (use indicators as column name)
df4 = df3.pivot(index=["BASIC_entity_name", "BASIC_code", "BASIC_industry", "BASIC_year", "BASIC_quarter"], #以主体划分
                columns="code",
                values="end_value").reset_index()

# 筛选需要计算的指标
lst_variable1 = ["BASIC_entity_name", "BASIC_code", "BASIC_industry", 'BASIC_year', 'BASIC_quarter']
lst_formula = list(df_formulae["公式"])
lst_variable2 = list(gen_variable(lst_formula))
# print(lst_variable2)
lst_variable1.extend(lst_variable2)
lst_variable1 = [variable for variable in lst_variable1 if variable in list(df4)]
df5 = df4[lst_variable1]

# 数据缓存
df5.to_csv(path_dfs+"季报数据宽表.csv")


# 补充数据，每一个主体都包涵 start 至 end 的行
year_quar = []
for i in range(start_year, end_year+1):
    if i == start_year:
        for j in range(start_quarter, 5):
            year_quar.append((i, j))
    elif i == end_year:
        for j in range(1, end_quarter+1):
            year_quar.append((i, j))
    else:
        for j in range(1, 5):
            year_quar.append((i, j))

df_temp = pd.DataFrame(columns=["BASIC_entity_name", "BASIC_year", "BASIC_quarter"])

lst_entity = set(df5["BASIC_entity_name"])

i = 0
for entity in lst_entity:
    for date in year_quar:
        df_temp.loc[i] = [entity, date[0], date[1]]
        i += 1

df_full = pd.merge(df_temp, df5, how='left', on=["BASIC_entity_name", "BASIC_year", "BASIC_quarter"])
df_full_sort = df_full.sort_values(by=["BASIC_entity_name", "BASIC_year", "BASIC_quarter"]).reset_index()

# 数据缓存
df_full_sort.to_csv(path_dfs+"季报数据宽表2.csv")

# 函数包需要调用缓存数据，请勿移动此import语句顺序
import general_quarterly as gq

# 转换数据格式
for col in df_full_sort.columns[6:]:
    df_full_sort[col] = pd.to_numeric(df_full_sort[col])

# 将数据表中的-999999.9999转为NaN
df_clean = df_full_sort.replace(-999999.9999, np.nan).drop(columns="index")

##########################################################################
################################ 指标预处理 ################################
# 生成计算公式变量
formulae_all = gq.gen_formulae(df_formulae, '指标代码', '公式', '类别', '计算优先级', '关键科目名', '季度')

# 按计算优先级进行拆分
formulae_0 = gq.select_formulae(formulae_all, 0)
formulae_1 = gq.select_formulae(formulae_all, 1)
formulae_2 = gq.select_formulae(formulae_all, 2)

#########################################################################
################################ 指标计算 ################################

# 计算每个观测值所包含的前期值
df_consecutive = df_clean.copy()
df_consecutive['BASIC_consecutive'] = ''

# 填充basic_consecutive的值
for index, row in df_clean.iterrows():
    year_diff = row["BASIC_year"] - start_year
    quarter_diff = row["BASIC_quarter"] - start_quarter
    date = year_diff * 4 + quarter_diff
    df_consecutive.at[index, "BASIC_consecutive"] = [i for i in range(1, date + 1)]

# 计算包含byear()的指标
df_data_byear = byear_indicator("df_consecutive")

# 计算指标 QW_NONRECURRENT_NETPROFIT_DELTA
df_data_QW_NONRECURRENT_NETPROFIT_DELTA = QW_NONRECURRENT_NETPROFIT_DELTA("df_data_byear")


# 按优先级计算其余指标
df_data_0 = calculate_indicator("df_data_QW_NONRECURRENT_NETPROFIT_DELTA", formulae_0)
# df_data_0 = df_data_0['PF_0139'].fillna(0)

data_types_dict1 = {'byear(PF_0102)': float}
data_types_dict2 = {'QW_PERFORMANCE_RATIO': float}
data_types_dict3 = {'QW_RANGE_PERFORMANCE_RATIO': float}
data_types_dict4 = {'QW_NONRECURRENT_NETPROFIT_DELTA': float}
df_data_0 = df_data_0.astype(data_types_dict1)
df_data_0 = df_data_0.astype(data_types_dict2)
df_data_0 = df_data_0.astype(data_types_dict3)
# df_data_0 = df_data_0.astype(data_types_dict4)


df_data_1 = calculate_indicator("df_data_0", formulae_1)
df_data_2 = calculate_indicator("df_data_1", formulae_2)

# 将无前期数据的指标修改为空值
df_data_0 = gq.interval(df_data_2, formulae_0)
df_data_1 = gq.interval(df_data_0, formulae_1)
df_data_2 = gq.interval(df_data_1, formulae_2)

# 将不需要为此季度计算的指标修改为空值
df_data_0 = gq.quarter(df_data_2, formulae_0)
df_data_1 = gq.quarter(df_data_0, formulae_1)
df_data_2 = gq.quarter(df_data_1, formulae_2)

##########################################################################
######################### 指标数据缓存 #####################################

# 缺失值填充为-999999.9999
df_data_final = df_data_2.fillna(-999999.9999)
df_data_final = df_data_final.replace(np.inf, -999999.9999)
df_data_final = df_data_final.replace(-np.inf, -999999.9999)


# 仅保留基础列和指标列
basic = ['BASIC_entity_name', 'BASIC_code', 'BASIC_industry', 'BASIC_year', 'BASIC_quarter']
indicators = [item for item in df_data_final if item not in df_clean]
basic.extend(indicators)
df_data_final = df_data_final[basic]
df_data_final = df_data_final.drop(labels='BASIC_consecutive', axis=1)
# 删除数据缓存
os.remove(path_dfs+"季报数据宽表.csv")
os.remove(path_dfs+"季报数据宽表2.csv")

# 输出结果
df_data_final.to_csv(output_df, index=False)
