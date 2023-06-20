# _*_ coding utf-8 _*_
# Author: Yiwen Jiang
# Date: 14/4/2022 9:56 am
# File name: general_quarterly

# Library packages
import pandas as pd
import numpy as np
import os

import re
path_dfs = r"dfs/"
df_data = pd.read_csv(path_dfs+'季报数据宽表.csv')
df_data1 = pd.read_csv(path_dfs+'季报数据宽表2.csv')
########################################################################
################################ 指标相关 ################################

def gen_formulae(my_df, col_code, col_eq, col_category, col_order, col_important, col_quarter):
    tmp = my_df.copy()
    tmp = tmp.dropna(subset=[col_eq]).sort_values(by=[col_order])
    lst_code = list(tmp[col_code])
    lst_eq = list(tmp[col_eq])
    lst_cat = list(tmp[col_category])
    lst_order = list(tmp[col_order].astype(int))
    lst_import_index = list(tmp[col_important])
    lst_quarter = list(tmp[col_quarter])
    results = list(zip(lst_code, lst_eq, lst_cat, lst_order, lst_import_index, lst_quarter))
    return results


def select_formulae(my_formulae, my_order):
    temp_dict = {}
    for code, formula, category, order, import_index, quarter in my_formulae:
        if order == my_order and code not in temp_dict:
            lst_infor = [formula, category, import_index, []]
            lst_infor[3].append(quarter)
            temp_dict[code] = lst_infor
        elif order == my_order and code in temp_dict:
            lst_infor = temp_dict[code]
            lst_infor[3].append(quarter)
    return temp_dict


##########################################################################
################################ 数据预处理 ################################

def insert_consecutive(my_df):
    temp_df = my_df.copy()
    temp_df['BASIC_consecutive'] = ""
    temp_df = temp_df.sort_values(by=['BASIC_entity_name', 'BASIC_year', 'BASIC_quarter'])
    lst_entity = []
    for j in range(temp_df.shape[0]):
        if lst_entity.count(temp_df['BASIC_entity_name'][j]) > 1:
            temp_year = (temp_df['BASIC_year'][j] - temp_df['BASIC_year'][j - 1]) * 4
            temp_quarter = temp_df['BASIC_quarter'][j] - temp_df['BASIC_quarter'][j - 1]
            lst_temp = temp_df.at[j - 1, 'BASIC_consecutive']
            k = temp_year + temp_quarter
            lst_temp2 = [item + k for item in lst_temp]
            lst_temp2.append(k)
            temp_df.at[j, 'BASIC_consecutive'] = lst_temp2
        elif lst_entity.count(temp_df['BASIC_entity_name'][j]) == 1:
            temp_year = (temp_df['BASIC_year'][j] - temp_df['BASIC_year'][j - 1]) * 4
            temp_quarter = temp_df['BASIC_quarter'][j] - temp_df['BASIC_quarter'][j - 1]
            lst_temp = [temp_year + temp_quarter]
            temp_df.at[j, 'BASIC_consecutive'] = lst_temp
        else:
            temp_df.at[j, 'BASIC_consecutive'] = [0]
        lst_entity.append(temp_df['BASIC_entity_name'][j])
    return temp_df


#########################################################################
################################ 指标计算 ################################

def select_components(my_formula):
    temp = my_formula
    lst_operator = ['+', '-', '*', '/', '(', ')', ',']
    lst_function = ['last', 'ylast', 'blast', 'lag', '1', 'average', 'abs', 'std', '2', 'byear', 'btlast', 'btlag']
    for operator in lst_operator:
        temp = temp.replace(operator, ";")
    lst1 = [x.strip() for x in temp.split(';') if x]
    lst2 = []
    for item in lst1:
        if item not in lst_function:
            lst2.append(item)
    return list(set(lst2))


def check_components(my_equation, my_df):
    temp_cols = my_df.columns
    temp_check = 1
    lst_component = select_components(my_equation)
    for item in lst_component:
        if item in temp_cols:
            temp_check *= 1
        else:
            temp_check *= 0
            print("Error:", item, "is missing")
    return temp_check


def covert_field(my_df, my_variable):
    lst_function = ['ylast', 'blast', 'lag', '1', 'average', 'abs', 'std', '2', 'byear', 'btlast', 'btlag', 'last']
    lst_built_in = ['1', '2', 'abs']
    if my_variable not in lst_function:
        df_format = my_df + "['" + my_variable + "']"
        return df_format
    elif my_variable not in lst_built_in:
        df_format = "gq." + my_variable
        return df_format
    else:
        return my_variable


def parse_equation(my_df, my_equation):
    temp = my_equation
    lst_operator = ['+', '-', '*', '/', '(', ')', ',']
    lst_equation = []
    temp_str = ""
    i = 0
    for item in temp:
        i += 1
        if item not in lst_operator and i < len(temp):
            temp_str += item
        elif item not in lst_operator and i == len(temp):
            temp_str += item
            lst_equation.append(covert_field(my_df, temp_str))
        else:
            if temp_str == "":
                lst_equation.append(item)
            else:
                lst_equation.append(covert_field(my_df, temp_str))
                lst_equation.append(item)
                temp_str = ""

    str_equation = ''.join(map(str, lst_equation))
    return str_equation


def lag(variable):
    return variable.shift(4)


def btlag(variable):
    return variable.shift(8)


def ylast(variable):
    return variable.shift(1)


def last(variable):
    res = variable.shift(1)
    for idx in range(len(df_data["BASIC_quarter"])):
        if df_data1["BASIC_quarter"][idx] == 1:
            res[idx] = 0

    return res

def blast(variable):
    res = variable.shift(5)
    for idx in range(len(df_data["BASIC_quarter"])):
        if df_data1["BASIC_quarter"][idx] == 1:
            res[idx] = 0

    return res

def btlast(variable):
    res = variable.shift(9)
    for idx in range(len(df_data["BASIC_quarter"])):
        if df_data1["BASIC_quarter"][idx] == 1:
            res[idx] = 0

    return res

# def blast(variable):
#     return variable.shift(5)
#
# def btlast(variable):
#     return variable.shift(9)


def std(variable):
    temp = pd.concat([variable, variable.shift(1), variable.shift(2),
                      variable.shift(3), variable.shift(4), variable.shift(5),
                      variable.shift(6), variable.shift(7)], axis=1)
    temp_std = temp.std(axis=1)
    return temp_std


def interval_satisfy(lst1, lst2):
    i = 0
    # 修改
    try:
        lst1 = eval(lst1)
    except:
        lst1 = lst1

    if lst1 in [np.nan]:
        return 1
    else:
        for item in lst1:
            if item not in lst2:
                i += 1
            else:
                i *= 1
    if i == 0:
        return 1
    else:
        return 0


def interval(my_data, my_formula):
    temp_df = my_data.copy()
    for index, row in temp_df.iterrows():
        for key, value in my_formula.items():
            required_time_interval = value[1]
            real_time_interval = row['BASIC_consecutive']
            check_interval = interval_satisfy(required_time_interval, real_time_interval)
            if check_interval == 0:
                temp_df.at[index, key] = None
            else:
                pass
    return temp_df


def quarter(my_data, my_formula):
    temp_df = my_data.copy()
    for index, row in temp_df.iterrows():
        for key, value in my_formula.items():
            if 0 not in value[3] and row['BASIC_quarter'] not in value[3]:
                temp_df.at[index, key] = None
            else:
                pass
    return temp_df