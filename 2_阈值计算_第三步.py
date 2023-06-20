# _*_ coding utf-8 _*_
# Author: You Tian
# Date: 24/7/2022 11:32 am
# File name: 2_阈值计算

############################################################################
######################### 代码说明 #####################################
# 该步骤通常不与1，3两部分一同运行，只在需要更新阈值数据的时候运行即可
# 用后台的指标计算阈值（争取同期限），与后台的阈值比较，得出阈值结果差异，判断我们的阈值计算有没有问题


import pandas as pd
import numpy as np
import re
from datetime import date
import sys

############################################################################
######################### 数据读取 #####################################
# input_df = sys.argv[1]
# output_df = sys.argv[2]

# 后台指标
input_df = '/Users/zhuangyuhao/PycharmProjects/SeasonRepMod/整合季报代码模型/meta_code/19-22合并数据_230109.csv'
output_df = '/Users/zhuangyuhao/PycharmProjects/SeasonRepMod/整合季报代码模型/meta_output/第三步_阈值计算结果_20230110.csv'

path_dfs = r"dfs/"
path_input = r"input/"

df_trigger = pd.read_excel(path_input + "阈值计算数据字典.xlsx")  # 倘若数据字典的名称有修改，在这里更改
df = pd.read_csv(input_df)

#######################################################################################
##################################### 数据处理 #####################################


df_clean = df.replace(-999999.9999, np.nan)

#######################################################################################
##################################### 阈值异常值处理 #####################################

outlier_indicator_dict = {}
for index, row in df_trigger.iterrows():
    if row["阈值异常值处理"] != "-":
        if row["指标代码"] in outlier_indicator_dict:
            outlier_indicator_dict[row["指标代码"]].append(row["季度"])
        elif row["指标代码"] not in outlier_indicator_dict:
            outlier_indicator_dict[row["指标代码"]] = [row["季度"]]

for key, value in outlier_indicator_dict.items():
    quarter = value
    indicator = key
    indicator_per_high = indicator + "_percentail_high"
    indicator_per_low = indicator + "_percentail_low"
    try: ################################################### 倘若指标数据缺失，这里会报错。采用try函数跳过部分暂时缺失的指标
        df_clean[indicator_per_high] = df_clean.groupby(["BASIC_quarter","BASIC_industry"])[indicator].transform(lambda x: np.nanpercentile(x, 95))
        df_clean[indicator_per_low] = df_clean.groupby(["BASIC_quarter", "BASIC_industry"])[indicator].transform(lambda x: np.nanpercentile(x, 5))

        df_clean.loc[(df_clean[indicator] > df_clean[indicator_per_high])|(df_clean[indicator] < df_clean[indicator_per_low]), indicator] = np.nan
    except:
        print(key)


#######################################################################################
##################################### 阈值选取对象 #####################################
ignorelist =[]   # 统计被跳过未计算的指标阈值

industry_avg = df_clean.groupby(["BASIC_quarter", "BASIC_industry"]).mean().reset_index()
industry_avg = industry_avg.add_suffix('_avg')


def select_entity(my_data, condition, requirements):
    temp_df = my_data.copy()

    if condition == "<【0】":
        temp_df_filtered = temp_df.loc[temp_df[requirements] < 0]
        return temp_df_filtered
    elif condition == ">【0】":
        temp_df_filtered = temp_df.loc[temp_df[requirements] > 0]
        return temp_df_filtered
    elif condition == "<【行业均值】":
        temp_df = pd.merge(temp_df, industry_avg, how='left',
                           left_on=["BASIC_quarter", "BASIC_industry"],
                           right_on=['BASIC_quarter_avg', 'BASIC_industry_avg'])
        avg = requirements + "_avg"
        temp_df_filtered = temp_df.loc[temp_df[requirements] < temp_df[avg]]
        return temp_df_filtered
    elif condition == ">【行业均值】":
        avg = requirements + "_avg"
        temp_df = pd.merge(temp_df, industry_avg, how='left',
                           left_on=["BASIC_quarter", "BASIC_industry"],
                           right_on=['BASIC_quarter_avg', 'BASIC_industry_avg'])
        temp_df_filtered = temp_df.loc[temp_df[requirements] > temp_df[avg]]
        return temp_df_filtered
    elif condition == "下降的":
        lag = "lag(" + requirements + ")"
        btlag = "btlag(" + requirements + ")"
        temp_df[lag] = temp_df[requirements].shift(4).where(temp_df.BASIC_entity_name.eq(temp_df.BASIC_entity_name.shift(4)))
        temp_df[btlag] = temp_df[requirements].shift(8).where(temp_df.BASIC_entity_name.eq(temp_df.BASIC_entity_name.shift(8)))
        temp_df_filtered = temp_df.loc[(temp_df[requirements] < temp_df[lag]) & (temp_df[lag] < temp_df[btlag])]
        return temp_df_filtered
    elif condition == "上升的":
        lag = "lag(" + requirements + ")"
        btlag = "btlag(" + requirements + ")"
        temp_df[lag] = temp_df[requirements].shift(4).where(temp_df.BASIC_entity_name.eq(temp_df.BASIC_entity_name.shift(4)))
        temp_df[btlag] = temp_df[requirements].shift(8).where(temp_df.BASIC_entity_name.eq(temp_df.BASIC_entity_name.shift(8)))
        temp_df_filtered = temp_df.loc[(temp_df[requirements] > temp_df[lag]) & (temp_df[lag] > temp_df[btlag])]
        return temp_df_filtered


trigger = pd.DataFrame(columns=["季度", "指标代码", "项目代码",  "industry", "general_trigger", "high_trigger"])

for index, row in df_trigger.iterrows():
    indicator_code = row["指标代码"]
    project_code = row["项目代码"]
    filter_entity = row["阈值选取对象"]
    general = row["普通阈值"]
    high = row["高危阈值"]

    temp_trigger = trigger.copy()
    try: ################################################### 倘若指标数据缺失，这里会报错。采用try函数跳过部分暂时缺失的指标

        if filter_entity == "-":
            temp_trigger = df_clean[df_clean["BASIC_quarter"] == row["季度"]]
            temp_trigger = temp_trigger.groupby(["BASIC_quarter", "BASIC_industry"]).size().reset_index()
            temp_trigger["指标代码"] = indicator_code
            temp_trigger["项目代码"] = project_code

            if "绝对值" in general and "绝对值" in high:
                temp_trigger["general_trigger"] = int(re.findall(r"(-?\d+)%\（", general)[0])/100
                temp_trigger["high_trigger"] = int(re.findall(r"(-?\d+)%\（", high)[0])/100
            elif "-" == general and "-" == high:
                temp_trigger["general_trigger"] = np.nan
                temp_trigger["high_trigger"] = np.nan
            elif "对应行业均值" in general and "对应行业均值" in high:
                avg = indicator_code + "_avg"
                temp_trigger = pd.merge(temp_trigger, industry_avg, how='left',
                                        left_on=["BASIC_quarter", "BASIC_industry"],
                                        right_on=['BASIC_quarter_avg', 'BASIC_industry_avg'])
                temp_trigger["general_trigger"] = temp_trigger[avg] - int(re.findall(r"(-?\d+)", general)[0])/100
                temp_trigger["high_trigger"] = temp_trigger[avg] - int(re.findall(r"(-?\d+)", high)[0])/100

            temp_trigger.rename(columns={"BASIC_quarter": "季度",  "BASIC_industry": "industry"},
                                inplace=True)
            temp_trigger = temp_trigger[["季度", "指标代码", "项目代码", "industry", "general_trigger", "high_trigger"]]
            trigger = pd.concat([trigger, temp_trigger])

        elif filter_entity == "【同行业】公司":
            temp_trigger = df_clean[df_clean["BASIC_quarter"] == row["季度"]]
            temp_trigger["general_trigger"] = temp_trigger.groupby(["BASIC_quarter", "BASIC_industry"])[
                indicator_code].transform(lambda x: np.nanpercentile(x, general))
            temp_trigger["high_trigger"] = temp_trigger.groupby(["BASIC_quarter", "BASIC_industry"])[
                indicator_code].transform(lambda x: np.nanpercentile(x, high))
            temp_trigger = temp_trigger.groupby(
                ["BASIC_quarter", "BASIC_industry", "general_trigger", "high_trigger"]).size().reset_index()
            temp_trigger["指标代码"] = indicator_code
            temp_trigger["项目代码"] = project_code
            temp_trigger.rename(columns={"BASIC_quarter": "季度", "BASIC_industry": "industry"},
                                inplace=True)
            temp_trigger = temp_trigger[["季度", "指标代码", "项目代码",  "industry", "general_trigger", "high_trigger"]]
            trigger = pd.concat([trigger, temp_trigger])

        elif (">" in filter_entity or "<" in filter_entity) and "&" not in filter_entity:
            requirements = filter_entity[1:filter_entity.find("】")]
            condition = filter_entity[filter_entity.find("】")+1:filter_entity.find("】")+filter_entity[filter_entity.find("】")+1:].find("】")+2]
            temp_df = select_entity(df_clean, condition, requirements)

            temp_trigger = temp_df[temp_df["BASIC_quarter"] == row["季度"]]
            temp_trigger["general_trigger"] = temp_trigger.groupby(["BASIC_quarter", "BASIC_industry"])[
                indicator_code].transform(lambda x: np.nanpercentile(x, general))
            temp_trigger["high_trigger"] = temp_trigger.groupby(["BASIC_quarter", "BASIC_industry"])[
                indicator_code].transform(lambda x: np.nanpercentile(x, high))
            temp_trigger = temp_trigger.groupby(
                ["BASIC_quarter", "BASIC_industry", "general_trigger", "high_trigger"]).size().reset_index()
            temp_trigger["指标代码"] = indicator_code
            temp_trigger["项目代码"] = project_code
            temp_trigger.rename(columns={"BASIC_quarter": "季度", "BASIC_industry": "industry"},
                                inplace=True)
            temp_trigger = temp_trigger[["季度", "指标代码", "项目代码",  "industry", "general_trigger", "high_trigger"]]
            trigger = pd.concat([trigger, temp_trigger])
        elif "【连续三年】" in filter_entity:

            requirements = re.findall(r"\【([a-zA-Z0-9_]+)\】", filter_entity)[0]
            condition = re.findall(r"\】(\w+)\【", filter_entity)[0]
            temp_df = select_entity(df_clean, condition, requirements)
            temp_trigger = temp_df[temp_df["BASIC_quarter"] == row["季度"]]

            temp_trigger["general_trigger"] = temp_trigger.groupby(["BASIC_quarter"])[
                indicator_code].transform(lambda x: np.nanpercentile(x, general))
            temp_trigger["high_trigger"] = temp_trigger.groupby(["BASIC_quarter"])[
                indicator_code].transform(lambda x: np.nanpercentile(x, high))
            temp_trigger = temp_trigger.groupby(
                ["BASIC_quarter", "BASIC_industry", "general_trigger", "high_trigger"]).size().reset_index()
            temp_trigger["指标代码"] = indicator_code
            temp_trigger["项目代码"] = project_code
            temp_trigger.rename(columns={"BASIC_quarter": "季度", "BASIC_industry": "industry"},
                                inplace=True)
            temp_trigger = temp_trigger[["季度", "指标代码", "项目代码", "industry", "general_trigger", "high_trigger"]]
            trigger = pd.concat([trigger, temp_trigger])

        elif "&" in filter_entity:

            s = filter_entity.split("&")
            temp_df = df_clean.copy()
            for i in s:
                requirements = re.findall(r"【([\w\+]+)】", i)[0]
                condition = re.findall(r"([<|>]【\w+】)", i)[0]
                temp_df = select_entity(temp_df, condition, requirements)

            temp_trigger = temp_df[temp_df["BASIC_quarter"] == row["季度"]]
            temp_trigger["general_trigger"] = temp_trigger.groupby(["BASIC_quarter", "BASIC_industry"])[
                indicator_code].transform(lambda x: np.nanpercentile(x, general))
            temp_trigger["high_trigger"] = temp_trigger.groupby(["BASIC_quarter", "BASIC_industry"])[
                indicator_code].transform(lambda x: np.nanpercentile(x, high))
            temp_trigger = temp_trigger.groupby(
                ["BASIC_quarter", "BASIC_industry", "general_trigger", "high_trigger"]).size().reset_index()
            temp_trigger["指标代码"] = indicator_code
            temp_trigger["项目代码"] = project_code
            temp_trigger.rename(columns={"BASIC_quarter": "季度", "BASIC_industry": "industry"},
                                inplace=True)
            temp_trigger = temp_trigger[["季度", "指标代码", "项目代码",  "industry", "general_trigger", "high_trigger"]]
            trigger = pd.concat([trigger, temp_trigger])

    except:
        ignorelist.append(requirements)
        print(requirements)  # 展示计算中忽略和跳过的指标


print(ignorelist)  # 展示计算中忽略和跳过的指标

# 输出结果
# trigger.to_excel(output_df,index=False)
trigger.to_csv(output_df, index=False)
