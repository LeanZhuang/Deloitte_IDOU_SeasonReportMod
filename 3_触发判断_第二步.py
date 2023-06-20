# _*_ coding utf-8 _*_
# Author: You Tian
# Date: 29/7/2022 5:51 pm
# File name: 3_触发判断


############################################################################
######################### 该代码说明 #####################################
# 本代码可以拆分为两个模块，触发判断+指标特殊值处理
# 触发判断只能选择一个季度的数据进行运算，如若需要计算多季的触发结果可以在master.py中添加循环
# 触发部分判断需要用到三年内的科目以及指标数据，因此这两部分的输入都需要有三年以上的时间跨度

# 触发判断部分仍需完善/注意的地方：
#    1. category 4.减半差值>0，完整差值>0。 这部分由于数据缺失，目前代码处理是暂时跳过该类型的判断
#    2. 连续三年下降/上升: 目前因为用于判断的指标数据缺失三年期的完整数据，因此代码中并没有采用给定的判断规则，而是使用了等效的替代方法进行判断。详见该部分代码说明

# 指标特殊值处理部分目前平均单季数据运行时间较长，需要20分钟左右，有待优化

# Library packages
import pandas as pd
import numpy as np
from datetime import date
import sys

############################################################################
######################### 数据读取 #####################################


input_df1 = '/Users/zhuangyuhao/PycharmProjects/SeasonRepMod/整合季报代码模型/dfs/原始科目数据_20230104.csv'

# 后台指标
input_df2 = '/Users/zhuangyuhao/PycharmProjects/SeasonRepMod/整合季报代码模型/整合后台数据/后台指标20Q1_22Q3_230113.csv'

# 后台阈值
input_df3 = '/Users/zhuangyuhao/PycharmProjects/SeasonRepMod/整合季报代码模型/meta_code/分行业阈值触发表_20220926.xlsx'

output_df = '/Users/zhuangyuhao/PycharmProjects/SeasonRepMod/整合季报代码模型/dfs/22Q3_触发项目及触发等级_20230113.xlsx'

year_cmp = int(2022)
quarter_cmp = int(3)

path_dfs = r"dfs/"
path_input = r"input/"
today = date.today().strftime("%Y%m%d")

# 导入阈值触发表
df_threshold = pd.read_excel(input_df3)
# 导入指标数据
df_data = pd.read_csv(input_df2)
# 指标异常值处理规则字段导入
formu = pd.read_excel(path_input+"触发判断数据字典.xlsx")  # 倘若数据字典的名称有修改，在这里更改
# 数据清洗
df_threshold_clean = df_threshold[['季度', '指标代码', '项目代码', 'industry', 'general_trigger', 'high_trigger']].drop_duplicates()
df_threshold_clean = pd.merge(df_threshold_clean, formu[['项目代码', 'category']], on='项目代码', how='left')

df_data_clean = df_data.rename({"BASIC_entity_name": "entity_name", "BASIC_code": "code", "BASIC_industry": "industry", "BASIC_year": "year", "BASIC_quarter": "季度"}, axis=1)
df_data_clean = df_data_clean.replace(-999999.9999, np.nan)


# 导入需要计算的指标年份与季度

df_data = df_data.loc[(df_data['BASIC_year'] == year_cmp) & (df_data['BASIC_quarter'] == quarter_cmp)]
df_data=df_data.drop_duplicates(subset=["BASIC_entity_name",  "BASIC_year", "BASIC_code", 'BASIC_quarter'], keep="first")
df_data = pd.melt(df_data,
                  id_vars=df_data.columns[:6],
                  value_vars=df_data.columns[6:],
                  var_name='code',
                  value_name='finnal_value')

# 筛选需要触发的指标
indicator_lst = list(set(df_threshold_clean['指标代码']))
df_data = df_data[df_data['code'].isin(indicator_lst)]
df_data =df_data.drop_duplicates()
# merge项目代码与判断规则分类
df_data.drop(columns='Unnamed: 0', inplace=True)
df_data_comb = pd.merge(df_data,df_threshold_clean.drop_duplicates(),how='left',left_on=['code','BASIC_quarter','BASIC_industry'],right_on=['指标代码','季度','industry'])
df_data_comb.drop(columns=['季度','指标代码','industry'],inplace=True)
df_data_comb.columns = ["entity_name",  "stock_code", "industry_whitewash","year", "季度","indicators","finnal_value",'code' ,"general_trigger", "high_trigger", "category"]
df_data_comb = df_data_comb.replace([-999999.9999], np.nan)


######################################################
##################### 计算触发结果 #####################

# 按判断规则拆分数据
df_category_0 = df_data_comb.loc[df_data_comb['category'] == 0]
df_category_1 = df_data_comb.loc[df_data_comb['category'] == 1]
df_category_2 = df_data_comb.loc[df_data_comb['category'] == 2]
df_category_3 = df_data_comb.loc[df_data_comb['category'] == 3]
df_category_4 = df_data_comb.loc[df_data_comb['category'] == 4]
df_category_5 = df_data_comb.loc[df_data_comb['category'] == 5]
df_category_6 = df_data_comb.loc[df_data_comb['category'] == 6]
df_category_7 = df_data_comb.loc[df_data_comb['category'] == 7]
df_category_8 = df_data_comb.loc[df_data_comb['category'] == 8]
df_category_9 = df_data_comb.loc[df_data_comb['category'] == 9]
df_category_10 = df_data_comb.loc[df_data_comb['category'] == 10]
df_category_11 = df_data_comb.loc[df_data_comb['category'] == 11]
df_category_12 = df_data_comb.loc[df_data_comb['category'] == 12]


# 1.低于0
df_category_0['is_high_trigger'] = np.where(df_category_0['finnal_value'] < 0, True, False)
df_category_0['is_general_trigger'] = False

df_category_2['is_high_trigger'] = False
df_category_2['is_general_trigger'] = np.where(df_category_2['finnal_value'] < 0, True, False)


# 2.上期>0，本期<0
df_category_3 = df_category_3.sort_values(by=["indicators", "entity_name", "year", "季度", "code"]).reset_index()

# 该分类仅涉及两个指标，因此这里直接提取这两个指标字段进行计算
lst = ['entity_name','QW_OPERATINGCASHFLOW', 'QW_RANGE_OPERATINGCASHFLOW']
lst = [variable for variable in lst if variable in list(df_data_clean)]

df3_merge1= df_data_clean[lst].loc[(df_data_clean['year'] == year_cmp) & (df_data_clean['季度'] == quarter_cmp-1)]
df3_merge1 = pd.melt(df3_merge1,
                     id_vars=df3_merge1.columns[:1],
                     value_vars=df3_merge1.columns[1:],
                     var_name='indicators',
                     value_name='finnal_value_last')
df_category_3 = pd.merge(df_category_3, df3_merge1, how='left', on=['entity_name','indicators'])

df_category_3['is_general_trigger'] = np.where((df_category_3['finnal_value'] < 0) & (df_category_3["finnal_value_last"] > 0), True, False)
df_category_3['is_high_trigger'] = False

df_category_3 = df_category_3[["entity_name", "stock_code", "industry_whitewash", "year", "季度", "code", "finnal_value", 'general_trigger','high_trigger', "category", "is_high_trigger", "is_general_trigger"]]


# 3.连续三年<0
# 将去年与前年同期值merge到数据中，便于执行判断
df1_merge1 = df_data_clean[lst].loc[(df_data_clean['year'] == year_cmp-1) & (df_data_clean['季度'] == quarter_cmp)]
df1_merge2 = df_data_clean[lst].loc[(df_data_clean['year'] == year_cmp-2) & (df_data_clean['季度'] == quarter_cmp)]
df1_merge1 = pd.melt(df1_merge1,
                     id_vars=df1_merge1.columns[:1],
                     value_vars=df1_merge1.columns[1:],
                     var_name='indicators',
                     value_name='finnal_value_lag')
df1_merge2 = pd.melt(df1_merge2,
                     id_vars=df1_merge2.columns[:1],
                     value_vars=df1_merge2.columns[1:],
                     var_name='indicators',
                     value_name='finnal_value_btlag')
df_category_1 = pd.merge(df_category_1, df1_merge1, how='left', on=['entity_name', 'indicators'])
df_category_1 = pd.merge(df_category_1, df1_merge2, how='left', on=['entity_name', 'indicators'])
df_category_1 = df_category_1.sort_values(by=["indicators", "entity_name", "year", "季度", "code"]).reset_index()
df_category_1['is_high_trigger'] = np.where((df_category_1['finnal_value'] < 0) &
                                            (df_category_1["finnal_value_lag"] < 0) &
                                            (df_category_1["finnal_value_btlag"] < 0), True, False)
df_category_1['is_general_trigger'] = False

df_category_1 = df_category_1[["entity_name", "stock_code", "industry_whitewash", "year", "季度", "code", "finnal_value", 'general_trigger','high_trigger', "category", "is_high_trigger", "is_general_trigger"]]


# 4.减半差值>0，完整差值>0。 这部分由于数据缺失，暂时跳过判断
df_category_4['is_high_trigger'] = np.where(df_category_4['finnal_value'] > 0, True, False)
df_category_4['is_general_trigger'] = False


# 5.监测指标 ≥【普通阈值】	监测指标 ≥【高危阈值】
df_category_5['is_high_trigger'] = np.where(df_category_5['finnal_value'] <= df_category_5['high_trigger'], True, False)
df_category_5['is_general_trigger'] = np.where(df_category_5['finnal_value'] <= df_category_5['general_trigger'], True, False)


# 6.监测指标 ≥【普通阈值】	监测指标 ≥【高危阈值】
df_category_6['is_high_trigger'] = np.where(df_category_6['finnal_value'] >= df_category_6['high_trigger'], True, False)
df_category_6['is_general_trigger'] = np.where(df_category_6['finnal_value'] >= df_category_6['general_trigger'], True, False)


# 7.连续三年下降:
# 判断方法是看 '指标_Growth_2'< '指标_Growth' < 0，即可证明三年连续下降.之所以采取这种判断方式是因为21年之前的指标数据缺失，如若数据补齐则可考虑改回原先判断方式
temp = df_category_5[['entity_name', 'year', '季度', 'indicators', 'finnal_value']].drop_duplicates()
temp.rename(columns={
    'indicators': 'indicators2',
    "finnal_value": "finnal_value2"},
    inplace=True)
# 在category7的indicator字段末尾删去_2，便于和category5合并。
df_category_7['indicators2'] = df_category_7['indicators'].str.slice(stop = -2)
df_category_7 = pd.merge(df_category_7,temp,on=['entity_name', 'year', '季度', 'indicators2'], how='left')
df_category_7['is_high_trigger'] = np.where((df_category_7['finnal_value'] <= df_category_7['high_trigger']) & (df_category_7['finnal_value'] < df_category_7['finnal_value2']) & (df_category_7['finnal_value2'] < 0), True, False)
df_category_7['is_general_trigger'] = np.where((df_category_7['finnal_value'] <= df_category_7['general_trigger']) & (df_category_7['finnal_value'] < df_category_7['finnal_value2']) & (df_category_7['finnal_value2'] < 0), True, False)
df_category_7.drop(columns=['indicators2', 'finnal_value2'], inplace=True)


# 8.连续三年上升
# 判断方法类似连续三年下降
temp = df_category_6[['entity_name', 'year', '季度', 'indicators', 'finnal_value']]
temp.rename(columns={
    'indicators': 'indicators2',
    "finnal_value": "finnal_value2"
    }, inplace=True)
# 在category8的indicator字段末尾删去_2，便于和category6合并。
df_category_8['indicators2'] = df_category_8['indicators'].str.slice(stop=-2)
df_category_8 = pd.merge(df_category_8,temp,on=['entity_name','year','季度','indicators2'],how='left')
df_category_8['is_high_trigger'] = np.where((df_category_8['finnal_value'] >= df_category_8['high_trigger']) & (df_category_8['finnal_value'] > df_category_8['finnal_value2']) & (df_category_8['finnal_value2'] > 0) , True, False)
df_category_8['is_general_trigger'] = np.where((df_category_8['finnal_value'] >= df_category_8['general_trigger']) & (df_category_8['finnal_value'] > df_category_8['finnal_value2']) & (df_category_8['finnal_value2'] > 0) , True, False)
df_category_8.drop(columns=['indicators2','finnal_value2'],inplace=True)


# 9.毛利率单判断<
temp = df_category_0[['entity_name','year','季度','indicators','is_general_trigger']]
temp.rename(columns={
    'indicators': 'judge',
    "is_general_trigger": "is_negative"
    }, inplace=True)
temp.judge[temp.judge == 'QW_GROSSPROFITMARGIN'] = 0
temp.judge[temp.judge == 'QW_RANGE_GROSSPROFITMARGIN'] = 1

df_category_9['judge'] = np.where(df_category_9['indicators'].str.contains('RANGE'), 1, 0)

df_category_9 = pd.merge(df_category_9, temp, on=['entity_name', 'year', '季度', 'judge'], how='left')
df_category_9['is_high_trigger'] = np.where((df_category_9['finnal_value'] <= df_category_9['high_trigger']) & (df_category_9['is_negative'] != True), True, False)
df_category_9['is_general_trigger'] = np.where((df_category_9['finnal_value'] <= df_category_9['general_trigger']) & (df_category_9['is_negative'] != True), True, False)
df_category_9.drop(columns=['is_negative', 'judge'],inplace=True)


# 10.毛利率单判断>
df_category_10['judge']=np.where(df_category_10['indicators'].str.contains('RANGE'), 1,0)

df_category_10 = pd.merge(df_category_10,temp,on=['entity_name','year','季度','judge'],how='left')
df_category_10['is_high_trigger'] = np.where((df_category_10['finnal_value'] >= df_category_10['high_trigger']) & (df_category_10['is_negative'] != True) , True, False)
df_category_10['is_general_trigger'] = np.where((df_category_10['finnal_value'] >= df_category_10['general_trigger']) & (df_category_10['is_negative'] != True), True, False)
df_category_10.drop(columns = ['is_negative','judge'],inplace=True)


# 11.毛利率双重判断<
temp = df_category_0[['entity_name','year','季度','indicators','is_general_trigger']]
temp.rename(columns= {
    "is_general_trigger": "is_negative"
    }, inplace= True)
temp = temp.loc[temp['indicators']=='QW_GROSSPROFITMARGIN'].drop(columns = 'indicators')

temp2 = df_category_9[['entity_name','year','季度','indicators','is_general_trigger']]
temp2 = temp2.loc[(temp2['indicators']=='QW_RANGE_GROSSPROFITMARGIN_GROWTH') | (temp2['indicators']=='QW_RANGE_GROSSPROFITMARGIN_DELTA') ]
temp2.rename(columns= {
    'indicators': 'judge',
    "is_general_trigger": "is_change"
    }, inplace=True)

df_category_11['judge']=np.where(df_category_11['indicators'].str.contains('GROWTH'), 'QW_RANGE_GROSSPROFITMARGIN_GROWTH','QW_RANGE_GROSSPROFITMARGIN_DELTA')
df_category_11 = pd.merge(df_category_11,temp,on=['entity_name','year','季度'],how='left')
df_category_11 = pd.merge(df_category_11,temp2,on=['entity_name','year','季度','judge'],how='left')

df_category_11['is_high_trigger'] = np.where((df_category_11['finnal_value'] <= df_category_11['high_trigger']) & (df_category_11['is_negative'] != True) & (df_category_11['is_change'] != True), True, False)
df_category_11['is_general_trigger'] = np.where((df_category_11['finnal_value'] <= df_category_11['general_trigger']) & (df_category_11['is_negative'] != True)& (df_category_11['is_change'] != True), True, False)
df_category_11.drop(columns = ['is_negative','is_change','judge'],inplace=True)


# 12.毛利率双重判断>
temp2 = df_category_10[['entity_name','year','季度','indicators','is_general_trigger']]
temp2 = temp2.loc[temp2['indicators']=='QW_RANGE_GROSSPROFITMARGIN_DELTA' ].drop(columns = 'indicators')

df_category_12 = pd.merge(df_category_12,temp,on=['entity_name','year','季度'],how='left')
df_category_12 = pd.merge(df_category_12,temp2,on=['entity_name','year','季度'],how='left')

df_category_12['is_high_trigger'] = np.where((df_category_12['finnal_value'] >= df_category_12['high_trigger']) & (df_category_12['is_negative'] != True) & (df_category_12['is_general_trigger'] != True), True, False)
df_category_12['is_general_trigger'] = np.where((df_category_12['finnal_value'] >= df_category_12['general_trigger']) & (df_category_12['is_negative'] != True)& (df_category_12['is_general_trigger'] != True), True, False)
df_category_12.drop(columns = ['is_negative','is_general_trigger'],inplace=True)


#########################################################################
################################ 数据整合 ################################

df_data_threshold = pd.concat([df_category_0, df_category_1, df_category_2, df_category_3, df_category_4, df_category_5, df_category_6,df_category_7,df_category_8,df_category_9, df_category_10, df_category_11, df_category_12])
df_data_threshold['is_trigger'] = np.where(df_data_threshold['is_high_trigger'] == True, 2,
                                  np.where(df_data_threshold['is_general_trigger'] == True, 1,
                                           0))


#########################################################################
###################### 数据缓存（指标异常值处理前） ##########################

df_data_threshold.drop(columns=['is_high_trigger','category','is_general_trigger'],inplace= True)
df_data_threshold['is_trigger_origin']=df_data_threshold['is_trigger']


#######################################################################
############################ 指标异常值处理-数据清洗 #############################

# 指标异常值处理规则字段导入
formu = pd.read_excel(path_input+"触发判断数据字典.xlsx")
# 指标异常值处理判断所需底层科目数据
caiwu= pd.read_csv(input_df1)
# 提取指标异常值处理所需的财务指标
df_data_clean.drop(columns=["code","industry"],inplace=True)
df_data_clean.rename(columns= {
    "entity_name": "BASIC_entity_name",
    "year": "BASIC_year",
    "季度": "BASIC_quarter"
    }, inplace= True)
zhibiao = df_data_clean

lst = ['BASIC_entity_name','BASIC_year','BASIC_quarter','QW_ACCOUNTSRECEIVABLETURNOVER_DELTA','QW_CASH_RATIO_DELTA','QW_GROSSPROFITMARGIN','QW_GROSSPROFITMARGIN_DELTA',	'QW_INTERESTCOVERAGE_DELTA',	'QW_INVENTORYTURNOVER_DELTA',	'QW_OPERATINGCASHFLOW',	'QW_PERFORMANCE_RATIO_DELTA',	'QW_PERIODEXPENSE_DELTA',	'QW_QUICK_RATIO_DELTA',	'QW_RESTRICTEDFUNDS_RATIO_DELTA',	'QW_REVENUE_GROWTH',	'QW_REVENUECASH_RATIO_DELTA',	'QW_ROE_DELTA',	'QW_TOTALASSETTURNOVER_DELTA','QW_RANGE_ACCOUNTSRECEIVABLETURNOVER_DELTA', 'QW_RANGE_GROSSPROFITMARGIN', 'QW_RANGE_INVENTORYTURNOVER_DELTA', 'QW_RANGE_OPERATINGCASHFLOW', 'QW_RANGE_PERIODEXPENSE_DELTA', 'QW_RANGE_REVENUECASH_RATIO_DELTA', 'QW_RANGE_ROE_DELTA']
lst = [variable for variable in lst if variable in list(zhibiao)]


zhibiao =zhibiao[lst]
# 如若上一行报错，则可能是因为以下指标缺失，需要修改代码从上一行中删除缺失的指标：'QW_RANGE_ACCOUNTSRECEIVABLETURNOVER_DELTA', 'QW_RANGE_GROSSPROFITMARGIN', 'QW_RANGE_INVENTORYTURNOVER_DELTA', 'QW_RANGE_OPERATINGCASHFLOW', 'QW_RANGE_PERIODEXPENSE_DELTA', 'QW_RANGE_REVENUECASH_RATIO_DELTA', 'QW_RANGE_ROE_DELTA'


# 筛选财务数据所需的年份
df_greater_2018 = caiwu[caiwu.BASIC_year > year_cmp-3]

# Reorder the dataframe
df2 = df_greater_2018.sort_values(
    by=["BASIC_entity_name", "BASIC_industry", "BASIC_year", "BASIC_quarter", "code", "end_value"])

# Replace duplicate observations by using the observation before
df3 = df2.drop_duplicates(
    subset=["BASIC_entity_name", "BASIC_year", "BASIC_quarter", "code"], keep='first') #保留存在的值

# Long to wide (use indicators as column name)
df4 = df3.pivot(index=["BASIC_entity_name","BASIC_year", "BASIC_quarter"], #以主体划分
                columns="code",
                values="end_value").reset_index()
df5 = df4[["BASIC_entity_name", "BASIC_year", "BASIC_quarter",'BS_0101','BS_0102',	'BS_0104',	'BS_0105',	'BS_0109',	'BS_0191',	'BS_0299',	'BS_0391',	'BS_0591','CF_0105',	'CF_0145',	'PF_0102','PF_0107','PF_0116','PF_0117','PF_0118','PF_0118_1'	,'PF_0129',	'PF_0132',	'stmnote_Eoitems_24']]
# 若有科目缺失，上一行这里会报错，需要修改删去缺失的科目

trig = pd.merge(df5,zhibiao,how='inner',on=["BASIC_entity_name", "BASIC_year", "BASIC_quarter"])
#得到trig：用于指标异常值处理作为判断的数据


#读取之前触发后的数据
df = df_data_threshold[['entity_name','year','季度','code','is_trigger_origin','is_trigger']]
df.columns=["BASIC_entity_name", "BASIC_year", "BASIC_quarter", "项目代码",'is_trigger_origin','is_trigger']
df = df.sort_values(
    by=["BASIC_entity_name", "BASIC_year", "BASIC_quarter", "项目代码"])

formu = formu[['项目代码','指标异常值处理']]
df=pd.merge(df,formu, how='left',on='项目代码')


#######################################################################
############################ 指标异常值处理-判断部分 ############################


tfset = df['项目代码']
spl = df['指标异常值处理'].str.split('or|and|\；',expand = True, n=1) #将规则文字按逻辑切割曾多个层级，分部判断T/F
alist = list(trig.columns)[3:]
p = 0 # 处理的规则字段层级


while spl.empty ==False:
    work = spl[0]
    # 处理work里的字符串
    work = work.str.replace("如果|\【|\】|\，则此项不触发|\，则指标为N/A\，不触发项目|\，则指标为99999\，此项不触发\，后期展示阶段特殊标注","")
    part = work.str.split('\<|\>',expand = True)[0]

    k = 0 # 行数
    for i in part:  # 一行行处理数据
        if (i == i) & (i != None) & (df['is_trigger'][k] != 0):  # 判断是不是float'nan'且触发了，不是就跳过运算，节约时间
            for st in alist:  # 一个个匹配指标替换
                if st in str(i):
                    # 检查是否有lag，last：
                        # 先btlag后lag
                    try:
                        #以下是对指标取过去值的不同处理
                        if 'btlag('+st in str(i):
                            # 索引到trig中的数据，
                            x = trig[st].loc[(trig['BASIC_entity_name'] == df['BASIC_entity_name'][k]) & (
                                        trig['BASIC_year'] == df['BASIC_year'][k]-2) & (
                                                         trig['BASIC_quarter'] == df['BASIC_quarter'][k])]
                            data = x.to_string(index=False)
                            work[k] = work[k].replace('btlag('+st+')', data)
                        if 'lag('+st in str(i):
                            # 索引到trig中的数据，
                            x = trig[st].loc[(trig['BASIC_entity_name'] == df['BASIC_entity_name'][k]) & (
                                        trig['BASIC_year'] == df['BASIC_year'][k]-1) & (
                                                         trig['BASIC_quarter'] == df['BASIC_quarter'][k])]
                            data = x.to_string(index=False)
                            work[k] = work[k].replace('lag('+st+')', data)
                        if 'ylast('+st in str(i):
                            # 索引到trig中的数据，
                            if df['BASIC_quarter'][k]==1:
                                x = trig[st].loc[(trig['BASIC_entity_name'] == df['BASIC_entity_name'][k]) & (
                                            trig['BASIC_year'] == df['BASIC_year'][k]-1) & (
                                                             trig['BASIC_quarter'] == df['BASIC_quarter'][k]+3)]
                            else:
                                x = trig[st].loc[(trig['BASIC_entity_name'] == df['BASIC_entity_name'][k]) & (
                                        trig['BASIC_year'] == df['BASIC_year'][k]) & (
                                                         trig['BASIC_quarter'] == df['BASIC_quarter'][k]-1)]
                            data = x.to_string(index=False)
                            work[k] = work[k].replace('ylast('+st+')', data)
                        if 'yblast('+st in str(i):
                            # 索引到trig中的数据，
                            if df['BASIC_quarter'][k]==1:
                                x = trig[st].loc[(trig['BASIC_entity_name'] == df['BASIC_entity_name'][k]) & (
                                            trig['BASIC_year'] == df['BASIC_year'][k]-2) & (
                                                             trig['BASIC_quarter'] == df['BASIC_quarter'][k]+3)]
                            else:
                                x = trig[st].loc[(trig['BASIC_entity_name'] == df['BASIC_entity_name'][k]) & (
                                        trig['BASIC_year'] == df['BASIC_year'][k]-1) & (
                                                         trig['BASIC_quarter'] == df['BASIC_quarter'][k]-1)]
                            data = x.to_string(index=False)
                            work[k] = work[k].replace('yblast('+st+')', data)
                        if 'last('+st in str(i):
                            # 索引到trig中的数据，
                            if df['BASIC_quarter'][k]==1:
                                x = 0
                            else:
                                x = trig[st].loc[(trig['BASIC_entity_name'] == df['BASIC_entity_name'][k]) & (
                                        trig['BASIC_year'] == df['BASIC_year'][k]) & (
                                                         trig['BASIC_quarter'] == df['BASIC_quarter'][k]-1)]
                            data = x.to_string(index=False)
                            work[k] = work[k].replace('last('+st+')', data)

                        x= trig[st].loc[(trig['BASIC_entity_name']==df['BASIC_entity_name'][k])&(trig['BASIC_year']==df['BASIC_year'][k])&(trig['BASIC_quarter']==df['BASIC_quarter'][k])]
                        data= x.to_string(index = False)
                        work[k]=work[k].replace(st, data)
                    except:
                        work[k] = False
                    #对大于小于进行判断，得到结果Ture/False

            try:  #如果判定所需的指标/财务数据缺失，则不改变结果
                work[k] = eval(work[k])
            except:
                work[k] = False
        else:
            work[k] = False
        k += 1
    p +=1
    work.name = p
    #将每一层级的结果与指标列拼接
    tfset = pd.concat([tfset,work],axis=1)
    try:
        spl = spl[1].str.split('or|and|；',expand = True, n=1)
    except:
        spl = pd.DataFrame()
        print('end')

# 把t，f用1，0替换，求和，大于一则改为不触发

# 特殊处理判断文字中包含'and'逻辑的几个指标
for idx,row in tfset[tfset['项目代码'].str.contains('PP_009_O0|DID_003_O0|OID_001_Q01|OID_002_Q01|OID_002_Q01')].iterrows():
    # print(row['项目代码'].contains('PP_009_O0|DID_003_O0|OID_001_Q01|OID_002_Q01|OID_002_Q01'))
    s = 1
    while s <= p/2:
        if row[(2*s-1)] == True and row[(2*s)] == False:
            tfset.at[idx,(2 * s - 1)] = False
        if row[(2 * s - 1)] == False and row[(2 * s)] == True:
            tfset.at[idx,(2 * s )] = False
        s += 1
tfset.drop(columns='项目代码',inplace=True)

# 对先前拆分的判断规则进行整合，总判断
tfset.replace(True, int(1), inplace=True)
tfset.replace(False, int(0), inplace=True)

result = tfset.apply(np.sum, axis=1)

df = pd.concat([df, result], axis=1)
df[0] = np.where(df[0] >= 1, True, False)
df.loc[df[0] == True, ['is_trigger']] = 0


#######################################################################
############################ 最终结果整合与导出 ############################
df = pd.merge(df,
              df_data_comb,
              how='inner',
              left_on=["BASIC_entity_name", "BASIC_year", "BASIC_quarter", "项目代码"],
              right_on=["entity_name", "year", "季度", "code"])

df = df.sort_values(by=["BASIC_entity_name", "BASIC_year", "BASIC_quarter", "项目代码"])
df = df[['entity_name','stock_code','industry_whitewash','year','季度','indicators','code','finnal_value','general_trigger','high_trigger','is_trigger_origin','指标异常值处理',0,'is_trigger']]
df.columns = ['entity_name','stock_code','industry','year','quarter','indicators','code','finnal_value','general_trigger','high_trigger','is_trigger_origin','指标异常值处理','指标异常值处理结果','is_trigger']

df.to_excel(output_df, index=False)
