### 合并22年原始数据与提供的补充数据

import pandas as pd

# 导入原始科目数据和缺失科目补充数据
df1 = pd.read_csv('/整合季报代码模型/dfs/原始科目数据_20221111.csv')
df2 = pd.read_csv('/整合季报代码模型/dfs/2019-2022Q3缺失科目_20230103.csv')

# 补充数据重命名
df2.rename(columns={'entity_name': 'BASIC_entity_name',
                    'stock_code': 'BASIC_code',
                    'industry_whitewash': 'BASIC_industry',
                    'YEAR': 'BASIC_year',
                    'quarterly_type': 'BASIC_quarter'},
           inplace=True)

# 合并数据
df = pd.concat([df1, df2])
df.drop('add_time', axis=1)
df.drop('old_value', axis=1)

df.to_csv('/Users/zhuangyuhao/PycharmProjects/SeasonRepMod/整合季报代码模型/dfs/原始科目数据_20230104.csv')