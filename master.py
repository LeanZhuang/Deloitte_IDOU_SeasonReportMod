import os
import time
from datetime import date

#根目录名称与路径在此修改
# os.chdir('')
path_dfs = r"dfs/"  # 在此更改数据输入文件夹
path_input = r"input/"  # 在此更改字典输入文件夹

# 数据文件名称设置：
subject_file_name = '原始科目数据_20221111.csv'
indicator_file_name = '指标计算结果_20221111.csv'
trigger_file_name = '阈值计算输出_20221229.csv'
result_file_name= '触发项目及触发等级_20221111.xlsx'

# 指标计算起始及结束时间设置：时间跨度需为三年
indicator_cmp_start_year = 2019
indicator_cmp_start_quarter = 1
indicator_cmp_end_year = 2022
indicator_cmp_end_quarter = 3

# 阈值触发时间设置：
result_cmp_year = 2022
result_cmp_quarter = 3

###############################################################
######################### 计算指标 ##############################
start = time.perf_counter()

#参数分别为：输入数据文件名，输出数据文件名，指标计算起始年份，指标计算起始季度，指标计算结束年份，指标计算结束季度。如 '2017'，'4' ,'2021','3' 计算的是2017年4季度（包含）至2021.3季度（包含）的指标
os.system("python3 -u 1_指标值计算.py {} {} {} {} {} {}".format(path_dfs+subject_file_name,path_dfs+indicator_file_name,indicator_cmp_start_year,indicator_cmp_start_quarter,indicator_cmp_end_year,indicator_cmp_end_quarter))

end = time.perf_counter()
runtime = end-start
print('first program finished')
print("运行时间：",runtime,"秒")

###############################################################
######################### 计算指标 ##############################
#  (该步骤在更新季度触发结果时无需进行，跳过即可）
start = time.perf_counter()

os.system("python3 -u 2_阈值计算.py {} {}".format(path_dfs+indicator_file_name,path_dfs+trigger_file_name))

print('second program finished')
end = time.perf_counter()
runtime = end-start
print("运行时间：",runtime,"秒")

###############################################################
######################### 阈值触发判断 ###########################
start = time.perf_counter()

#年份和季度参数为需要进行触发计算的年份和季度
os.system("python3 -u 3_触发判断.py {} {} {} {} {} {}".format(path_dfs+subject_file_name,path_dfs+indicator_file_name,path_dfs+trigger_file_name,path_dfs+result_file_name,result_cmp_year,result_cmp_quarter))

print('third program finished')
end = time.perf_counter()
runtime = end-start
print("运行时间：",runtime,"秒")