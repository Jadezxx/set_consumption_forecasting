'''
对某个站点的 台账 进行健康度评估
输入：站点名称site_id、站点的台账日期数据
输出：设备是否过期 expire_score + 设备安装时间 set_score + 设备年检情况 check_score
'''

import pandas as pd   
import numpy as np   
import datetime

# 输入：站点名称site_id
# 输出：设备各类日期的融合表格 equip_data_fusion

def equip_data_score(site_id,equip_data,now_date,check_deadline,illegal_score,expire_deadline):
    equip_data_fusion = pd.DataFrame()

    ### 计算设备是否过期需要的数据
    equip_data_fusion['equipType'] = equip_data[equip_data['site_id'] == site_id]['equipType']          # 设备类型
    equip_data_fusion['device_id'] = equip_data[equip_data['site_id'] == site_id]['device_id']          # 设备ID
    equip_data_fusion['product_date'] = equip_data[equip_data['site_id'] == site_id]['product_date']    # 设备生产日期
    equip_data_fusion['expire_date'] = equip_data[equip_data['site_id'] == site_id]['expire_date']      # 设备过期日期
    equip_data_fusion['now_date'] = pd.Series(data=[now_date for i in range(len(equip_data_fusion))],index=equip_data_fusion.index) # 当前日期
    equip_data_fusion['deadline_date_expire'] = equip_data_fusion['expire_date'] + datetime.timedelta(days = expire_deadline)       # 允许超过过期日期的日期
    
    # to_end_expire ： 现在距离允许超过过期的日期的时间
    # to_expire ： 现在距离过期日期的时间
    # validity ： 生产日期与销售日期的据里 有效时长
    equip_data_fusion['to_end_expire'] = (pd.to_datetime(equip_data_fusion['deadline_date_expire']) - pd.to_datetime(equip_data_fusion['now_date'])).dropna().map(lambda x: x/np.timedelta64(1, 'D'))
    equip_data_fusion['to_expire'] = (pd.to_datetime(equip_data_fusion['expire_date']) - pd.to_datetime(equip_data_fusion['now_date'])).dropna().map(lambda x: x/np.timedelta64(1, 'D'))
    equip_data_fusion['validity'] = (pd.to_datetime(equip_data_fusion['expire_date']) - pd.to_datetime(equip_data_fusion['product_date'])).dropna().map(lambda x: x/np.timedelta64(1, 'D'))
    
    ### 计算设备安装时间需要的数据
    # install_date ： 安装时间
    # using ： 剩余使用时长
    equip_data_fusion['install_date'] = equip_data[equip_data['site_id'] == site_id]['install_date']
    equip_data_fusion['using'] = (pd.to_datetime(equip_data_fusion['expire_date']) - pd.to_datetime(equip_data_fusion['install_date'])).dropna().map(lambda x: x/np.timedelta64(1, 'D'))

    ### 计算设备年检情况需要的数据
    
    # inspect_date ： 年检日期
    # check_date ： 下一次年检日期
    # deadline_date_check ： 最大允许超过年检时间的日期
    # 距最大允许超过年检时间的日期 剩余的时间
    # 距下一次检查剩余的时间
    equip_data_fusion['inspect_date'] = equip_data[equip_data['site_id'] == site_id]['inspect_date']
    equip_data_fusion['check_date'] = equip_data_fusion['inspect_date'] + datetime.timedelta(days = 365)
    equip_data_fusion['deadline_date_check'] = equip_data_fusion['check_date'] + datetime.timedelta(days = check_deadline)
    equip_data_fusion['to_end_check'] = (pd.to_datetime(equip_data_fusion['deadline_date_check']) - pd.to_datetime(equip_data_fusion['now_date'])).dropna().map(lambda x: x/np.timedelta64(1, 'D'))
    equip_data_fusion['to_check'] = (pd.to_datetime(equip_data_fusion['check_date']) - pd.to_datetime(equip_data_fusion['now_date'])).dropna().map(lambda x: x/np.timedelta64(1, 'D'))
    return equip_data_fusion


# 输入：设备各类日期的融合表格 equip_data_fusion
# 输出：设备是否过期 expire_score + 设备安装时间 set_score + 设备年检情况 check_score + 是否过期站点总分 score_expire + 安装时间站点总分 score_set + 年检情况站点总分 score_check

def equip_data_score_calculation(equip_data_fusion,expire_deadline,check_deadline,illegal_score):
    
    ### 预定义一些变量 
    illegal_expire,illegal_set,illegal_check = 0,0,0
    index_equip_data_fusion = equip_data_fusion.index

    ### 
    to_end_expire = equip_data_fusion['to_end_expire'].values
    to_end_check = equip_data_fusion['to_end_check'].values
    validity_day = equip_data_fusion['validity'].values
    using_day = equip_data_fusion['using'].values
    
    expire_score = np.zeros((len(to_end_expire)))
    set_score = np.zeros((len(to_end_expire)))
    check_score = np.zeros((len(to_end_expire)))
    
    for i in range(len(index_equip_data_fusion)):
        for j in range(3):
            if j == 0:
                if to_end_expire[i] <= 0:
                    expire_score[i] = 0
                    set_score[i] = 0
                elif 0 < to_end_expire[i] <= expire_deadline:
                    expire_score[i] = (to_end_expire[i]/expire_deadline)*60
                    set_score[i] = expire_score[i]
                elif expire_deadline < to_end_expire[i] <= validity_day[i] + expire_deadline:
                    expire_score[i] = 60 + ((to_end_expire[i]-expire_deadline)/validity_day[i])*40
                else:
                    expire_score[i] = illegal_score  ## dosen't exit
                    illegal_expire += 1
            if j == 1:
                if expire_deadline < to_end_expire[i] <= using_day[i] + expire_deadline:
                    set_score[i] = 60 + ((to_end_expire[i]-expire_deadline)/using_day[i])*40
                elif using_day[i] + expire_deadline < to_end_expire[i]:
                    set_score[i] = illegal_score  ## dosen't exit
                    illegal_set += 1
            if j == 2:
                if to_end_check[i] <= 0:
                    check_score[i] = 0
                elif 0 < to_end_check[i] <= check_deadline:
                    check_score[i] = (to_end_check[i]/check_deadline)*60
                elif check_deadline < to_end_check[i] <= 365 + check_deadline:
                    check_score[i] = 60 + ((to_end_check[i]-check_deadline)/365)*40
                else:
                    check_score[i] = illegal_score  ## dosen't exit
                    illegal_check += 1
    score_expire = (expire_score.sum())/(len(index_equip_data_fusion) - illegal_expire)
    score_expire = np.nan_to_num(score_expire)
    score_set = (set_score.sum())/(len(index_equip_data_fusion) - illegal_set)
    score_set = np.nan_to_num(score_set)
    score_check = (check_score.sum())/(len(index_equip_data_fusion) - illegal_check)
    score_check = np.nan_to_num(score_check)
    return (score_expire,score_set,score_check)