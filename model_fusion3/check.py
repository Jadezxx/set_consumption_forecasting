'''
对某个站点的 运营 进行健康评估
输入：站点名称site_id
输出：站场巡检状态分数 check_state_score + 站场巡检次数分数 check_station_num_score 
    + 站场巡检时间分数 check_station_time_score + 所有设备巡检状态分数 equip_state_score
    + 总设备巡检状态分数 equip_score + 所有设备巡检次数分数 equip_num_score
    + 总设备巡检次数分数 equip_total_num_score
'''

import pandas as pd  
import numpy as np  
import datetime

# 输入：站点名称site_id
# 输出：站场巡检状态分数 station_state_score + 站场巡检时间分数 station_time_score + 站场巡检次数分数 station_checknum_score + 
#       设备巡检状态总分数 equip_score + 设备巡检次数总分数 equip_total_num_score
# 可扩展的点 ： 如果没有巡检 可以用备注的方式显示出来

def station_check_score(site_id,equip_num,station_check,now_date,check_arrange_day,standard_station_num,equip_check,standard_equip_num,weights_equip,standard_station_time,weights_check_a,weights_check_b):
    
    ### 预定义站点的巡检信息
    N_station = np.zeros((5))  # [状态1次数，状态2次数，状态3次数，巡检总次数，时间合格次数]

    ### 预定义设备的巡检信息
    N_equip = np.zeros((equip_num,4))  # [状态1次数，状态2次数，状态3次数，巡检总次数]
    
    ### 筛选对应站点的对应时间内范围数据
    data_station_site = station_check[station_check['site'] == site_id]
    end_date = str(now_date)
    start_date = str(now_date - datetime.timedelta(days = check_arrange_day))
    equip_check_data = data_station_site[(data_station_site['stat_date'] < end_date) 
                                    & (start_date < data_station_site['stat_date'])]
    
    ### 得到站点的巡检信息
    station_check_time = (pd.to_datetime(equip_check_data['end_time']) - pd.to_datetime(equip_check_data['start_time'])).dropna().map(lambda x: x/np.timedelta64(1, 'm')).values
    N_station[3] = len(station_check_time)
    for i in range(len(station_check_time)):
        if station_check_time[i] >= standard_station_time:
            N_station[4] += 1
    all_state = equip_check_data['state'].values  # 得到该站点规定时间内每次巡检的状态
    
    for j in range(len(all_state)):
        if all_state[j] == 1:
            N_station[0] += 1
        elif all_state[j] == 2:
            N_station[1] += 1
        elif all_state[j] ==3:
            N_station[2] += 1

    ### 计算各站点巡检分数
    if N_station[3] == 0:

        ## 设置一个判断没有巡检次数的模型输出代码
        station_state_score = 0
        station_time_score = 0
        station_num_score = 0
    else:
        station_state_score = (((N_station[0]+N_station[1]+N_station[2])-weights_check_a*N_station[1]-weights_check_b*N_station[2])/(N_station[3]))*100
        station_time_score = (N_station[4]/N_station[3])*100
        station_num_score = (min(N_station[3],standard_station_num)/standard_station_num)*100
    
    ###  对站点设备进行操作
    check_id = equip_check_data['id'].values # 给定站点在规定时间内的巡检id号
    
    # 对具体每次巡检中的设备检查状态进行操作
    for k in range(len(check_id)):
        check_id_data = equip_check[equip_check['inspect'] == check_id[k]]
        check_id_data_equip_id = check_id_data['id'].values # 每次巡检的设备的id号
        for m in range(len(check_id_data_equip_id)):
            check_id_data_row = check_id_data[check_id_data['id'] == check_id_data_equip_id[m]]
            if ((int(check_id_data_row['state']) == 1) and int((check_id_data_row['fixed'] == 0))) or ((int(check_id_data_row['state']) == 1) and (int(check_id_data_row['fixed'] == 1))): 
                ###状态1 巡检正常
                N_equip[int(check_id_data_row['item']-1)][0] += 1
                N_equip[int(check_id_data_row['item']-1)][3] += 1
            elif (int(check_id_data_row['state'] == 0)) and (int(check_id_data_row['fixed'] == 1)):  # 状态3 巡检异常但已修复
                N_equip[int(check_id_data_row['item']-1)][1] += 1
                N_equip[int(check_id_data_row['item']-1)][3] += 1
            elif (int(check_id_data_row['state'] == 0)) and (int(check_id_data_row['fixed'] == 0)):  # 状态2 巡检异常但未修复
                N_equip[int(check_id_data_row['item']-1)][2] += 1
                N_equip[int(check_id_data_row['item']-1)][3] += 1
    
    equip_state_score = np.nan_to_num((((N_equip[:,0]+N_equip[:,1]+N_equip[:,2])-weights_check_a*N_equip[:,1]-weights_check_b*N_equip[:,2])/(N_equip[:,3]))*100)
    equip_score = np.nan_to_num((equip_state_score*weights_equip).sum())
    for i in range(len(N_equip)):
        N_equip[i,3] = min(N_equip[i,3], standard_equip_num[i])
    equip_num_score = (N_equip[:,3]/standard_equip_num)*100
    #print(equip_num_score)
    equip_total_num_score = (equip_num_score*weights_equip).sum()
    #print(equip_total_num_score)
    return station_state_score,station_time_score,station_num_score,equip_score,equip_total_num_score