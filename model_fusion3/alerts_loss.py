'''
对某个站点的 运行 进行健康评估
输入：站点名称site_id
输出：该站点所有设备的告警分数 score_dict 以及该站点的告警总分数 score_total
'''

import numpy as np 
import datetime

# 输入：站点名称site_id
# 输出：该站点的告警总分数score_total
# 可扩展的点 ： 设备对应告警分数形成的告警字典

def alert_score(site_id,equip_data,tank_alerts,now_date,check_arrange_day,alert_1,alert_2,alert_3):
    
    score_dict = {}     # 设备对应告警分数形成的告警字典
    score_arr = []      # 设备告警分数
    
    ### 设备集合中，有可能设备没有id号，是NAN值，这时默认是一个设备，要加入分数计算，但无需计算权重
    device_data = equip_data[equip_data['site_id'] == site_id]['device_id'].unique()

    ### 对每个设备进行告警分数计算
    for i in range(len(device_data)):
        alerts_data = tank_alerts[tank_alerts['device_id'] == device_data[i]]

        ## 筛选时间段内的告警数据
        end_date = str(now_date)
        start_date = str(now_date - datetime.timedelta(days = check_arrange_day))
        alerts_range_data = alerts_data[(alerts_data['start_time'] < end_date) 
                                    & (start_date < alerts_data['start_time'])]

        ## 统计告警类型次数
        level_id = alerts_range_data['level'].values
        n1,n2,n3 = 0,0,0
        for j in level_id:
            if j == 'alarm':
                n1 += 1
            elif j == 'critical':
                n2 += 1
            elif j == 'major':
                n3 += 1

        ## 计算告警分数
        score_dict[str(device_data[i])] = (max(0,100 - alert_1*n1 - alert_2*n2 - alert_3*n3))
        score_arr.append(max(0,100 - alert_1*n1 - alert_2*n2 - alert_3*n3))
    score_total = np.array(score_arr).sum()/(len(score_arr))
    return score_total


# 输入：站点名称site_id
# 输出：该站点的气损总分数score
# 可扩展的点 ： 设备对应告警分数形成的告警字典

def gasloss_score_cal(site_id, gasloss_data, standard_gasloss, major_gasloss, now_date, check_arrange_day):

    ### 筛选出对应站点的数据
    gasloss_station = gasloss_data[gasloss_data['site_id'] == site_id]

    ### 筛选出时间段内的气损数据
    end_date = str(now_date)
    start_date = str(now_date - datetime.timedelta(days = check_arrange_day))
    gasloss_range_station = gasloss_station[(gasloss_station['stat_date'] < end_date) 
                                & (start_date < gasloss_station['stat_date'])]
    gasloss_range_station = gasloss_range_station.sort_values(by='stat_date')
    
    ### 得出计算气损需要数值
    gasloss_values = np.zeros((3,len(gasloss_range_station)))
    gasloss_values[0] = gasloss_range_station['weight'].values
    gasloss_values[1] = gasloss_range_station['lljUsed'].values
    gasloss_values[2] = gasloss_range_station['filling'].values
    gasloss_values = np.nan_to_num(gasloss_values)
    
    ### 计算气损
    gasloss = []
    gasloss_score = []
    for i in range((len(gasloss_range_station)-1)):
        gasloss.append(gasloss_values[0,i] + gasloss_values[2,i+1] 
                       - gasloss_values[0,i+1] - (gasloss_values[1,i+1]/1.43))
    illegal_gasloss = 0
    norm_gasloss = 0
    alert_gasloss = 0

    ### 计算气损分数
    for i in range(len(gasloss)):
        if gasloss[i] < 0:
            gasloss_score.append(60)
        elif 0 <= gasloss[i] < standard_gasloss:
            gasloss_score.append(100)
        elif standard_gasloss <= gasloss[i] < major_gasloss:
            gasloss_score.append(((major_gasloss - gasloss[i])/(major_gasloss - standard_gasloss))*100)
        else:
            gasloss_score.append(0)
    score = np.array(gasloss_score).sum()/len(gasloss_score)
    score = np.nan_to_num(score)
    return score