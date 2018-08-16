'''
模型融合

数据：设备台账（日期）数据、站场运营（站场巡检）数据、站场运营设备（设备巡检）数据、站场运行（设备告警）数据、站场运行（站点气损）数据
输入：Datafram格式
输出：所有站点的各类分数score_fram
        设备是否过期 + 设备安装时间 + 设备年检情况 = 台账分数
        站场巡检状态 + 站场巡检次数 + 站场巡检时间 + 设备巡检状态 + 设备巡检次数 = 运营分数
        站场告警分数 + 站场气损分数 = 运行分数
'''

import pandas as pd
import numpy as np
import datetime
import time
import urllib.request
import json
from equipdate import equip_data_score, equip_data_score_calculation
from check import station_check_score
from send_data import send
from alerts_loss import alert_score, gasloss_score_cal
from urllib import parse,request
from functools import partial


'''
数据初始化

设备类别权重：
设备权重：

'''

### 数据
global equip_data         # 设备台账（日期）数据
global station_check      # 站场运营（站场巡检）数据
global equip_check        # 站场运营（设备巡检）数据
global tank_alerts        # 站场运行（站点告警）数据
global gasloss_data       # 站场运行（站点气损）数据

### 变量
global now_date           # 当前日期
global expire_deadline    # 设备最多过期多少天 则判为零分
global check_deadline     # 设备最多超年检多少天 则判为零分
global check_arrange_day  # 检查的时间范围
global weights_check_a    # 巡检状态2的权重
global weights_check_b    # 巡检状态3的权重
global standard_station_num     # 标准巡检次数
global standard_station_time    # 标准巡检时长  单位：min
global equip_num          # 每个站点设备数量
global weights_equip      # 每个站点设备权重
global standard_equip_num # 标准的设备巡检次数
global alertsType_weight  # 告警类型权重
global illegal_score      # 当数据非法时算的分数 比如日期都一样的 或者缺失数据 都算非法
global alert_1            # 告警类型1 alarm 时的扣分
global alert_2            # 告警类型2 critical 时的扣分
global alert_3            # 告警类型3 major 时的扣分
global standard_gasloss   # 正常气损 气损值小于该值的算100分 
global major_gasloss      # 允许最大气损 气损值大于该值的算0分

standard_gasloss = 10
major_gasloss = 100
alert_1 = 1
alert_2 = 10
alert_3 = 100
# alertsType_weight = ？？？
illegal_score = 0  # 暂时设为0分
standard_equip_num = [3 for i in range(11)]
weights_equip = [0,0,0,0.1,0.2,0.1,0.1,0.1,0.1,0.1,0.1]
equip_num = 11
weights_check_a = 0.8
weights_check_b = 0.2
now_date = datetime.date.today()
check_arrange_day = 365
expire_deadline = 30
check_deadline = 30
standard_station_time = 10
standard_station_num = 5

# 各级分数权重
W1,W2,W3 = 0.2,0.5,0.3
W11,W12,W13 = 0.33,0.33,0.33
W21,W22,W23,W24,W25 = 0.2,0.2,0.2,0.2,0.2
W31,W32 = 0.5,0.5

'''
预定义数据来源地址
'''


## 设备台账（日期）数据
filename_equip_data = 'D:/document/gas_healthy/data/site_ledgers_20180710.xlsx'
## 站场运营（站场巡检）数据
filename_station_check = 'D:/document/gas_healthy/data/tank_inspects_20180710.xlsx'
## 站场运营（设备巡检）数据
filename_equip_check = 'D:/document/gas_healthy/data/inspect_items_20180714.xlsx'
## 站场运行（站点告警）数据
filename_tank_alerts = 'D:/document/gas_healthy/data/tank_alerts_20180710.xlsx'
## 站场运行（站点气损）数据
filename_gasloss = 'D:/document/gas_healthy/data/rpt_daily_site.xlsx'
print(1)


'''
获得数据

设备台账（日期）数据：equip_data
站场运营（站场巡检）数据：station_check
站场运营（设备巡检）数据：equip_check
站场运行（告警气损）数据：tank_alerts,gasloss_data
'''


equip_data = pd.read_excel(filename_equip_data)
equip_data = equip_data[equip_data['state'] == 1]
station_check = pd.read_excel(filename_station_check)
print(2)
equip_check = pd.read_excel(filename_equip_check)
tank_alerts = pd.read_excel(filename_tank_alerts)
print(3)
gasloss_data = pd.read_excel(filename_gasloss)
print(4)



### 从服务器中读取数据
response1 = urllib.request.urlopen('http://10.13.49.140:8080/getAllSiteLedgers')
response2 = urllib.request.urlopen('http://10.13.49.140:8080/getAllTankInspects')
response3 = urllib.request.urlopen('http://10.13.49.140:8080/getAllInspectItems')
response4 = urllib.request.urlopen('http://10.13.49.140:8080/getAllTankAlerts')
response5 = urllib.request.urlopen('http://10.13.49.140:8080/getAllRptDailySite')
html1 = response1.read().decode('utf-8')
html2 = response2.read().decode('utf-8')
html3 = response3.read().decode('utf-8')
html4 = response4.read().decode('utf-8')
html5 = response5.read().decode('utf-8')
data1 = pd.DataFrame(json.loads(html1))
data2 = pd.DataFrame(json.loads(html2))
data3 = pd.DataFrame(json.loads(html3))
data4 = pd.DataFrame(json.loads(html4))
data5 = pd.DataFrame(json.loads(html5))

print(1)
### 修改一些读取过来的数据列名
equip_data = data1.rename(columns={'siteId':'site_id','productDate':'product_date','installDate':'install_date',
                            'expireDate':'expire_date','inspectDate':'inspect_date','deviceId':'device_id','equiptype':'equipType'})
station_check = data2.rename(columns={'statDate':'stat_date','arriveTime':'arrive_time','leaveTime':'leave_time',
                                     'startTime':'start_time', 'endTime':'end_time'})
equip_check = data3
tank_alerts = data4.rename(columns={'deviceId':'device_id','startTime':'start_time','stopTime':'stop_time'})
gasloss_data = data5.rename(columns={'statDate':'stat_date','siteId':'site_id',
                                    'lljused':'lljUsed'})
print(2)

### 将读取过来数据的时间格式统一一下
import pandas as pd
from functools import partial
to_datetime_fmt = partial(pd.to_datetime, format='%Y-%m-%d')
equip_data['product_date'] = equip_data['product_date'].apply(to_datetime_fmt)
equip_data['expire_date'] = equip_data['expire_date'].apply(to_datetime_fmt)
equip_data['inspect_date'] = equip_data['inspect_date'].apply(to_datetime_fmt)
equip_data['install_date'] = equip_data['install_date'].apply(to_datetime_fmt)
station_check['arrive_time'] = station_check['arrive_time'].apply(to_datetime_fmt)
station_check['leave_time'] = station_check['leave_time'].apply(to_datetime_fmt)
station_check['start_time'] = station_check['start_time'].apply(to_datetime_fmt)
station_check['end_time'] = station_check['end_time'].apply(to_datetime_fmt)
station_check['stat_date'] = station_check['stat_date'].apply(to_datetime_fmt)
tank_alerts['start_time'] = tank_alerts['start_time'].apply(to_datetime_fmt)
tank_alerts['stop_time'] = tank_alerts['stop_time'].apply(to_datetime_fmt)
gasloss_data['stat_date'] = gasloss_data['stat_date'].apply(to_datetime_fmt)


### 输入：所有站点名称id
### 输出：所有站点的各类分数

def score_cal(station_id):

    # 预定义最终分数的格式
    Score_fram = pd.DataFrame(index=[i for i in range(len(station_id))])
    station_expire_score = np.zeros(len(station_id))    #是否过期分数
    station_set_score = np.zeros(len(station_id))       #安装时间分数
    station_yearcheck_score = np.zeros(len(station_id)) #年检情况分数
    station_state_score = np.zeros(len(station_id))     #站点巡检状态分数
    station_time_score = np.zeros(len(station_id))      #站点巡检时间分数
    station_num_score = np.zeros(len(station_id))       #站点巡检次数分数
    equip_score = np.zeros(len(station_id))             #设备巡检状态分数
    equip_total_num_score = np.zeros(len(station_id))   #设备巡检次数分数
    station_alerts_score = np.zeros(len(station_id))    #站点设备告警分数
    station_gasloss_score = np.zeros(len(station_id))   #站点气损分数

    # 对所有站点id号进行循环，调用各模块函数，求出各站点的各类分数
    for i in range(len(station_id)):
        equip_data_fusion = equip_data_score(station_id[i], equip_data,now_date,check_deadline,illegal_score,expire_deadline)
        station_expire_score[i],station_set_score[i],station_yearcheck_score[i] = equip_data_score_calculation(equip_data_fusion, expire_deadline,check_deadline,illegal_score)
        station_state_score[i],station_time_score[i],station_num_score[i],equip_score[i],equip_total_num_score[i] = station_check_score(station_id[i], equip_num,station_check,now_date,check_arrange_day,standard_station_num,equip_check,standard_equip_num,weights_equip,standard_station_time,weights_check_a,weights_check_b)
        station_alerts_score[i] = alert_score(station_id[i],equip_data,tank_alerts,now_date,check_arrange_day,alert_1,alert_2,alert_3)
        station_gasloss_score[i] = gasloss_score_cal(station_id[i], gasloss_data, standard_gasloss, major_gasloss, now_date, check_arrange_day)
    
    # 将求出的各类分数存放于DataFram表格中
    Score_fram['siteId'] = station_id
    Score_fram['expireScore'] = station_expire_score
    Score_fram['setScore'] = station_set_score
    Score_fram['yearcheckScore'] = station_yearcheck_score
    Score_fram['stationStateScore'] = station_state_score
    Score_fram['stationTimeScore'] = station_time_score
    Score_fram['stationNumScore'] = station_num_score
    Score_fram['equipStateScore'] = equip_score
    Score_fram['equipNumScore'] = equip_total_num_score
    Score_fram['stationAlertsScore'] = station_alerts_score
    Score_fram['stationGaslossScore'] = station_gasloss_score

    # 一级分数 台账+运行+运营     其中二级分数的权重都一样
    Score_fram['taizhangScore'] = Score_fram['expireScore']*W11 + Score_fram['setScore']*W12 + Score_fram['yearcheckScore']*W13
    Score_fram['yunyingScore'] = Score_fram['stationStateScore']*W21 + Score_fram['stationTimeScore']*W22 + Score_fram['stationNumScore']*W23 + Score_fram['equipStateScore']*W24 + Score_fram['equipNumScore']*W25
    Score_fram['yunxingScore'] = Score_fram['stationAlertsScore']*W31 + Score_fram['stationGaslossScore']*W32

    # 总分数       总分数的权重分别为0.2，0.5，0.3
    Score_fram['totalScore'] = Score_fram['taizhangScore']*W1 + Score_fram['yunyingScore']*W2 + Score_fram['yunxingScore']*W3
    return Score_fram

if __name__ == "__main__":
    ### 得到所有站点编号 共108个站点
    station_id = equip_data['site_id'].unique() 

    ### 将分数的数值保留小数点后两位
    score = score_cal(station_id).round(2)

    ### 保存本地 真实模型时去掉
    #score.to_csv('D:/document/gas_healthy/data/score_test.csv')
    ### 向服务器发送数据
    send(score)


