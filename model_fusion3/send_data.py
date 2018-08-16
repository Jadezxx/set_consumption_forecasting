import pandas as pd
import json
from urllib import parse,request

def send(data):
	length = len(data)
	data = data.reindex()
	data = data.T
	d = []
	for i in range(length):
		d.append(eval ("(" + data[i].to_json() + ")")) #
	textmod = json.dumps(d).encode(encoding='utf-8')
	header_dict = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',"Content-Type": "application/json"}
	url='http://10.13.49.140:8080/setAllProcessedData'
	req=request.Request(url=url,data=textmod,headers=header_dict)
	res=request.urlopen(req)
	print(res)

