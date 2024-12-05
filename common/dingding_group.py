###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: liuh
"""

import sys
import os
import traceback
import time
import urllib.request
import json

def send_dingding_group(req_url, content):
    # req_url = "https://oapi.dingtalk.com/robot/send?access_token=afa0ef45f99d106abda74cf64c65b06dddedfcdb56510abf5621432ad8252aac" # 群机器人
    if not req_url.strip().startswith("https://oapi.dingtalk.com"):
        return
    
    body =  {
	    "at":{
		    "isAtAll":"false",
		    "atUserIds":["","user002"],
		    "atMobiles":["15xxx","xx"]
            },
	    "text":{
		    "content":f"Databaas监控报警\n{content}"
	        },
	    "msgtype":"text"
        }

    # 将字典转换为 JSON 格式
    data_json = json.dumps(body).encode('utf-8')
    # 创建一个请求对象，设置请求类型为 POST，并添加 Content-Type 头
    request = urllib.request.Request(req_url, data=data_json, method='POST')
    request.add_header('Content-Type', 'application/json')
    # 发送请求并读取响应
    response = urllib.request.urlopen(request, timeout=3).read()
    response_json = json.loads(response.decode('utf-8'))
    print("dinding body: %s, response: %s" % (data_json, response.decode('utf-8')))
    if response_json["errcode"] != 200 and response_json["errcode"] != 0:
        print(response_json)
        return
    
if __name__ == "__main__":
    content = "错误告警"  #需要带上机器配置的字段，否则无法推送消息，errcode = 310000
    send_dingding_group("https://oapi.dingtalk.com/robot/send?access_token=afa0ef45f99d106abda74cf64c65b06dddedfcdb56510abf5621432ad8252aac", content)