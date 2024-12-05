###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import sys
import traceback
import urllib.request
import logging
import json
import threading
import time
from kafka import KafkaConsumer
from kafka import KafkaProducer
import tornado.httpserver
import tornado.web
import tornado.ioloop
import horae.models
import task_controled_by_user
import django.db

sys.path.append('../common')
import task_util
import admin_sql_manager
    
global_queue_lock = threading.Lock()
class KafkaRequestManager(threading.Thread):
    """
        1.管理长等待的http请求
        2.多线程执行http请求，并保存执行结果
    """
    def __init__(self, config, horae_logger):
        if not hasattr(self, "a"):
            threading.Thread.__init__(self)
            self.__log = horae_logger
            self.__kafka_servers = config.get("admin", "kafka_servers")
            if self.__kafka_servers is None or self.__kafka_servers.strip() == "":
                self.__kafka_servers = ""

            self.__msg_map = {}
            self.a = "a"  # 必须在后面

    def get_data(self, msg_id):
        global_queue_lock.acquire()
        try:
            if msg_id in self.__msg_map:
                data = self.__msg_map[msg_id]
                del self.__msg_map[msg_id]
                return data
        except:
            pass
        finally:
            global_queue_lock.release()
            
        return None

    def run(self):
        if self.__kafka_servers == "":
            return
        
        self.__log.info("KafkaRequestManager thread starting...")
        # 全局退出标示字段
        consumer = KafkaConsumer("admin_all_message",
                         bootstrap_servers=self.__kafka_servers,
                         auto_offset_reset='latest')
        while True:
            try:
                data = consumer.poll(timeout_ms=100, max_records=1)  # 拉取消息，字典类型
                if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                    self.__log.info("not locked and get data failed")
                    continue

                if data:
                    self.__log.info("success locked and get data success")
                    for key in data:
                        global_queue_lock.acquire()
                        try:
                            res_map = json.loads(data[key][0].value)
                            self.__msg_map[res_map["msg_id"]] = res_map["data"]
                            self.__log.info("get data %d: %s" % (res_map["msg_id"], res_map["data"]))
                        except:
                            pass
                        finally:
                            global_queue_lock.release()
            except:
                pass

        self.__log.info("HttpRequestHandler thread exited!")

class AdminHttpServer(object):
    global_access_token = ""
    '''
        admin的http服务
    '''
    def __init__(self, config, task_dispacher):
        self.__config = config
        self.__admin_http_port = config.get("admin", "admin_http_port")
        self.__node_http_port = config.get("admin", "node_http_port")
        self.__task_dispacher = task_dispacher
        self.__kafka_manager = None
        try:
            self.__kafka_servers = config.get("admin", "kafka_servers")
            producer = KafkaProducer(bootstrap_servers=self.__kafka_servers, api_version=(0, 10, 1), value_serializer=lambda m: json.dumps(m).encode())
            data = {}
            data["init"] = True
            producer.send("admin_all_message", data)
            self.__kafka_manager = KafkaRequestManager(config, logging)
            print("__kafka_manager start.")
            logging.info("__kafka_manager start.")
            self.__kafka_manager.start()
        except:
            print("not support kafka")
        
        self.__http_param = HttpHandlerParams(
            config,
            self.__node_http_port, 
            self.__task_dispacher,
            self.__kafka_manager)

            
    def start(self):
        self.__app = tornado.web.Application(handlers=[
                (r"/stop_task", StopTaskHandler),
                (r"/stop_pipeline", StopPipelineHandler),
                (r"/restart_task", RestartTask),
                (r"/restart_pipeline", RestartPipeline), 
                (r"/list_work_dir", ListWorkDirHandler),
                (r"/get_file_content", GetFileContentHandler),
                (r"/get_file_tail", GetFileTailHandler),
                (r"/get_proceeding", GetProceeding),
                (r"/get_access_token", GetAccessToken),
                (r"/create_job", CreateJob),
                (r"/get_job_list", GetJobList),
                (r"/get_job_detail", GetJobDetail),
                (r"/get_job_metrics", GetJobMetrics),
                (r"/get_job_log", GetJobLog),
                (r"/delete_job", DeleteJob),
        ])

        
        #self.__httpserver = tornado.httpserver.HTTPServer(self.__app)
        #self.__httpserver.listen(self.__admin_http_port)
        #self.__httpserver.start()
        self.__app.listen(self.__admin_http_port)
        logging.info("admin httpserver bind[%s:%s] started!" % (
                task_util.StaticFunction.get_local_ip(), 
                self.__admin_http_port))
        tornado.ioloop.IOLoop.instance().start()

    def stop(self):
        #self.__httpserver.stop()
        #tornado.ioloop.IOLoop.instance().stop()
        ioloop = tornado.ioloop.IOLoop.instance()
        ioloop.add_callback(ioloop.stop)
        logging.info("admin httpserver bind[%s:%s] stopped!" % (
                task_util.StaticFunction.get_local_ip(), 
                self.__admin_http_port))

class HttpHandlerParams(task_util.Singleton):
    def __init__(self, config=None, node_http_port=None, task_dispacher=None, kafka_manager=None):
        if not hasattr(self, "a"):
            if node_http_port is None or task_dispacher is None:
                raise EnvironmentError("none:task_dispacher, node_http_port")

            self.node_http_port = node_http_port
            self.task_dispacher = task_dispacher
            self.kafka_servers = None
            try:
                self.kafka_servers = config.get("admin", "kafka_servers")
            except:
                print("not support kafka")
                
            self.kafka_manager = kafka_manager
            self.a = "a"  # 必须在后面

class StopTaskHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        try:
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                self.write('error: this server has not got lock!')
                return

            self.__node_http_port = HttpHandlerParams().node_http_port
            self.__kafka_servers = HttpHandlerParams().kafka_servers
            self.__kafka_manager = HttpHandlerParams().kafka_manager
            self.__sql_manager = admin_sql_manager.SqlManager()

            unique_id = self.get_argument("unique_id", "").strip()
            if unique_id == '':
                self.write("error:unique_id is none")
                return

            schedule_id = self.get_argument("schedule_id", "").strip()
            if not schedule_id:
                self.write("error:schedule_id is none")
                return

            schedule = horae.models.Schedule.objects.get(id=schedule_id)
            if schedule.status == task_util.TaskState.TASK_WAITING \
                    or schedule.status == task_util.TaskState.TASK_READY:
                if not self.__sql_manager.stop_task_with_schedule_id(
                        schedule_id):
                    self.write("error:stop waiting task failed![db error!]")
                    return

                self.write("OK")
                return

            if schedule.status not in (
                    task_util.TaskState.TASK_RUNNING, 
                    task_util.TaskState.TASK_V100_PENDING, 
                    task_util.TaskState.TASK_V100_RUNING):
                if not self.__sql_manager.stop_task_with_schedule_id(
                        schedule_id):
                    self.write("error:stop task failed![db error!]")
                    return
                self.write("OK")
                return

            run_history = self.__sql_manager.get_runhsitory_with_id(
                    schedule_id)
            if run_history is None:
                self.write("error: find running task failed! not running!")
                return

            if task_util.StaticFunction.is_lan(run_history.run_server):
                msg_id = int(round(time.time() * 100000))
                data = {}
                data["msg_id"] = msg_id
                data["cmd"] = "stop_task"
                data["admin"] = True
                data["unique_id"] = unique_id
                data["schedule_id"] = schedule_id
                data["response"] = False
                producer = KafkaProducer(bootstrap_servers=self.__kafka_servers, api_version=(0, 10, 1), value_serializer=lambda m: json.dumps(m).encode())
                producer.send(run_history.run_server, data)
                try_times = 0
                res_data = None
                while try_times < 20:
                    res_data = self.__kafka_manager.get_data(msg_id)
                    if res_data is not None:
                        logging.info("success get res data %s, msg id: %d: %s" % (run_history.run_server, msg_id, res_data))
                        break
                        
                    logging.info("waiting get res data: %s, %d" % (run_history.run_server, msg_id))
                    time.sleep(0.1)
                    try_times += 1

                if res_data is None: 
                    self.write("failed")

                else:
                    self.write(res_data)
            else:
                node_req_url = (
                    "http://%s:%s/stop_task?unique_id=%s&schedule_id=%s" % (
                    run_history.run_server, 
                    self.__node_http_port, 
                    unique_id,
                    schedule_id))
                self.write(urllib.request.urlopen(node_req_url).read())
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.write("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
        except Exception as ex:
            logging.error("stop task failed![ex:%s][trace:%s]" % (
                    str(ex), traceback.format_exc()))
            self.write('error:' + str(ex) + traceback.format_exc())
        finally:
            self.finish()

class StopPipelineHandler(tornado.web.RequestHandler):
    """
        此处停止pipeline可能会对task_node执行任务产生冲突，
        所以，task_node如果改写db失败，则将其任务改写状态为失败
    """
    # 停止pipeline的流程
    # 1.停止schedule里面所有waiting状态的task
    # 2.改写run_history的任务状态
    # 3.停止ready_task正在执行的任务
    @tornado.gen.coroutine
    def get(self):
        try:
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                self.write('error: this server has not got lock!')
                return

            self.__sql_manager = admin_sql_manager.SqlManager()
            pl_id = self.get_argument("pl_id", "").strip()
            run_time = self.get_argument("run_time", "").strip()
            if pl_id == '' or run_time == '':
                self.write("error:pl_id or run_time is none")
                return
            pl_id = int(pl_id)
            if not self.__sql_manager.stop_pipeline(pl_id, run_time):
                self.write("error:write db failed")
                return
            self.write("OK")
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.write("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
        except Exception as ex:
            logging.error("stop pipeline failed![ex:%s][trace:%s]" % (
                    str(ex), traceback.format_exc()))
            self.write('error:' + str(ex) + traceback.format_exc())
        finally:
            self.finish()

class ListWorkDirHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        try:
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                self.write('error: this server has not got lock!')
                return

            self.__node_http_port = HttpHandlerParams().node_http_port
            self.__sql_manager = admin_sql_manager.SqlManager()
            schedule_id = self.get_argument("schedule_id", "").strip()
            if not schedule_id:
                self.write("error:schedule_id is none")
                return
            path = self.get_argument("path", "").strip()
            rerun_id = int(self.get_argument("rerun_id", 0).strip())
            if rerun_id > 0:
                run_history = self.__sql_manager.get_rerunhsitory_with_id(rerun_id)
            else:
                run_history = self.__sql_manager.get_runhsitory_with_id(
                        int(schedule_id))
            if run_history is None:
                self.write('error: has no ready_task info with'
                        ' schedule_id:%s' % schedule_id)
                return
        
            node_req_url = (
                    "http://%s:%s/list_work_dir?"
                    "schedule_id=%s&path=%s" % (
                    run_history.run_server, 
                    self.__node_http_port, 
                    schedule_id, 
                    path))
            self.write(urllib.request.urlopen(node_req_url).read())
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.write("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
        except Exception as ex:
            logging.error("list workdir failed![ex:%s][trace:%s]" % (
                    str(ex), traceback.format_exc()))
            self.write("error:%s" % traceback.format_exc())
        finally:
            self.finish()

class GetFileContentHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        try:
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                self.write('error: this server has not got lock!')
                return

            self.__node_http_port = HttpHandlerParams().node_http_port
            self.__sql_manager = admin_sql_manager.SqlManager()
            schedule_id = self.get_argument("schedule_id", "").strip()
            file_name = self.get_argument("file", "").strip()
            start = int(self.get_argument("start", "0").strip())
            len = int(self.get_argument("len", "10240").strip())
            if schedule_id.strip() == '' or file_name.strip() == '':
                self.write("admin error:schedule_id or file_name required")
                return

            rerun_id = int(self.get_argument("rerun_id", "0").strip())
            if rerun_id > 0:
                run_history = self.__sql_manager.get_rerunhsitory_with_id(rerun_id)
            else:
                run_history = self.__sql_manager.get_runhsitory_with_id(
                        int(schedule_id))
            if run_history is None:
                self.write('error: has no ready_task info with'
                        ' schedule_id:%s' % schedule_id)
                return
        
            node_req_url = ("http://%s:%s/get_file_content?schedule_id=%s"
                "&file=%s&start=%d&len=%d" % (
                run_history.run_server, 
                self.__node_http_port, 
                schedule_id,
                file_name, 
                start, 
                len))
            self.write(urllib.request.urlopen(node_req_url).read())
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.write("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
        except Exception as ex:
            logging.error("get file content failed![ex:%s][trace:%s]" % (
                    str(ex), traceback.format_exc()))
            self.write("error:%s" % traceback.format_exc())
        finally:
            self.finish()

class GetFileTailHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        try:
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                self.write('error: this server has not got lock!')
                return

            self.__node_http_port = HttpHandlerParams().node_http_port
            self.__sql_manager = admin_sql_manager.SqlManager()
            schedule_id = self.get_argument("schedule_id", "").strip()
            file_name = self.get_argument("file", "").strip()
            if schedule_id.strip() == '' or file_name.strip() == '':
                self.write("admin error:schedule_id or file_name required")
                return
            
            rerun_id = int(self.get_argument("rerun_id", "0").strip())
            if rerun_id > 0:
                run_history = self.__sql_manager.get_rerunhsitory_with_id(rerun_id)
            else:
                run_history = self.__sql_manager.get_runhsitory_with_id(
                        int(schedule_id))

            if run_history is None:
                self.write('error: has no ready_task info with'
                        ' schedule_id:%s' % schedule_id)
                return
        
            node_req_url = ("http://%s:%s/get_file_tail?schedule_id=%s"
                "&file=%s" % (
                run_history.run_server, 
                self.__node_http_port, 
                schedule_id,
                file_name))
            self.write(urllib.request.urlopen(node_req_url).read())
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.write("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
        except Exception as ex:
            logging.error("get file content failed![ex:%s][trace:%s]" % (
                    str(ex), traceback.format_exc()))
            self.write("error:%s" % traceback.format_exc())
        finally:
            self.finish()

class RestartTask(tornado.web.RequestHandler):
    #@tornado.web.asynchronous
    def get(self):
        try:
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                self.write('error: this server has not got lock!')
                return

            task_pair_json = self.get_argument("task_pair_json", "").strip()
            if task_pair_json == '':
                self.write("error:task_id_list is none")
                return

            ordered = self.get_argument("ordered", "0").strip()
            if ordered != '0':
                ordered = int(ordered)
                if ordered <= 0:
                    ordered = 0
            else:
                ordered = 0

            # task_pair_json = urllib.request.unquote(str(task_pair_json)).decode("gbk")
            task_pair_json = urllib.request.unquote(str(task_pair_json))
            task_pair_json = json.loads(task_pair_json)
            json_task_pair_list = task_pair_json["task_pair_list"]
            task_pair_list = []
            for task_pair in json_task_pair_list:
                task_pair_list.append((
                        str(task_pair["task_id"]), 
                        task_pair["run_time"].strip()))

            task_control = task_controled_by_user.TaskControledByUser()
            ret = task_control.restart_tasks(task_pair_list, ordered)
            self.write(ret)
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.write("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
        except Exception as ex:
            logging.error("RestartTask task failed![ex:%s][trace:%s]" % (
                    str(ex), traceback.format_exc()))
            self.write('error:' + str(ex) + traceback.format_exc())
        finally:
            self.finish()

class GetAccessToken(tornado.web.RequestHandler):
    #@tornado.web.asynchronous
    def get(self):
        try:
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                self.write('error: this server has not got lock!')
                return
            # 要请求的 URL
            node_req_url = "http://zjcp.zhejianglab.cn:38080/api/open/credentials"  #目标 URL
            # 要发送的数据，使用字典表示
            data = {
                'appKey': '6a0f59bd46ec4e44bfd3b8cbe70f5395',
                'appSecret': 'ddc6224168ea47bda71680badd338599'
            }
            # 将字典转换为 JSON 格式
            data_json = json.dumps(data).encode('utf-8')

            # 创建一个请求对象，设置请求类型为 POST，并添加 Content-Type 头
            request = urllib.request.Request(node_req_url, data=data_json, method='POST')
            request.add_header('Content-Type', 'application/json')
            # 发送请求并读取响应
            response = urllib.request.urlopen(request).read()
            self.write(response.decode('utf-8'))
            response_json = json.loads(response.decode('utf-8'))
            # 获取 accessToken 的值
            access_token = response_json['data']['accessToken']
            AdminHttpServer.global_access_token = access_token
            logging.info("Access Token:", access_token)
        except urllib.error.HTTPError as ex:
            self.write('error:' + str(ex) + traceback.format_exc())
        except urllib.error.URLError as ex:
            self.write('error:' + str(ex) + traceback.format_exc())
        finally:
            self.finish()

def get_access_token(req_url):
    try:
        if req_url is None:
            req_url = "http://127.0.0.1:18791/get_access_token"
        # 发送请求并读取响应
        response = urllib.request.urlopen(req_url).read()

        # 解码并解析响应为 JSON 对象
        response_json = json.loads(response.decode('utf-8'))

        # 获取 accessToken 的值
        access_token = response_json['data']['accessToken']
        return access_token

    except urllib.error.HTTPError as e:
        print(f"HTTP error: {e.code} - {e.reason}")
    except urllib.error.URLError as e:
        print(f"URL error: {e.reason}")
    except json.JSONDecodeError as e:
        print("JSON decode error:", e)
    except KeyError as e:
        print(f"Key error: {e} not found in the response.")
    return None  # 如果发生错误，返回 None


class CreateJob(tornado.web.RequestHandler):
    #@tornado.web.asynchronous
    def get(self):
        try:
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                self.write('error: this server has not got lock!')
                return
            job_name = self.get_argument("jobName", "").strip()
            image = self.get_argument("image", "").strip()
            command_ = self.get_argument("command", "").strip()
            gpu = int(self.get_argument("gpu", "0").strip())  # 将 gpu 转为整型，默认值为 0
            cpu = int(self.get_argument("cpu", "1").strip())  # 将 cpu 转为整型，默认值为 1
            memory = int(self.get_argument("memory", "2").strip())  # 将 cpu 转为整型，默认值为 2
            if not job_name:  # 如果 job_name 是 None 或者 ""
                job_name = "datapipe_run9999";
            if not image:
                image = "10.200.88.53/liuyangyang-zhejianglab.com/data_pipeline:v1.8";
            if not command_:
                command_ = "echo \"hello world\"";
            # 要请求的 URL
            node_req_url = "http://zjcp.zhejianglab.cn:38080/api/open/open-job/origin/v2/createJob?userId=585398485541453824&bizType=DROS"  #目标 URL
            # 要发送的数据，使用字典表示
            data = {
                "userId": "5853984855414538240",
                "jobMeta": {
                    "jobName": job_name,
                    "describe": "一个推理作业",
                    "bizType": "DROS",
                    "jobType": "AI_INFERENCE",
                    "jobSpotType": "normal",
                    "subMissionId": "pr-7986679722762338304",
                    "subMissionName": "GeoCloud数据网络及计算引擎默认子任务"
                },
                "jobInfo": {
                    "networkType": "default",
                    "image": image,
                    "volumes": [
                        {
                            "volumeId": "vol-8003538313461092352",
                            "subPath": None,
                            "mountPath": "/DATA/",
                            "readOnly": False
                        }
                    ],
                    "command": command_
                },
                "jobResource": {
                    "zoneType": "AI_GPU",
                    "spec": "GPU_V100_32GB",
                    "resourceType": "PUBLIC",
                    "jobStartCount": 2,
                    "gpu": gpu,
                    "cpu": cpu,
                    "memory": memory
                }
            }
            # 将字典转换为 JSON 格式
            data_json = json.dumps(data).encode('utf-8')

            # 创建一个请求对象，设置请求类型为 POST，并添加 Content-Type 头
            request = urllib.request.Request(node_req_url, data=data_json, method='POST')
            request.add_header('Content-Type', 'application/json')
            request.add_header('accessToken', AdminHttpServer.global_access_token)
            # 发送请求并读取响应
            response = urllib.request.urlopen(request).read()
            self.write(response.decode('utf-8'))
        except urllib.error.HTTPError as ex:
            self.write('error:' + str(ex) + traceback.format_exc())
        except urllib.error.URLError as ex:
            self.write('error:' + str(ex) + traceback.format_exc())
        finally:
            self.finish()

class GetJobList(tornado.web.RequestHandler):
    #@tornado.web.asynchronous
    def get(self):
        try:
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                self.write('error: this server has not got lock!')
                return
            # 要请求的 URL
            node_req_url = "http://zjcp.zhejianglab.cn:38080/api/open/credentials"  #目标 URL
            # 要发送的数据，使用字典表示
            data = {
                'appKey': '6a0f59bd46ec4e44bfd3b8cbe70f5395',
                'appSecret': 'ddc6224168ea47bda71680badd338599'
            }
            # 将字典转换为 JSON 格式
            data_json = json.dumps(data).encode('utf-8')

            # 创建一个请求对象，设置请求类型为 POST，并添加 Content-Type 头
            request = urllib.request.Request(node_req_url, data=data_json, method='POST')
            request.add_header('Content-Type', 'application/json')
            # 发送请求并读取响应
            response = urllib.request.urlopen(request).read()
            self.write(response.decode('utf-8'))
            response_json = json.loads(response.decode('utf-8'))
            # 获取 accessToken 的值
            access_token = response_json['data']['accessToken']
            logging.info("Access Token:", access_token)
        except urllib.error.HTTPError as ex:
            self.write('error:' + str(ex) + traceback.format_exc())
        except urllib.error.URLError as ex:
            self.write('error:' + str(ex) + traceback.format_exc())
        finally:
            self.finish()

class GetJobDetail(tornado.web.RequestHandler):
    #@tornado.web.asynchronous
    def get(self):
        try:
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                self.write('error: this server has not got lock!')
                return
            # 要请求的 URL
            node_req_url = "http://zjcp.zhejianglab.cn:38080/api/open/credentials"  #目标 URL
            # 要发送的数据，使用字典表示
            data = {
                'appKey': '6a0f59bd46ec4e44bfd3b8cbe70f5395',
                'appSecret': 'ddc6224168ea47bda71680badd338599'
            }
            # 将字典转换为 JSON 格式
            data_json = json.dumps(data).encode('utf-8')

            # 创建一个请求对象，设置请求类型为 POST，并添加 Content-Type 头
            request = urllib.request.Request(node_req_url, data=data_json, method='POST')
            request.add_header('Content-Type', 'application/json')
            # 发送请求并读取响应
            response = urllib.request.urlopen(request).read()
            self.write(response.decode('utf-8'))
            response_json = json.loads(response.decode('utf-8'))
            # 获取 accessToken 的值
            access_token = response_json['data']['accessToken']
            logging.info("Access Token:", access_token)
        except urllib.error.HTTPError as ex:
            self.write('error:' + str(ex) + traceback.format_exc())
        except urllib.error.URLError as ex:
            self.write('error:' + str(ex) + traceback.format_exc())
        finally:
            self.finish()

class GetJobMetrics(tornado.web.RequestHandler):
    #@tornado.web.asynchronous
    def get(self):
        try:
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                self.write('error: this server has not got lock!')
                return
            # 要请求的 URL
            node_req_url = "http://zjcp.zhejianglab.cn:38080/api/open/credentials"  #目标 URL
            # 要发送的数据，使用字典表示
            data = {
                'appKey': '6a0f59bd46ec4e44bfd3b8cbe70f5395',
                'appSecret': 'ddc6224168ea47bda71680badd338599'
            }
            # 将字典转换为 JSON 格式
            data_json = json.dumps(data).encode('utf-8')

            # 创建一个请求对象，设置请求类型为 POST，并添加 Content-Type 头
            request = urllib.request.Request(node_req_url, data=data_json, method='POST')
            request.add_header('Content-Type', 'application/json')
            # 发送请求并读取响应
            response = urllib.request.urlopen(request).read()
            self.write(response.decode('utf-8'))
            response_json = json.loads(response.decode('utf-8'))
            # 获取 accessToken 的值
            access_token = response_json['data']['accessToken']
            logging.info("Access Token:", access_token)
        except urllib.error.HTTPError as ex:
            self.write('error:' + str(ex) + traceback.format_exc())
        except urllib.error.URLError as ex:
            self.write('error:' + str(ex) + traceback.format_exc())
        finally:
            self.finish()

class GetJobLog(tornado.web.RequestHandler):
    #@tornado.web.asynchronous
    def get(self):
        try:
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                self.write('error: this server has not got lock!')
                return
            # 要请求的 URL
            node_req_url = "http://zjcp.zhejianglab.cn:38080/api/open/credentials"  #目标 URL
            # 要发送的数据，使用字典表示
            data = {
                'appKey': '6a0f59bd46ec4e44bfd3b8cbe70f5395',
                'appSecret': 'ddc6224168ea47bda71680badd338599'
            }
            # 将字典转换为 JSON 格式
            data_json = json.dumps(data).encode('utf-8')

            # 创建一个请求对象，设置请求类型为 POST，并添加 Content-Type 头
            request = urllib.request.Request(node_req_url, data=data_json, method='POST')
            request.add_header('Content-Type', 'application/json')
            # 发送请求并读取响应
            response = urllib.request.urlopen(request).read()
            self.write(response.decode('utf-8'))
            response_json = json.loads(response.decode('utf-8'))
            # 获取 accessToken 的值
            access_token = response_json['data']['accessToken']
            logging.info("Access Token:", access_token)
        except urllib.error.HTTPError as ex:
            self.write('error:' + str(ex) + traceback.format_exc())
        except urllib.error.URLError as ex:
            self.write('error:' + str(ex) + traceback.format_exc())
        finally:
            self.finish()

class DeleteJob(tornado.web.RequestHandler):
    #@tornado.web.asynchronous
    def get(self):
        try:
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                self.write('error: this server has not got lock!')
                return
            job_id = self.get_argument("job_id", "").strip()

            # 要请求的 URL
            node_req_url = ("http://zjcp.zhejianglab.cn:38080/api/open/open-job/user/v1/deleteJob?userId=585398485541453824&bizType=DROS&jobId=%s"
                             % (job_id))
            # 创建一个请求对象，设置请求类型为 DELETE，并添加 Content-Type 头
            request = urllib.request.Request(node_req_url, data=None, method='DELETE')
            request.add_header('Content-Type', 'application/json')
            request.add_header('accessToken', AdminHttpServer.global_access_token)
            # 发送请求并读取响应
            response = urllib.request.urlopen(request).read()
            self.write(response.decode('utf-8'))
        except urllib.error.HTTPError as ex:
            self.write('error:' + str(ex) + traceback.format_exc())
        except urllib.error.URLError as ex:
            self.write('error:' + str(ex) + traceback.format_exc())
        finally:
            self.finish()

class RestartPipeline(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        try:
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                self.write('error: this server has not got lock!')
                return

            pl_id = self.get_argument("pl_id", "").strip()
            if pl_id == '':
                self.write("error:pipeline_id is none")
                return

            run_time = self.get_argument("run_time", "").strip()
            if run_time == '':
                self.write("error:run_time is none")
                return

            tasks = horae.models.Task.objects.filter(pl_id=pl_id)
            if tasks is None or len(tasks) <= 0:
                self.write("error:pipeline_id[%s] has no tasks." % pl_id)
                return

            task_pair_list = []
            for task in tasks:
                task_pair_list.append((str(task.id), run_time))

            task_control = task_controled_by_user.TaskControledByUser()
            ret = task_control.restart_tasks(task_pair_list, 0)
            self.write(ret)
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.write("error:%s" % traceback.format_exc())
        except Exception as ex:
            logging.error("stop task failed![ex:%s][trace:%s]" % (
                    str(ex), traceback.format_exc()))
            self.write('error:' + str(ex) + traceback.format_exc())
        finally:
            self.finish()

class GetProceeding(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        try:
            if task_util.CONSTANTS.HOLD_FOR_ZOOKEEPER_LOCK:
                self.write('error: this server has not got lock!')
                return

            self.__node_http_port = HttpHandlerParams().node_http_port
            self.__sql_manager = admin_sql_manager.SqlManager()
            self.__kafka_servers = HttpHandlerParams().kafka_servers
            self.__kafka_manager = HttpHandlerParams().kafka_manager
            unique_id = self.get_argument("unique_id", "").strip()
            if unique_id == '':
                self.write("error:unique_id is none")
                return

            schedule_id = self.get_argument("schedule_id", "").strip()
            if not schedule_id:
                self.write("error:schedule_id is none")
                return
            run_history = self.__sql_manager.get_runhsitory_with_id(
                    int(schedule_id))
            if run_history is None:
                self.write('error: has no run_history info with'
                        ' schedule_id:%s' % schedule_id)
                return
            if task_util.StaticFunction.is_lan(run_history.run_server):
                msg_id = int(round(time.time() * 100000))
                data = {}
                data["msg_id"] = msg_id
                data["cmd"] = "get_proceeding"
                data["admin"] = True
                data["unique_id"] = unique_id
                data["schedule_id"] = schedule_id
                data["response"] = False
                producer = KafkaProducer(bootstrap_servers=self.__kafka_servers, api_version=(0, 10, 1), value_serializer=lambda m: json.dumps(m).encode())
                producer.send(run_history.run_server, data)
                try_times = 0
                res_data = None
                while try_times < 20:
                    res_data = self.__kafka_manager.get_data(msg_id)
                    if res_data is not None:
                        logging.info("success get res data %s : %s" % (run_history.run_server, res_data))
                        break
                        
                    logging.info("waiting get res data: %s" % run_history.run_server)
                    time.sleep(0.1)
                    try_times += 1

                if res_data is None: 
                    self.write("failed")

                else:
                    self.write(res_data)
            else:
                node_req_url = (
                    "http://%s:%s/get_proceeding?"
                    "unique_id=%s&schedule_id=%s" % (
                    run_history.run_server, 
                    self.__node_http_port, 
                    unique_id, 
                    schedule_id))
                self.write(urllib.request.urlopen(node_req_url).read())
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.write("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
        except Exception as ex:
            logging.error("stop list workdir failed![ex:%s][trace:%s]" % (
                    str(ex), traceback.format_exc()))
            self.write("error:%s" % traceback.format_exc())
        finally:
            self.finish()

if __name__ == "__main__":
    server = HttpServer()
    server.start()
