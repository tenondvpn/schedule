###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import os
import sys
import traceback
import logging
import threading
import json
import random

import tornado.httpserver
import tornado.web
import tornado.ioloop
import horae.models
import django.db
from kafka import KafkaConsumer
from kafka import KafkaProducer

sys.path.append('../common')
import task_util
import linux_file_cmd
import node_sql_manager
import script_task_handler
import odps_sql_task_handler
import http_msg_queue
import script_task_handler
#import oozie_task_handler
import docker_task_handler
import clickhouse_task_handler
import v100_script_task_handler
import shell_task_handler

def list_work_dir(req_json):
    try:
        schedule_id = req_json["schedule_id"].strip()
        if not schedule_id:
            return "error:schedule_id is none"
        
        rerun_id = int(req_json["rerun_id"].strip())
        work_dir = HttpHandlerParams().get_log_path(
                schedule_id, rerun_id)
        if work_dir is None:
            return "error:this node has no work dir"
        
        path = req_json["path"].strip()
        if path != '':
            work_dir = os.path.join(work_dir, path)

        file_cmd = linux_file_cmd.LinuxFileCommand()
        return file_cmd.ls_dir(work_dir)
    except Exception as ex:
        logging.error(traceback.format_exc())
        return "error:%s" % traceback.format_exc()
    
def get_file_content(req_json):
    try:
        schedule_id = req_json["schedule_id"].strip()
        file_name = req_json["file"].strip()
        start = int(req_json["start"].strip())
        if start >= task_util.CONSTANTS.MAX_FILE_DOWNLOAD_SIZE:
            start = 0

        len = int(req_json["len"])
        if len <= 0 or len >= 102400:
            len = 102400

        if schedule_id.strip() == '' or file_name.strip() == '':
            return ("error:schedule_id or file_name required")

        rerun_id = int(req_json["rerun_id"].strip())
        work_dir = HttpHandlerParams().get_log_path(
                schedule_id, rerun_id)
        if work_dir is None or work_dir.strip() == "":
            return ("error:this node has no work dir")

        file_path = os.path.join(
                work_dir, 
                file_name)
        ret, content = task_util.StaticFunction.\
                get_file_content_with_start_and_len(file_path, start, len)
        if ret != task_util.FileCommandRet.FILE_COMMAND_SUCC:
            return("error:read file failed %s" % file_path)
            
        return (content)
    except Exception as ex:
        return (traceback.format_exc())

def get_file_tail(req_json):
    try:
        schedule_id = req_json["schedule_id"].strip()
        file_name = req_json["file"].strip()
        if schedule_id.strip() == '' or file_name.strip() == '':
            return ("error:schedule_id or file_name required")

        rerun_id = int(req_json["rerun_id"].strip())
        work_dir = HttpHandlerParams().get_log_path(
                schedule_id, rerun_id)
        if work_dir is None:
            return ("error:this node has no work dir")

        file_path = os.path.join(work_dir, file_name)
        content = linux_file_cmd.LinuxFileCommand().file_tail(file_path)
        if content is None:
            return ("error:read file failed %s" % file_path)

        return (content.decode("utf-8"))
    except Exception as ex:
        return ("error:%s" % traceback.format_exc())
    
def stop_task_call_back(param_list):
    try:
        schedule_id = param_list[0].strip()
        if not schedule_id:
            return "error:schedule_id is none"
        schedule = horae.models.Schedule.objects.get(id=schedule_id)
        task = horae.models.Task.objects.get(id=schedule.task_id)
        processor = horae.models.Processor.objects.get(id=task.pid)
        task_hanlder = HttpHandlerParams().get_task_handler(processor.type)
        if task_hanlder is None:
            return "wrong task type:%s" % processor.type
        sql_manager = node_sql_manager.SqlManager()
        task_info = sql_manager.get_task_info_with_schedule_id(
                HttpHandlerParams().get_file_path_with_schedule_id(
                        schedule_id), 
                schedule_id)
        if task_info is None:
            return "error:get task info form db error!"

        if not task_hanlder.stop_task(task_info):
            return "error: stop task failed!"

        if not sql_manager.stop_task_with_schedule_id(
                int(schedule_id)):
            return 'error: stop task write db error: %s' % schedule_id

        return "OK"
    except django.db.OperationalError as ex:
        django.db.close_old_connections()
        return task_util.CONSTANTS.HTTP_RETRY_MSG
    except Exception as ex:
        logging.error(traceback.format_exc())
        return "error:%s" % traceback.format_exc()
        
def stop_task(req_json):
    try:
        unique_id = req_json["unique_id"].strip()
        if unique_id == '':
            return ("error:unique_id is none")

        schedule_id = req_json["schedule_id"].strip()
        if not schedule_id:
            return ("error:schedule_id is none")
        HttpHandlerParams().http_msg_queue.add_request_to_queue_map(
                unique_id, 
                [schedule_id,], 
                stop_task_call_back)
        
        response = HttpHandlerParams().http_msg_queue.get_response_from_map(
                        unique_id)
        return (response)
    except django.db.OperationalError as ex:
        django.db.close_old_connections()
        return ("error:%s" % traceback.format_exc())
    except Exception as ex:
        logging.error(traceback.format_exc())
        return ("error:%s" % traceback.format_exc())
        
    logging.info("stop_task 9")
    
def get_proceeding(req_json):
        try:
            unique_id = req_json["unique_id"].strip()
            if unique_id == '':
                return ("error:unique_id is none")

            schedule_id = req_json["schedule_id"].strip()
            if not schedule_id:
                return ("error:schedule_id is none")

            HttpHandlerParams().http_msg_queue.add_request_to_queue_map(
                    unique_id, 
                    [schedule_id,], 
                    proceeding_callback)
            response = HttpHandlerParams().http_msg_queue.get_response_from_map(
                            unique_id)
            return (response)
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            return ("error:%s" % traceback.format_exc())
        except Exception as ex:
            logging.error(traceback.format_exc())
            return ("error:%s" % traceback.format_exc())

def proceeding_callback(param_list):
    try:
        if len(param_list) <= 0:
            return "error: handle param_list failed!"

        schedule_id = int(param_list[0])
        schedule = horae.models.Schedule.objects.get(id=schedule_id)
        task = horae.models.Task.objects.get(id=schedule.task_id)
        processor = horae.models.Processor.objects.get(id=task.pid)
        task_hanlder = HttpHandlerParams().get_task_handler(processor.type)
        if task_hanlder is None:
            raise Exception(
                    "wrong task type:%s" % processor.type)
        sql_manager = node_sql_manager.SqlManager()
        task_info = sql_manager.get_task_info_with_schedule_id(
                HttpHandlerParams().get_file_path_with_schedule_id(
                        schedule_id), 
                schedule_id)
        if task_info is None:
            return ("error:get task info form db error!")
        
        proceding = task_hanlder.get_proceeding(task_info)
        return proceding
    except django.db.OperationalError as ex:
        django.db.close_old_connections()
        return task_util.CONSTANTS.HTTP_RETRY_MSG
    except Exception as ex:
        logging.error(traceback.format_exc())
        return "error:%s" % traceback.format_exc()
                
class KafkaRequestManager(threading.Thread):
    """
        1.管理长等待的http请求
        2.多线程执行http请求，并保存执行结果
    """
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.__log = logging
        self.__public_ip = config.get("node", "public_ip")
        self.__kafka_servers = config.get("node", "kafka_servers")
        if self.__kafka_servers is None:
            self.__kafka_servers = ""

        if task_util.StaticFunction.is_lan(self.__public_ip):
            producer = KafkaProducer(
                bootstrap_servers=self.__kafka_servers, 
                api_version=(0, 10, 1), 
                value_serializer=lambda m: json.dumps(m).encode())
            producer.send(self.__public_ip, "init")
        self.__config = config

    def run(self):
        if not task_util.StaticFunction.is_lan(self.__public_ip):
            return
        
        self.__log.info("KafkaRequestManager thread starting...")
        # 全局退出标示字段
        producer = KafkaProducer(
            bootstrap_servers=self.__kafka_servers, 
            api_version=(0, 10, 1), 
            value_serializer=lambda m: json.dumps(m).encode())
        consumer = KafkaConsumer(
            self.__public_ip,
            bootstrap_servers=self.__kafka_servers,
            auto_offset_reset='latest')
        while True:
            data = consumer.poll(timeout_ms=1000, max_records=1)  # 拉取消息，字典类型
            if data:
                for key in data:
                    json_map = json.loads(data[key][0].value)
                    res_map = {}
                    res_map["msg_id"] = json_map["msg_id"]
                    if json_map["cmd"] == "list_work_dir":
                        res_map["data"] = list_work_dir(json_map)
                    elif json_map["cmd"] == "get_file_content":
                        res_map["data"] = get_file_content(json_map)
                    elif json_map["cmd"] == "get_file_tail":
                        res_map["data"] = get_file_tail(json_map)
                    elif json_map["cmd"] == "stop_task":
                        res_map["data"] = stop_task(json_map)
                    elif json_map["cmd"] == "get_proceeding":
                        res_map["data"] = get_proceeding(json_map)
                    else:
                        res_map["data"] = "invalid cmd: " + json_map["cmd"]

                    if json_map["cmd"] == "stop_task" or json_map["cmd"] == "get_proceeding":
                        producer.send("admin_all_message", res_map)
                    else:
                        producer.send("all_message", res_map)

        self.__log.info("HttpRequestHandler thread exited!")

class NodeHttpServer(object):
    '''
        node的http服务 
    '''
    def __init__(self, config, handle_ready_tasks):
        self.__config = config
        self.__node_http_port = config.get("node", "node_http_port")
        self.__handle_ready_task = handle_ready_tasks
        self.__disks = config.get('node', 'disks').split(';')
        self.__http_param = HttpHandlerParams(
                self.__handle_ready_task,
                self.__disks,
                self.__config)
        
    def start(self):
        self.__app = tornado.web.Application(handlers=[
                (r"/list_work_dir", ListWorkDirHandler),
                (r"/get_file_content", GetFileContentHandler),
                (r"/get_proceeding", GetProceeding),
                (r"/get_file_tail", GetFileTailHandler),
                (r"/stop_task", StopTaskHandler),
            ])

        #self.__httpserver = tornado.httpserver.HTTPServer(self.__app)
        #self.__httpserver.listen(self.__node_http_port)
        #self.__httpserver.start()
        self.__app.listen(self.__node_http_port)
        tornado.ioloop.IOLoop.instance().start()

    def stop(self):
        #self.__httpserver.stop()
        #tornado.ioloop.IOLoop.instance().stop()
        ioloop = tornado.ioloop.IOLoop.instance()
        ioloop.add_callback(ioloop.stop)
        logging.info("admin httpserver bind[%s:%s] stopped!" % (
                task_util.StaticFunction.get_local_ip(), 
                self.__node_http_port))

class HttpHandlerParams(task_util.Singleton):
    def __init__(self, handle_ready_task=None, disks=None, config=None):
        if not hasattr(self, "a"):
            if handle_ready_task is None or disks is None:
                raise EnvironmentError("none:base_work_dir, handle_ready_task")

            self.handle_ready_task = handle_ready_task
            self.disks = disks
            self.config = config
            self.rerun_path = self.config.get("node", "rerun_path").strip()
            self.a = "a"  # 必须在后面
            self.http_msg_queue = http_msg_queue.HttpRequestManager(config)
            self.http_msg_queue.start()

    def get_file_path_with_schedule_id(self, schedule_id):
        path_list = []
        last_modify_path = None
        max_modify_time = 0
        for disk in self.disks:
            file_path = "%s/data_platform/%s/" % (disk, schedule_id)
            if os.path.exists(file_path):
                path_list.append(file_path)
                modify_time = os.path.getctime(file_path)
                if modify_time > max_modify_time:
                    max_modify_time = modify_time
                    last_modify_path = file_path

        return last_modify_path

    def get_log_path(self, schedule_id, rerun_id):
        if rerun_id > 0:
            log_path = os.path.join(self.rerun_path, str(rerun_id))
            if os.path.exists(log_path):
                return log_path

        return self.get_file_path_with_schedule_id(schedule_id)


    def get_task_handler(self, task_type):
        if task_type == task_util.TaskType.SCRIPT_TYPE:
            return script_task_handler.ScriptTaskHandler(
                    HttpHandlerParams().config, self.handle_ready_task)
        elif task_type == task_util.TaskType.SPARK_TYPE:
            return script_task_handler.ScriptTaskHandler(
                    HttpHandlerParams().config, self.handle_ready_task)
        #elif task_type == task_util.TaskType.OOZIE_TYPE:
        #    return oozie_task_handler.OozieTaskHandler(
        #            self.__config)
        elif task_type == task_util.TaskType.ODPS_TYPE:
            return odps_sql_task_handler.OdpsSqlTaskHandler(
                     HttpHandlerParams().config)
        elif task_type == task_util.TaskType.SHELL_TYPE:
            return shell_task_handler.ShellTaskHandler(
                    HttpHandlerParams().config, self.handle_ready_task)
        elif task_type == task_util.TaskType.DOCKER_TYPE:
            return docker_task_handler.DockerTaskHandler(
                     HttpHandlerParams().config)
        elif task_type == task_util.TaskType.CLICKHOUSE_TYPE:
            return clickhouse_task_handler.ClickhouseTaskHandler(
                     HttpHandlerParams().config)
        elif task_type == task_util.TaskType.V100_TYPE:
            return v100_script_task_handler.V100ScriptTaskHandler(
                    HttpHandlerParams().config, self.handle_ready_task)
        else:
            pass

        return None

class GetProceeding(tornado.web.RequestHandler):
    #@tornado.web.asynchronous
    def get(self):
        try:
            unique_id = self.get_argument("unique_id", "").strip()
            if unique_id == '':
                self.write("error:unique_id is none")
                return

            schedule_id = self.get_argument("schedule_id", "").strip()
            if not schedule_id:
                self.write("error:schedule_id is none")
                return

            HttpHandlerParams().http_msg_queue.add_request_to_queue_map(
                    unique_id, 
                    [schedule_id,], 
                    self.__proceeding_callback)
            
            response = HttpHandlerParams().http_msg_queue.get_response_from_map(
                            unique_id)
            self.write(response)
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.write("error:%s" % traceback.format_exc())
        except Exception as ex:
            logging.error(traceback.format_exc())
            self.write("error:%s" % traceback.format_exc())
        finally:
            self.finish()

    def __proceeding_callback(self, param_list):
        try:
            if len(param_list) <= 0:
                return "error: handle param_list failed!"

            schedule_id = int(param_list[0])
            schedule = horae.models.Schedule.objects.get(id=schedule_id)
            task = horae.models.Task.objects.get(id=schedule.task_id)
            processor = horae.models.Processor.objects.get(id=task.pid)
            task_hanlder = HttpHandlerParams().get_task_handler(processor.type)
            if task_hanlder is None:
                raise Exception(
                        "wrong task type:%s" % processor.type)
            sql_manager = node_sql_manager.SqlManager()
            task_info = sql_manager.get_task_info_with_schedule_id(
                    HttpHandlerParams().get_file_path_with_schedule_id(
                            schedule_id), 
                    schedule_id)
            if task_info is None:
                self.write("error:get task info form db error!")
                return
            proceding = task_hanlder.get_proceeding(task_info)
            return proceding
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            return task_util.CONSTANTS.HTTP_RETRY_MSG
        except Exception as ex:
            logging.error(traceback.format_exc())
            return "error:%s" % traceback.format_exc()

class ListWorkDirHandler(tornado.web.RequestHandler):
    #@tornado.web.asynchronous
    def get(self):
        try:
            self.__file_cmd = linux_file_cmd.LinuxFileCommand()
            schedule_id = self.get_argument("schedule_id", "").strip()
            if not schedule_id:
                self.write("error:schedule_id is none")
                return
            
            rerun_id = int(self.get_argument("rerun_id", "0").strip())
            work_dir = HttpHandlerParams().get_log_path(
                    schedule_id, rerun_id)
            if work_dir is None:
                self.write("error:this node has no work dir")
                return
            path = self.get_argument("path", "").strip()
            if path != '':
                work_dir = os.path.join(work_dir, path)
            self.write(self.__file_cmd.ls_dir(work_dir))
        except Exception as ex:
            logging.error(traceback.format_exc())
            self.write("error:%s" % traceback.format_exc())
        finally:
            self.finish()

class GetFileContentHandler(tornado.web.RequestHandler):
    #@tornado.web.asynchronous
    def get(self):
        try:
            schedule_id = self.get_argument("schedule_id", "").strip()
            file_name = self.get_argument("file", "").strip()
            start = int(self.get_argument("start", "0").strip())
            if start >= task_util.CONSTANTS.MAX_FILE_DOWNLOAD_SIZE:
                self.write("download ended! max 1G.")
                return

            len = int(self.get_argument("len", "10240").strip())
            if schedule_id.strip() == '' or file_name.strip() == '':
                self.write("error:schedule_id or file_name required")
                return

            rerun_id = int(self.get_argument("rerun_id", "0").strip())
            work_dir = HttpHandlerParams().get_log_path(
                    schedule_id, rerun_id)
            if work_dir is None or work_dir.strip() == "":
                self.write("error:this node has no work dir")
                return

            file_path = os.path.join(
                    work_dir, 
                    file_name)
            ret, content = task_util.StaticFunction.\
                    get_file_content_with_start_and_len(file_path, start, len)
            if ret != task_util.FileCommandRet.FILE_COMMAND_SUCC:
                self.write("error:read file failed %s" % file_path)
                return
            self.write(content)
        except Exception as ex:
            logging.error(traceback.format_exc())
            self.write("error:%s" % traceback.format_exc())
        finally:
            self.finish()

class GetFileTailHandler(tornado.web.RequestHandler):
    #@tornado.web.asynchronous
    def get(self):
        try:
            schedule_id = self.get_argument("schedule_id", "").strip()
            file_name = self.get_argument("file", "").strip()
            if schedule_id.strip() == '' or file_name.strip() == '':
                self.write("error:schedule_id or file_name required")
                return

            rerun_id = int(self.get_argument("rerun_id", "0").strip())
            work_dir = HttpHandlerParams().get_log_path(
                    schedule_id, rerun_id)
            if work_dir is None:
                self.write("error:this node has no work dir")
                return

            file_path = os.path.join(work_dir, file_name)
            content = linux_file_cmd.LinuxFileCommand().file_tail(file_path)
            if content is None:
                self.write("error:read file failed %s" % file_path)
                return

            self.write(content)
        except Exception as ex:
            logging.error(traceback.format_exc())
            self.write("error:%s" % traceback.format_exc())
        finally:
            self.finish()

class StopTaskHandler(tornado.web.RequestHandler):
    #@tornado.web.asynchronous
    def get(self):
        try:
            unique_id = self.get_argument("unique_id", "").strip()
            if unique_id == '':
                self.write("error:unique_id is none")
                return

            schedule_id = self.get_argument("schedule_id", "").strip()
            if not schedule_id:
                self.write("error:schedule_id is none")
                return
            HttpHandlerParams().http_msg_queue.add_request_to_queue_map(
                    unique_id, 
                    [schedule_id,], 
                    self.__stop_task_call_back)
            
            response = HttpHandlerParams().http_msg_queue.get_response_from_map(
                            unique_id)
            self.write(response)
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.write("error:%s" % traceback.format_exc())
        except Exception as ex:
            logging.error(traceback.format_exc())
            self.write("error:%s" % traceback.format_exc())
        finally:
            self.finish()

    def __stop_task_call_back(self, param_list):
        try:
            schedule_id = param_list[0].strip()
            if not schedule_id:
                return "error:schedule_id is none"
            schedule = horae.models.Schedule.objects.get(id=schedule_id)
            task = horae.models.Task.objects.get(id=schedule.task_id)
            processor = horae.models.Processor.objects.get(id=task.pid)
            task_hanlder = HttpHandlerParams().get_task_handler(processor.type)
            if task_hanlder is None:
                return "wrong task type:%s" % processor.type
            sql_manager = node_sql_manager.SqlManager()
            task_info = sql_manager.get_task_info_with_schedule_id(
                    HttpHandlerParams().get_file_path_with_schedule_id(
                            schedule_id), 
                    schedule_id)
            if task_info is None:
                return "error:get task info form db error!"

            if not task_hanlder.stop_task(task_info):
                return "error: stop task failed!"

            if not sql_manager.stop_task_with_schedule_id(
                    int(schedule_id)):
                return 'error: stop task write db error: %s' % schedule_id

            return "OK"
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            return task_util.CONSTANTS.HTTP_RETRY_MSG
        except Exception as ex:
            logging.error(traceback.format_exc())
            return "error:%s" % traceback.format_exc()
