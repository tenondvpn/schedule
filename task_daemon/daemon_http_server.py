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
import base64
import json

import tornado.httpserver
import tornado.web
import tornado.ioloop

sys.path.append('../common')
import task_util

class DaemonHttpServer(object):
    '''
        admin的http服务
    '''
    def __init__(self, config, task_manager):
        self.__config = config
        self.__log = logging
        self.__http_port = config.get("daemon", "http_port")
        self.__http_param = HttpHandlerParams(task_manager)

    def start(self):
        self.__app = tornado.web.Application(handlers=[
                (r"/run_task", RunTaskHandler),
                (r"/stop_task", StopTaskHandler),
                (r"/get_task_status", GetTaskStatus),
            ])
        #self.__httpserver = tornado.httpserver.HTTPServer(self.__app)
        #self.__httpserver.listen(self.__http_port)
        #self.__httpserver.start()
        self.__app.listen(self.__http_port)
        self.__log.info("admin httpserver bind[%s:%s] started!" % (
                task_util.StaticFunction.get_local_ip(), 
                self.__http_port))
        tornado.ioloop.IOLoop.instance().start()

    def stop(self):
        #self.__httpserver.stop()
        #tornado.ioloop.IOLoop.instance().stop()
        ioloop = tornado.ioloop.IOLoop.instance()
        ioloop.add_callback(ioloop.stop)

        self.__log.info("admin httpserver bind[%s:%s] stopped!" % (
                task_util.StaticFunction.get_local_ip(), 
                self.__http_port))

class HttpHandlerParams(task_util.Singleton):
    def __init__(self, task_manager=None):
        if not hasattr(self, "a"):
            if task_manager is None:
                raise EnvironmentError("none:task_dispacher, node_http_port")

            self.task_manager = task_manager
            self.a = "a"  # 必须在后面

class RunTaskHandler(tornado.web.RequestHandler):
    #@tornado.web.asynchronous
    def get(self):
        try:
            schedule_id = self.get_argument("schedule_id", "").strip()
            if schedule_id == '':
                self.write("error:schedule_id is none")
                return
            schedule_id = int(schedule_id)

            cmd_base64 = self.get_argument("cmd", "").strip()
            if cmd_base64 == '':
                self.write("error:cmd_base64 is none")
                return

            stdout_file = self.get_argument("stdout", "").strip()
            if stdout_file == '':
                self.write("error:stdout_file is none")
                return

            stderr_file = self.get_argument("stderr", "").strip()
            if stderr_file == '':
                self.write("error:stderr_file is none")
                return

            over_time = self.get_argument("over_time", "").strip()
            if over_time == '':
                over_time = 0
            else:
                over_time = int(over_time)

            expect_ret = self.get_argument("exp_ret", "").strip()
            if expect_ret == '':
                expect_ret = 0
            else:
                expect_ret = int(expect_ret)

            cmd = str(base64.b64decode(cmd_base64.encode('utf-8')), 'utf-8')

            self.__task_manager = HttpHandlerParams().task_manager
            if not self.__task_manager.run_task(
                    schedule_id, 
                    cmd, 
                    stdout_file, 
                    stderr_file,
                    over_time,
                    expect_ret):
                self.write("error:run_task failed!")
                return
            self.write("OK")
        except Exception as ex:
            logging.error("run task failed![ex:%s][trace:%s]" % (
                    str(ex), traceback.format_exc()))
            self.write('error:' + str(ex) + traceback.format_exc())
        finally:
            self.finish()

class StopTaskHandler(tornado.web.RequestHandler):
    #@tornado.web.asynchronous
    def get(self):
        try:
            schedule_id = self.get_argument("schedule_id", "").strip()
            if schedule_id == '':
                self.write("error: schedule_id is none")
                return
            schedule_id = int(schedule_id)
            HttpHandlerParams().task_manager.stop_task(schedule_id)
            self.write("OK")
        except Exception as ex:
            logging.error("stop task failed![ex:%s][trace:%s]" % (
                    str(ex), traceback.format_exc()))
            self.write('error:' + str(ex) + traceback.format_exc())
        finally:
            self.finish()

class GetTaskStatus(tornado.web.RequestHandler):
    #@tornado.web.asynchronous
    def get(self):
        try:
            schedule_id = self.get_argument("schedule_id", "").strip()
            if schedule_id == '':
                self.write("error:schedule_id is none")
                return
            schedule_id = int(schedule_id)
            status, ret = HttpHandlerParams().task_manager.get_task_status(
                    schedule_id)
            if status is None:
                status = task_util.TaskState.TASK_FAILED

            res_data = {
                "status": status,
                "ret": ret
            }
            self.write(json.dumps(res_data).encode('utf-8'))
            logging.info("get task status stuccess schedule_id: %d, status: %d, ret: %d" % (schedule_id, status, ret))
        except Exception as ex:
            logging.error("get task status failed![ex:%s][trace:%s]" % (
                    str(ex), traceback.format_exc()))
            self.write('error:' + str(ex) + traceback.format_exc())
        finally:
            self.finish()
