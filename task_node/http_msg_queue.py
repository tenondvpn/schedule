###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import sys
import threading
import queue
import time
import logging
import traceback
import copy

sys.path.append('../common')
import task_util
        
global_queue_lock = threading.Lock()
class HttpRequestManager(threading.Thread):
    """
        1.管理长等待的http请求
        2.多线程执行http请求，并保存执行结果
    """
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.__log = logging
        self.__config = config
        self.__request_queue = queue.Queue()
        self.__thread_num = int(config.get("node", "http_thread_num"))
        self.__unique_id_map = {}
        self.__request_handler_threads = []
        self.__create_request_handler()

    def run(self):
        global global_queue_lock
        self.__log.info("HttpRequestHandler thread starting...")
        self.__run_all_request_handler()
        # 全局退出标示字段
        while not task_util.CONSTANTS.GLOBAL_STOP:
            begin_time = time.time()
            try:
                self.__check_overtime()
            except Exception as ex:
                self.__log.error("handle request failed![ex:%s][trace: %s]" % (
                        str(ex), traceback.format_exc()))

            use_time = time.time() - begin_time
            self.__log.info("HttpRequestHandler handle data "
                    "exit.use time[%f]" % use_time)
            time.sleep(10)

        self.__log.info("HttpRequestHandler wait ready task handler exit!")
        self.__join_all_request_handler()
        self.__log.info("HttpRequestHandler thread exited!")

    def get_request_from_queue_map(self):
        _ = task_util.AutoLock(global_queue_lock)
        # global_queue_lock.acquire()
        try:
            if not self.__request_queue.empty():
                return self.__request_queue.get()
        except:
            pass
        # finally:
            # global_queue_lock.release()
        return None

    def add_request_to_queue_map(self, unique_id, param_list, handle_func):
        self.__log.info("add_request_to_queue_map 0")
        _ = task_util.AutoLock(global_queue_lock)
        # global_queue_lock.acquire()
        self.__log.info("add_request_to_queue_map 1")
        if unique_id in self.__unique_id_map:
            # global_queue_lock.release()
            self.__log.info("add_request_to_queue_map 2")
            return

        self.__request_queue.put((unique_id, param_list, handle_func))
        self.__log.info("add_request_to_queue_map 3")
        self.__unique_id_map[unique_id] = (
                task_util.CONSTANTS.HTTP_RESPONSE_WAIT,
                time.time())
        self.__log.info("add_request_to_queue_map 4")
        # global_queue_lock.release()
        self.__log.info("add_request_to_queue_map 5")

    def get_response_from_map(self, unique_id):
        _ = task_util.AutoLock(global_queue_lock)
        self.__log.info("get_response_from_map 0")
        # global_queue_lock.acquire()
        self.__log.info("get_response_from_map 1")
        try:
            if unique_id not in self.__unique_id_map:
                return "error: not handler this id:%s" % unique_id

            self.__log.info("get_response_from_map 2")
            return self.__unique_id_map[unique_id][0]
        except:
            pass
        # finally:
            # global_queue_lock.release()
            
        self.__log.info("get_response_from_map 3")

    def set_response_to_map(self, unique_id, response):
        _ = task_util.AutoLock(global_queue_lock)
        # global_queue_lock.acquire()
        self.__unique_id_map[unique_id] = (response, time.time())
        # global_queue_lock.release()

    def __check_overtime(self):
        _ = task_util.AutoLock(global_queue_lock)
        # global_queue_lock.acquire()
        now_time = time.time()
        keys = []
        for key in self.__unique_id_map.keys():
            keys.append(key)

        for key in keys:
            if now_time - self.__unique_id_map[key][1] >= 180:
                del self.__unique_id_map[key]
        # global_queue_lock.release()

    def __create_request_handler(self):
        for i in range(self.__thread_num):
            self.__request_handler_threads.append(
                    RequstHandler(self.__config, self, i))

    def __run_all_request_handler(self):
        for handler in self.__request_handler_threads:
            handler.start()
    
    def __join_all_request_handler(self):
        for handler in self.__request_handler_threads:
            handler.join()

class RequstHandler(threading.Thread):
    def __init__(self, config, request_mgr, thread_id):
        threading.Thread.__init__(self)
        self.__log = logging
        self.__config = config
        self.__request_mgr = request_mgr
        self.__thread_id = thread_id

    def run(self):
        self.__log.info("ReadyTaskHandler thread starting...")
        begin_time = time.time()
        # 全局退出标示字段
        while not task_util.CONSTANTS.GLOBAL_STOP:
            if time.time() - begin_time >= 60:
                begin_time = time.time()
                self.__log.info("thread num: %s is running" % self.__thread_id)

            request = self.__request_mgr.get_request_from_queue_map()
            if request is None:
                time.sleep(1)
                continue

            try:
                handle_ret = task_util.CONSTANTS.HTTP_RETRY_MSG
                retry_count = 3
                retried_count = 0
                while retried_count < retry_count:
                    handle_ret = request[2](request[1])
                    if handle_ret == task_util.CONSTANTS.HTTP_RETRY_MSG:
                        retried_count += 1
                        continue
                    break

                self.__request_mgr.set_response_to_map(
                        request[0], 
                        handle_ret)
            except Exception as ex:
                error = "handle task failed![ex:%s][trace: %s]" % (
                        str(ex), traceback.format_exc())
                self.__log.error(error)
                self.__request_mgr.set_response_to_map(request[0], error)
        
        self.__log.info("ReadyTaskHandler thread end!")
