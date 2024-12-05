###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import sys
import threading
import time
import logging
import traceback

import script_daemon_runner
sys.path.append('../common')
import task_util

class TaskManager(threading.Thread):
    """
        1.脚本执行任务的管理，会保存其状态，如果进程异常退出，
        会损失所有任务状态，所以task_node需要及时获取任务状态，避免错误
        2.线程会定期清理任务状态，过期后task_node将无法获取任务状态信息
    """
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.__log = logging
        self.__config = config
        self.__lock = threading.Lock()
        self.__running_task_set = set()
        self.__task_instance_map = {}
        self.__clear_time = float(config.get("daemon", "clear_time"))

    def run(self):
        self.__log.info("TaskManager thread starting...")
        # 全局退出标示字段
        while not task_util.CONSTANTS.GLOBAL_STOP:
            begin_time = time.time()
            self.__log.info("TaskManager handle data starting...")
            try:
                self.__check_finish_time()
            except Exception as ex:
                self.__log.error(
                        "__check_finish_time failed![ex:%s][trace:%s]!" % (
                        str(ex), traceback.format_exc()))
            use_time = time.time() - begin_time
            self.__log.info("TaskManager exit.use time[%f]" % use_time)
            time.sleep(10)

        self.__wait_daemon_handler()
        self.__log.info("TaskManager thread exited!")

    def run_task(
            self, 
            schedule_id, 
            cmd, 
            stdout_file, 
            std_err_file, 
            over_time, 
            exp_ret):
        # task_util.AutoLock(self.__lock)
        self.__lock.acquire()
        if schedule_id in self.__task_instance_map:
            self.__log.error("schedule_id has in "
                    "__task_instance_map[%s]" % schedule_id)
            self.__lock.release()
            return False
        daemon_handler = script_daemon_runner.ScriptDaemonHandler(
                self.__config, 
                cmd, 
                stdout_file, 
                std_err_file,
                over_time,
                exp_ret)
        self.__task_instance_map[schedule_id] = daemon_handler
        daemon_handler.start()
            
        self.__lock.release()
        return True

    def stop_task(self, schedule_id):
        # task_util.AutoLock(self.__lock)
        self.__lock.acquire()
        if schedule_id in self.__task_instance_map:
            self.__task_instance_map[schedule_id].stop_task()
            del self.__task_instance_map[schedule_id]
            self.__lock.release()
            return True
        self.__lock.release()
        return False

    def get_task_status(self, schedule_id):
        # task_util.AutoLock(self.__lock)
        self.__lock.acquire()
        try:
            if schedule_id in self.__task_instance_map:
                return self.__task_instance_map[schedule_id].get_task_status()
        except:
            pass
        finally:
            self.__lock.release()
        return None

    def __check_finish_time(self):
        # task_util.AutoLock(self.__lock)
        self.__lock.acquire()
        now_time = time.time()
        map_keys = self.__task_instance_map.keys()
        for schedule_id in map_keys:
            end_time = self.__task_instance_map[schedule_id].get_finish_time()
            if end_time is None:
                continue

            if now_time - end_time >= self.__clear_time:
                del self.__task_instance_map[schedule_id]
                self.__lock.release()
                return  # 迭代器已被破坏，等待线程下次调度
            
        self.__lock.release()

    def __wait_daemon_handler(self):
        # task_util.AutoLock(self.__lock)
        self.__lock.acquire()
        map_keys = self.__task_instance_map.keys()
        for schedule_id in map_keys:
            end_time = self.__task_instance_map[schedule_id].get_finish_time()
            # 等待线程安全退出，此处不能用join
            while end_time is None:
                self.__log.info("wait schedule [%s] to exit!" % schedule_id)
                time.sleep(1)
                continue
        self.__lock.release()
