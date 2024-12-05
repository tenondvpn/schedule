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

import horae.models
import django.db

sys.path.append('../common')
import task_util
import node_sql_manager
import script_task_handler
import odps_sql_task_handler
import check_limit_num
#import oozie_task_handler
import docker_task_handler
import clickhouse_task_handler
import v100_script_task_handler
import shell_task_handler

class ReadyTasksCreator(threading.Thread):
    """
        1.遍历ready_task表格，获取可以执行的任务
        2.管理优先队列
    """
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.__sql_manager = node_sql_manager.SqlManager()
        self.__log = logging
        self.__config = config
        self.__priority_queue_map = {}
        self.__init_priority_queue()
        self.__lock = threading.Lock()
        self.__queue_lock = threading.Lock()
        self.__ready_task_set = set()
        self.__thread_num = int(config.get("node", "thread_num"))
        self.__ready_task_handler_threads = []
        self.__init_priority_queue()
        self.__create_ready_task_handler()
        self.__schedule_tasks = {}
        self.__v100_lock = threading.Lock()
        self.__v100_pending_db_tasks = {}

    def run(self):
        self.__log.info("HandlerReadyTasks thread starting...")
        self.__run_all_ready_task_handler()
        # 全局退出标示字段
        while not task_util.CONSTANTS.GLOBAL_STOP:
            begin_time = time.time()
            self.__log.info(
                    "HandlerReadyTasks handle data starting"
                    "[now tasks:%s]" % len(self.__ready_task_set))
            try:
                self.__handle_all_ready_tasks()
                self.__get_all_v100_readytasks()
            except Exception as ex:
                self.__log.error("handle task failed![ex:%s][trace: %s]" % (
                        str(ex), traceback.format_exc()))

            use_time = time.time() - begin_time
            self.__log.info("HandlerReadyTasks handle data "
                    "exit.use time[%f]" % use_time)
            # 从临时表ready_task中获取数据，db压力不大，可以减小休眠时间
            time.sleep(10)

        self.__log.info("HandlerReadyTasks wait ready task handler exit!")
        self.__join_all_ready_task_handler()
        self.__log.info("HandlerReadyTasks thread exited!")

    def set_schedule_timeout(self, schedule_id):
        ret = True
        self.__queue_lock.acquire()
        if schedule_id not in self.__schedule_tasks or self.__schedule_tasks[schedule_id][1]:
            ret = False
        else:
            self.__schedule_tasks[schedule_id][1] = True

        self.__queue_lock.release()
        return ret
    
    def init_schedule_timeout(self, schedule_id, schedule_time):
        self.__queue_lock.acquire()
        self.__schedule_tasks[schedule_id] = [schedule_time,False]
        self.__queue_lock.release()

    def delete_invalid_schedule(self, valid_schedule_id):
        self.__queue_lock.acquire()
        now_ids = []
        for id in self.__schedule_tasks:
            now_ids.append(id)

        for id in now_ids:
            if id not in valid_schedule_id or self.__schedule_tasks[id][0] != valid_schedule_id[id]:
                del self.__schedule_tasks[id]

        self.__queue_lock.release()

    def get_one_task_from_queue_map(self):
        # task_util.AutoLock(self.__queue_lock)
        self.__log.info("now get task info")
        self.__queue_lock.acquire()
        try:
            for priority_num in range(
                    task_util.CONSTANTS.TASK_PRIORITY_MAX, 
                    task_util.CONSTANTS.TASK_PRIORITY_MIN - 1,
                    -1):
                if self.__priority_queue_map[priority_num].empty():
                    continue

                return self.__priority_queue_map[priority_num].get()
        except:
            pass
        finally:
            self.__queue_lock.release()
            self.__log.info("success get task info")

        return None

    def del_one_task_from_set(self, schedule_id):
        # task_util.AutoLock(self.__lock)
        self.__lock.acquire()
        if schedule_id in self.__ready_task_set:
            self.__ready_task_set.remove(schedule_id)

        self.__lock.release()

    def can_add_v100_pending_task(self, schedule_id):
        auto_lock = task_util.AutoLock(self.__v100_lock)
        if schedule_id in self.__v100_pending_db_tasks:
            return True
        
        if len(self.__v100_pending_db_tasks) >= task_util.CONSTANTS.V100_MAX_PENDING_COUNT:
            return False
        
        self.__v100_pending_db_tasks[schedule_id] = False
        return True
    
    def add_v100_pending_task(self, schedule_id):
        auto_lock = task_util.AutoLock(self.__v100_lock)
        self.__v100_pending_db_tasks[schedule_id] = True

    def __get_all_v100_readytasks(self):
        v100_tasks = horae.models.ReadyTask.objects.filter(status__in=[
            task_util.TaskState.TASK_V100_PENDING,
            task_util.TaskState.TASK_RUNNING], type=8)
        v100_pending_db_tasks = {}
        for task in v100_tasks:
            if task.status == task_util.TaskState.TASK_V100_PENDING or task.status == task_util.TaskState.TASK_RUNNING:
                v100_pending_db_tasks[task.schedule_id] = True

        auto_lock = task_util.AutoLock(self.__v100_lock)
        self.__v100_pending_db_tasks = v100_pending_db_tasks

    def __add_one_task_into_set(self, schedule_id):
        # task_util.AutoLock(self.__lock)
        self.__lock.acquire()
        if schedule_id not in self.__ready_task_set:
            self.__ready_task_set.add(schedule_id)
            self.__lock.release()
            return True

        self.__lock.release()

        return False

    def __add_one_task_into_queue_map(self, task_info):
        # task_util.AutoLock(self.__queue_lock)
        self.__log.info("now put task info: %d" % task_info[0])
        self.__queue_lock.acquire()
        self.__priority_queue_map[task_info[9]].put(task_info)
        self.__queue_lock.release()
        self.__log.info("finish put task info: %d" % task_info[0])

    def __init_priority_queue(self):
        for priority_num in range(
                task_util.CONSTANTS.TASK_PRIORITY_MIN, 
                task_util.CONSTANTS.TASK_PRIORITY_MAX + 1):
            # 对Queue的操作是线程安全的, 不需要加锁
            self.__priority_queue_map[priority_num] = queue.Queue()

    def __create_ready_task_handler(self):
        for i in range(self.__thread_num):
            self.__ready_task_handler_threads.append(
                    ReadyTaskHandler(self.__config, self, i))

    def __run_all_ready_task_handler(self):
        for handler in self.__ready_task_handler_threads:
            handler.start()
    
    def __join_all_ready_task_handler(self):
        for handler in self.__ready_task_handler_threads:
            handler.join()

    def __handle_all_ready_tasks(self):
        local_public_ip = self.__config.get("node", "public_ip")
        if local_public_ip is None or local_public_ip.strip() == "":
            local_public_ip = task_util.StaticFunction.get_local_ip()

        ready_tasks = self.__sql_manager.get_all_ready_tasks(local_public_ip)
        if ready_tasks is None:
            self.__log.info("get local public ip: %s, ready tasks: %d" % (local_public_ip, 0))
        else:
            self.__log.info("get local public ip: %s, ready tasks: %d" % (local_public_ip, len(ready_tasks)))

        if ready_tasks is None or len(ready_tasks) <= 0:
            return True

        valid_schedule_id = {}
        for task_info in ready_tasks:
            if task_info[9] < task_util.CONSTANTS.TASK_PRIORITY_MIN \
                    or task_info[9] > task_util.CONSTANTS.TASK_PRIORITY_MAX:
                self.__log.error("task priority must in[%d, %d] but now %d" % (
                        task_util.CONSTANTS.TASK_PRIORITY_MIN, 
                        task_util.CONSTANTS.TASK_PRIORITY_MAX, 
                        task_info[9]))
                continue

            # 只有一个线程生成，分为两步不会有问题
            valid_schedule_id[task_info[2]] = task_info[19]
            if self.__add_one_task_into_set(task_info[2]):
                if task_info[2] not in self.__schedule_tasks:
                    self.init_schedule_timeout(task_info[2],task_info[19])
                self.__add_one_task_into_queue_map(task_info)

        self.delete_invalid_schedule(valid_schedule_id)
        return True

class ReadyTaskHandler(threading.Thread):
    """
        1.修改可执行任务的状态，包括schedule，ready_task，run_history
        2.分析任务，并执行（直到执行完成）
    """
    def __init__(self, config, task_creator, thread_id):
        threading.Thread.__init__(self)
        self.__log = logging
        self.__config = config
        self.__task_creator = task_creator
        self.__sql_manager = node_sql_manager.SqlManager()
        self.__check_limit_num = check_limit_num.CheckLimitNum()
        self.__thread_id = thread_id

    def run(self):
        self.__log.info("ReadyTaskHandler thread starting...")
        begin_time = time.time()
        # 全局退出标示字段
        while not task_util.CONSTANTS.GLOBAL_STOP:
            if time.time() - begin_time >= 60:
                begin_time = time.time()
                self.__log.info("thread num: %s is running" % self.__thread_id)

            task_info = self.__task_creator.get_one_task_from_queue_map()
            self.__log.info("now get task from queue 0")
            if task_info is None:
                self.__log.info("now get task from queue 1")
                time.sleep(10)
                continue

            self.__log.info("now get task from queue 2")
            if len(task_info) < \
                    node_sql_manager.ConstantSql.ALL_READY_TASK_SQL_PARAM_NUM:
                self.__log.error("start task function param task_info must "
                        "has %d params,but now[%d]" % (
                        node_sql_manager.ConstantSql.\
                        ALL_READY_TASK_SQL_PARAM_NUM, 
                        len(task_info)))
                self.__task_creator.del_one_task_from_set(task_info[2])
                self.__log.info("now get task from queue 3")
                continue

            self.__log.info("now get task from queue 4")
            if task_info[12] == task_util.TaskState.TASK_READY:
                if not self.__check_limit_num.check_limit_num_can_run(
                        task_info[14], 
                        task_info[3]):
                    self.__task_creator.del_one_task_from_set(task_info[2])
                    self.__log.info("now get task from queue 5")
                    time.sleep(1)
                    continue

            self.__log.info("now get task from queue 6")
            if not self.__check_task_status(task_info):
                self.__task_creator.del_one_task_from_set(task_info[2])
                self.__log.info("now get task from queue 7")
                continue

            self.__log.info(
                    "handle task begin schedule_id: %s, thread_id: %s" % (
                    str(task_info), self.__thread_id))
            try:
                self.__handle_ready_task(task_info)
            except Exception as ex:
                self.__log.error("handle task failed![ex:%s][trace: %s]" % (
                        str(ex), traceback.format_exc()))

            self.__log.info("now get task from queue 8")
            if task_info[12] == task_util.TaskState.TASK_READY:
                time.sleep(1)
                self.__check_limit_num.del_wait_running_limit_num(
                        task_info[14], 
                        task_info[3])

            self.__task_creator.del_one_task_from_set(task_info[2])
            self.__log.info("now get task from queue 9")
            self.__log.info(
                    "handle task succ schedule_id: %s, thread_id: %s" % (
                    str(task_info), self.__thread_id))

        self.__log.info("ReadyTaskHandler thread exited!")

    def __check_task_status(self, task_info):
        tried_num = 0
        while tried_num < 10:
            try:
                readytask = horae.models.ReadyTask.objects.get(id=task_info[0])
                if int(task_info[12]) != int(readytask.status):
                    return False
                return True
            except django.db.OperationalError as ex:
                django.db.close_old_connections()
                self.__log.error("__check_task_status failed![ex:%s][trace:%s]!" % (
                        str(ex),
                        traceback.format_exc()))
                tried_num += 1
                time.sleep(1)
                continue
            except Exception as ex:
                self.__log.error("__check_task_status "
                        " failed![ex:%s][trace:%s]!" % (
                        str(ex), traceback.format_exc()))
                return False

    def __get_task_handler(self, task_type):
        if task_type == task_util.TaskType.SCRIPT_TYPE:
            return script_task_handler.ScriptTaskHandler(
                    self.__config, self.__task_creator)
        elif task_type == task_util.TaskType.SPARK_TYPE:
            return script_task_handler.ScriptTaskHandler(
                    self.__config, self.__task_creator)
        #elif task_type == task_util.TaskType.OOZIE_TYPE:
        #    return oozie_task_handler.OozieTaskHandler(
        #            self.__config)
        elif task_type == task_util.TaskType.ODPS_TYPE:
            return odps_sql_task_handler.OdpsSqlTaskHandler(
                    self.__config)
        elif task_type == task_util.TaskType.SHELL_TYPE:
            return shell_task_handler.ShellTaskHandler(
                    self.__config, self.__task_creator)
        elif task_type == task_util.TaskType.DOCKER_TYPE:
            return docker_task_handler.DockerTaskHandler(
                    self.__config)
        elif task_type == task_util.TaskType.CLICKHOUSE_TYPE:
            return clickhouse_task_handler.ClickhouseTaskHandler(
                    self.__config)
        elif task_type == task_util.TaskType.V100_TYPE:
            return v100_script_task_handler.V100ScriptTaskHandler(
                    self.__config, self.__task_creator)
        else:
            pass

        return None

    def __handle_ready_task(self, task_info):
        task_handler = self.__get_task_handler(task_info[3])
        if task_handler is None:
            self.__log.error("error task type[%d]" % task_info[3])
            return False

        status = int(task_info[12])
        if status == task_util.TaskState.TASK_READY:
            if not task_handler.run_task(task_info):
                self.__log.warn("run_task failed[%s]" % str(task_info))
                return False
        elif status == task_util.TaskState.TASK_TIMEOUT:
            if not task_handler.get_task_status(task_info):
                self.__log.warn("get_task_status failed task "
                        "info[%s]" % str(task_info))
                return False
        elif status == task_util.TaskState.TASK_RUNNING or status == task_util.TaskState.TASK_V100_PENDING or status == task_util.TaskState.TASK_V100_RUNING:
            task_handler.get_task_status(task_info)
            return True
        else:
            self.__log.error("wrong status task info[%s][%d]" % (
                    str(task_info), status))
            return False

        return True
