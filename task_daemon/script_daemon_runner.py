###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import sys
import os
import traceback
import time
import logging
import threading
import subprocess
import signal

sys.path.append('../common')
import task_util

class ScriptDaemonHandler(threading.Thread):
    """
        启动script任务，并等待，直到脚本任务运行完成
        每一种任务都需要重新创建实例，线程不安全
    """
    def __init__(
            self, 
            config, 
            cmd, 
            stdout_file, 
            std_err_file, 
            over_time, 
            exp_ret):
        threading.Thread.__init__(self)
        self.__status = None
        self.__error_log_list = []
        self.__log = logging
        self.__stop_task = False
        self.__cmd = cmd
        self.__stdout_file = stdout_file
        self.__stderr_file = std_err_file
        self.__finish_time = None
        self.__over_time = over_time
        self.__exp_ret = exp_ret
        self.__final_ret = None

    def run(self):
        try:
            self.__status = task_util.TaskState.TASK_RUNNING
            if not self.__run_task():
                self.__add_error_log("__run_task failed[%s]" % self.__cmd)
            self.__write_error_log_to_file(self.__stderr_file)
            self.__finish_time = time.time()
        except Exception as ex:
            self.__log.error("run python job failed![ex:%s][%s]" % (
                    str(ex), traceback.format_exc()))

    def get_finish_time(self):
        return self.__finish_time

    def get_task_status(self):
        return self.__status, self.__final_ret

    def stop_task(self):
        self.__stop_task = True
        # 等待线程退出
        while self.__finish_time is None:
            time.sleep(1)
            continue

    def __run_task(self):
        # 执行脚本任务并等待，直到脚本退出
        stdout_fd = open(self.__stdout_file, "a")
        stderr_fd= open(self.__stderr_file, "a")
        try:
            sub_pro = subprocess.Popen(
                    self.__cmd, 
                    shell=True, 
                    stdout=stdout_fd, 
                    stderr=stderr_fd,
                    close_fds=True, 
                    preexec_fn=os.setsid)
            begin_time = time.time()
            while not self.__stop_task: 
                if task_util.CONSTANTS.GLOBAL_STOP:
                    # 等待直到任务停止，如果kill -9,会直接退出
                    self.__log.error("task has not finished ,will wait...")

                ret = subprocess.Popen.poll(sub_pro) 
                if ret is not None: 
                    self.__final_ret = ret
                    # if ret != self.__exp_ret:
                    #     self.__log.warn("task has finished but "
                    #             "error ret code:%d" % ret)
                    #     self.__status = task_util.TaskState.TASK_FAILED
                    #     return False
                    self.__status = task_util.TaskState.TASK_SUCCEED
                    return True

                if self.__over_time != 0 \
                        and time.time() - begin_time >= self.__over_time \
                        and self.__status != task_util.TaskState.TASK_TIMEOUT:
                    self.__status = task_util.TaskState.TASK_TIMEOUT
                    self.__log.warn("stopped by user![cmd:%s]" % self.__cmd)
                time.sleep(1)
            # 用户停止
            self.__status = task_util.TaskState.TASK_STOPED_BY_USER
            # 经过测试，odps任务，scrip任务terminate后会终止任务，无需额外kill
            os.killpg(sub_pro.pid, signal.SIGTERM)
            self.__log.warn("stopped by user![cmd:%s]" % self.__cmd)
            return True
        except:
            stderr_fd.write(traceback.format_exc())
            self.__add_error_log("run python job failed![%s]" % \
                    traceback.format_exc())
            self.__status = task_util.TaskState.TASK_FAILED
            return False
        finally:
            stdout_fd.close()
            stderr_fd.close()
        return True

    def __add_error_log(self, log_str):
        if log_str is not None and log_str != '':
            self.__log.error(log_str)
            self.__error_log_list.append(log_str)

    def __write_error_log_to_file(self, file_name):
        if len(self.__error_log_list) > 0:
            error_log = '\n'.join(self.__error_log_list)
            if task_util.StaticFunction.write_content_to_file(
                    file_name, 
                    error_log,
                    'a') != task_util.FileCommandRet.FILE_COMMAND_SUCC:
                return False
        return True

if __name__ == "__main__":
    print("please run unit test in common/db_manabger")
