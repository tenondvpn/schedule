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
import urllib.request
import base64
import json
import datetime

import task_handle_base
import node_sql_manager

sys.path.append('../common')
import task_util
import no_block_sys_cmd

class ShellTaskHandler(task_handle_base.TaskHandleBase):
    """
        启动shell任务，非阻塞
        每一种任务都需要重新创建实例，线程不安全
    """
    def __init__(self, config, task_creator):
        task_handle_base.TaskHandleBase.__init__(self, config)
        self.__daemon_port = config.get("node", "daemon_port")
        self.__local_ip = task_util.StaticFunction.get_local_ip()
        self.__shell_cmd = "bash "
        self.__no_block_cmd = no_block_sys_cmd.NoBlockSysCommand()

    def run_task(self, task_info):
        self._job_status = task_util.TaskState.TASK_FAILED
        self._old_job_status = task_util.TaskState.TASK_READY
        self._task_handler = ""
        while True:
            if not self._init_task(task_info):
                err_log = ("init task failed!")
                self._log.error(err_log)
                self._add_error_log(err_log)
                break

            if self._task_type != task_util.TaskType.SHELL_TYPE:
                err_log = ("this is just for shell [%d] but now[%d]" %
                           (task_util.TaskType.SHELL_TYPE, self._task_type))
                self._log.error(err_log)
                self._add_error_log(err_log)
                break

            # 准备工作路径
            self.__job_work_dir = self._prepair_work_dir()
            self._add_error_log("work_ip: %s\nwork_dir: %s\n" % (
                task_util.StaticFunction.get_local_ip(),
                self.__job_work_dir))
            try:
                self._download_package(self.__job_work_dir)
            except:
                pass

            # 初始化，包括配置中的时间转化，下载运行包，
            if not self._init(self.__job_work_dir):
                err_log = ("shell job handle config failed!")
                self._log.error(err_log)
                self._add_error_log(err_log)
                break

            # run.conf.tpl转化为run.conf
            if not self.__handle_run_conf():
                break

            # 替换template为sql
            if self._template is not None or self._template.strip() !='':
                shell_command = task_util.StaticFunction.replace_str_with_regex(
                    self._template, 
                    self._conf_replace_pattern, 
                    self._config_map)
                if shell_command is None:
                    err_log = (
                            "replace tpl content[%s] failed!" % self._template)
                    self._log.error(err_log)
                    self._add_error_log(err_log)
                    break

                if shell_command.endswith(';'):
                    shell_command = shell_command[0: len(shell_command) - 1]

                self._log.info("add command to sh: " + shell_command)
                task_util.StaticFunction.write_content_to_file(self.__job_work_dir + "/run.sh", shell_command)

            self.__handle_run_shell()

            # 先停止之前的任务
            self.__stop_task(task_info)

            ret = self.__run_job()
            # 此时表示线程退出，不要修改db状态
            if ret is None:
                return True

            if not ret:
                break

            self._job_status = task_util.TaskState.TASK_RUNNING
            break  # break for while True

        # 执行成功后，改写相关db数据
        # 如果写db失败了，这个时候可能导致用户数据和任务的运行情况不一致
        # 需要人工介入修改db状态
        if not self._write_task_status_to_db(
                self._job_status,
                self._old_job_status,
                task_handler=self._task_handler,
                work_dir=self.__job_work_dir):
            err_log = ("write_start_task_status_to_db failed!")
            self._log.warn(err_log)
            self._add_error_log(err_log)

        if self.__job_work_dir is None:
            return False

        err_log_file = os.path.join(
            self.__job_work_dir,
            "trace.log")
        self._write_error_log_to_file(err_log_file)
        return self._job_status == task_util.TaskState.TASK_RUNNING

    def stop_task(self, task_info):
        if not self._init_task(task_info):
            return False
        
        self.__job_work_dir = self._get_work_dir()
        if self.__job_work_dir is None:
            return task_util.TaskState.TASK_FAILED

        if not self._init(self.__job_work_dir):
            err_log = ("shell job handle config failed!")
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False

        ret = self.__stop_shell_task(task_info)
        err_log_file = os.path.join(
            self.__job_work_dir,
            "trace.log")
        self._write_error_log_to_file(err_log_file)
        return ret

    def get_task_status(self, task_info):
        if not self._init_task(task_info):
            return False

        self.__job_work_dir = self._get_work_dir()
        if self.__job_work_dir is None:
            return False

        if not self._init(self.__job_work_dir):
            err_log = ("shell job handle config failed!")
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False
        
        ret = task_util.TaskState.TASK_RUNNING
        ret_code = 0
        self._update_run_history_end_time(self._schedule_id)
        ret, ret_code = self.__get_shell_status(task_info)
        if ret in (
                task_util.TaskState.TASK_FAILED,
                task_util.TaskState.TASK_SUCCEED):
            if not self._write_task_status_to_db(
                    ret,
                    task_util.TaskState.TASK_RUNNING,
                    ret_code=ret_code):
                err_log = ("write_start_task_status_to_db failed!")
                self._log.warn(err_log)
                self._add_error_log(err_log)
                ret = task_util.TaskState.TASK_FAILED

        err_log_file = os.path.join(
            self.__job_work_dir,
            "trace.log")
        self._write_error_log_to_file(err_log_file)
        return True
        
    def __get_shell_status(self, task_info):
        return self.__get_task_status(task_info)
    
    def __stop_shell_task(self, task_info):
        self.__job_work_dir = self._get_work_dir()
        if self.__job_work_dir is None:
            return False

        task_handle_base.TaskHandleBase.stop_task(self)
        ret = self.__stop_task(task_info)
        err_log_file = os.path.join(
            self.__job_work_dir,
            "trace.log")
        self._write_error_log_to_file(err_log_file)
        return ret

    def __handle_run_shell(self):
        if 'shell_name' in self._config_map \
                and self._config_map['shell_name'].strip() != '' \
                and self._config_map['shell_name'].strip() != \
                task_util.CONSTANTS.SHELL_DEFULT_RUN_FILE:
            cmd = 'mv %s/%s %s/%s' % (
                self.__job_work_dir,
                self._config_map['shell_name'].strip(),
                self.__job_work_dir,
                task_util.CONSTANTS.SHELL_DEFULT_RUN_FILE)
            self.__no_block_cmd.run_once(cmd)

        if '_out' in self._config_map \
                and self._config_map['_out'].strip() != '':
            cmd = 'mv %s/%s %s/%s' % (
                self.__job_work_dir,
                'run.conf',
                self.__job_work_dir,
                self._config_map['_out'].strip())
            self.__no_block_cmd.run_once(cmd)

    def __handle_run_conf(self):
        # run.conf.tpl转化为run.conf
        tpl_in_file = os.path.join(
            self.__job_work_dir,
            task_util.CONSTANTS.SHELL_DEFSULT_TPL_CONF_NAME)
        if not os.path.exists(tpl_in_file):
            if task_util.CONSTANTS.TPL_CONFIG_NAME in self._config_map \
                    and self._config_map[
                    task_util.CONSTANTS.TPL_CONFIG_NAME].strip() != '':
                tpl_file = os.path.join(
                    self.__job_work_dir,
                    self._config_map[
                        task_util.CONSTANTS.TPL_CONFIG_NAME].strip())
                if os.path.exists(tpl_file):
                    cmd = 'mv %s %s' % (
                        tpl_file,
                        tpl_in_file)
                    self.__no_block_cmd.run_once(cmd)

        run_json_out_file = os.path.join(
            self.__job_work_dir,
            task_util.CONSTANTS.SHELL_DEFSULT_CONF_NAME)

        default_conf_content = '[run]\n'
        for key in self._config_map:
            default_conf_content += "%s = %s\n" % (key, self._config_map[key])

        if default_conf_content == '':
            return True

        if task_util.StaticFunction.write_content_to_file(
                run_json_out_file,
                default_conf_content) != \
                task_util.FileCommandRet.FILE_COMMAND_SUCC:
            err_log = ("write tpl file failed![%s]" %
                       run_json_out_file)
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False
        return True

    def __stop_task(self, task_info):
        daemon_req_url = ("http://%s:%s/stop_task?schedule_id=%s" % (
            self.__local_ip,
            self.__daemon_port,
            self._schedule_id))
        try:
            http_res = str(urllib.request.urlopen(daemon_req_url).read(), 'utf-8')
        except Exception as ex:
            err_log = ("daemon server failed[%s]" % daemon_req_url)
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False

        if http_res != 'OK':
            return False
        return True

    def __get_task_status(self, task_info):
        daemon_req_url = ("http://%s:%s/get_task_status?schedule_id=%s" % (
            self.__local_ip,
            self.__daemon_port,
            self._schedule_id))
        http_res = "error"
        try:
            http_res = str(urllib.request.urlopen(daemon_req_url).read(), 'utf-8')
        except Exception as ex:
            err_log = ("daemon server failed[%s]" % daemon_req_url)
            self._log.error(err_log)
            self._add_error_log(err_log)
            return task_util.TaskState.TASK_RUNNING, 0

        status = task_util.TaskState.TASK_FAILED
        json_res = json.loads(http_res)
        if type(json_res["status"]) == str:
            res_status = json_res["status"]
        else:
            res_status = str(json_res["status"])

        res_code = json_res["ret"]
        if res_status.startswith("error"):
            err_log = ("run task failed:%s[res:%s]" % (
                daemon_req_url, http_res))
            self._log.error(err_log)
            self._add_error_log(err_log)
        else:
            status = int(res_status)

        if status == task_util.TaskState.TASK_SUCCEED and res_code == 2:
            status = task_util.TaskState.TASK_FAILED

        if status == task_util.TaskState.TASK_TIMEOUT:
            err_log = ("task time out[%s]" % str(task_info))
            self._log.info(err_log)
            self._add_error_log(err_log)
            return task_util.TaskState.TASK_TIMEOUT, 0
        return status, res_code
    
    def __run_shell_job(self):
        prev_cmd = ""
        if "--prev_command" in self._config_map:
            prev_cmd = self._config_map["--prev_command"].strip()
            prev_cmd += " && "

        if not os.path.exists(self.__job_work_dir + task_util.CONSTANTS.SHELL_DEFULT_RUN_FILE):
            self._add_error_log("run file not exits: " + self.__job_work_dir + task_util.CONSTANTS.SHELL_DEFULT_RUN_FILE)
            return False

        cmd = prev_cmd + "cd %s && %s %s" % (
            self.__job_work_dir,
            self.__shell_cmd,
            task_util.CONSTANTS.SHELL_DEFULT_RUN_FILE)

        self._add_error_log(cmd)
        stdout_file = os.path.join(self.__job_work_dir, "stdout.log")
        stderr_file = os.path.join(self.__job_work_dir, "stderr.log")
        cmd_base64 = base64.b64encode(cmd.encode('utf-8'))
        daemon_req_url = ("http://%s:%s/run_task?"
                          "schedule_id=%s&cmd=%s&stdout=%s&"
                          "stderr=%s&expret=%d&over_time=%d" % (
                              self.__local_ip,
                              self.__daemon_port,
                              self._schedule_id,
                              str(cmd_base64, 'utf-8'),
                              stdout_file,
                              stderr_file,
                              int(self._except_ret),
                              int(self._over_time)))
        # 等待直到可以访问daemon服务
        while True:
            if task_util.CONSTANTS.GLOBAL_STOP:
                # 直接退出线程，不做额外处理
                return None

            try:
                http_res = str(urllib.request.urlopen(daemon_req_url).read(), 'utf-8')
                if http_res != 'OK':
                    self._log.error("run task failed:%s[res:%s]" % (
                        daemon_req_url, http_res))
                    err_log = ("run task failed:%s" % daemon_req_url)
                    self._log.error(err_log)
                    self._add_error_log(err_log)
                    return False
                return True
            except Exception as ex:
                err_log = ("daemon server fail[%s][ex:%s][trace:%s]" % (
                    daemon_req_url, str(ex), traceback.format_exc()))
                self._log.error(err_log)
                self._add_error_log(err_log)
                time.sleep(1)
                continue
    
    def __run_job(self):
        ret = self.__run_shell_job()
        self._log.info("run task ret: %d" % ret)
        return ret
