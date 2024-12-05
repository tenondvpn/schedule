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

import task_handle_base
sys.path.append('../common')
import task_util
import no_block_sys_cmd

class ClickhouseTaskHandler(task_handle_base.TaskHandleBase):
    """
        启动clickhouse-sql任务，非阻塞
        每一种任务都需要重新创建实例，线程不安全
    """
    def __init__(self, config):
        task_handle_base.TaskHandleBase.__init__(self, config)
        self.__ck_cmd = config.get("node", "ck_command")
        self.__job_work_dir = None
        self.__clickhouse_sql = None
        self.__no_block_cmd = no_block_sys_cmd.NoBlockSysCommand()

    def run_task(self, task_info):
        self._job_status = task_util.TaskState.TASK_FAILED
        self._old_job_status = task_util.TaskState.TASK_READY
        while True:
            if not self._init_task(task_info):
                err_log = ("init task failed!")
                self._log.error(err_log)
                self._add_error_log(err_log)
                break

            if self._task_type != task_util.TaskType.CLICKHOUSE_TYPE:
                err_log = (
                        "this is just for clickhouse_sql[%d],but now[%d]" % \
                        (task_util.TaskType.CLICKHOUSE_TYPE, self._task_type))
                self._log.error(err_log)
                self._add_error_log(err_log)
                break

            # 准备工作路径
            self.__job_work_dir = self._prepair_work_dir()
            self._add_error_log("work_ip: %s\nwork_dir: %s\n" % (
                    task_util.StaticFunction.get_local_ip(), 
                    self.__job_work_dir))

            # 初始化，包括配置中的时间转化，盘古路径处理，
            if not self._init(self.__job_work_dir):
                err_log = ("apsara job handle config failed!")
                self._log.error(err_log)
                self._add_error_log(err_log)
                break

            # 替换template为sql
            if self._template is None or self._template.strip() =='':
                err_log = ("clickhouse sql has no sql template!")
                self._log.warn(err_log)
                self._add_error_log(err_log)
                break
            self.__clickhouse_sql = task_util.StaticFunction.replace_str_with_regex(
                    self._template, 
                    self._conf_replace_pattern, 
                    self._config_map)
            if self.__clickhouse_sql is None:
                err_log = (
                        "replace tpl content[%s] failed!" % self._template)
                self._log.error(err_log)
                self._add_error_log(err_log)
                break
        
            # 执行clickhouse-sql，非阻塞执行，需要定时获取执行状态
            if not self.__run_job():
                err_log = ("script run job failed![%s]" % str(task_info))
                self._log.error(err_log)
                self._add_error_log(err_log)
                break

            self._job_status = task_util.TaskState.TASK_RUNNING
            break  # break for while True

        # 执行成功后，改写相关db数据
        # 如果写db失败了，这个时候可能导致用户数据和任务的运行情况不一致
        # 需要人工介入修改db状态
        if not self._write_task_status_to_db(
                self._job_status, 
                self._old_job_status,
                task_handler=None,
                work_dir=self.__job_work_dir):
            err_log = ("write_start_task_status_to_db failed!")
            self._log.warn(err_log)
            self._add_error_log(err_log)
            self._set_task_status_failed()

        if self.__job_work_dir is None:
            return False

        err_log_file = os.path.join(
                self.__job_work_dir, 
                "trace.log")
        self._write_error_log_to_file(err_log_file)
        return self._job_status == task_util.TaskState.TASK_RUNNING

    def stop_task(self, task_info):
        if not self._init_task(task_info):
            self._log.error("init task failed!")
            return False

        if not self._handle_config():
            self._log.error("handle config failed!")
            return False

        self.__job_work_dir = self._get_work_dir()
        if self.__job_work_dir is None:
            self._log.error("get work dir failed!")
            return False
        
        ret = self.__stop_task(task_info)
        err_log_file = os.path.join(
                self.__job_work_dir, 
                "trace.log")
        self._write_error_log_to_file(err_log_file)
        return ret

    def get_task_status(self, task_info):
        if not self._init_task(task_info):
            return task_util.TaskState.TASK_FAILED
 
        self._update_run_history_end_time(self._schedule_id)
        if not self._handle_config():
            return task_util.TaskState.TASK_FAILED

        self.__job_work_dir = self._get_work_dir()
        if self.__job_work_dir is None:
            return task_util.TaskState.TASK_FAILED
        ret = self.__get_task_status(task_info)
        err_log_file = os.path.join(
                self.__job_work_dir, 
                "trace.log")
        self._write_error_log_to_file(err_log_file)
        return ret

    def get_proceeding(self, task_info):
        if not self._init_task(task_info):
            return False
 
        if not self._handle_config():
            return False

        self.__job_work_dir = self._get_work_dir()
        if self.__job_work_dir is None:
            return False
        ret = self.__get_proceeding()
        err_log_file = os.path.join(
                self.__job_work_dir, 
                "trace.log")
        self._write_error_log_to_file(err_log_file)
        return ret

    def __get_proceeding(self):
        return "running"

    def __get_task_status(self, task_info):
        status = task_util.TaskState.TASK_SUCCEED
        if status == task_util.TaskState.TASK_RUNNING:
            return task_util.TaskState.TASK_RUNNING
        
        if not self._write_task_status_to_db(
                status, 
                task_util.TaskState.TASK_RUNNING):
            err_log = ("write_start_task_status_to_db failed!")
            self._log.warn(err_log)
            self._add_error_log(err_log)
            status = task_util.TaskState.TASK_FAILED
            self._set_task_status_failed()
        return status

    def __stop_task(self, task_info):
        task_handle_base.TaskHandleBase.stop_task(self)
        return True

    # TODO：确认下clickhouse通过ret_code是否可以确定任务运行成功，
    #       如果可以则不用检查instance的status
    # clickhousecmd [OPTION]
    # 参数    说明
    # --help/-h                       显示clickhousecmd的帮助信息
    # --project=<prj_name>            指定登录clickhousecmd后进入的默认project
    # --endpoint=<http://host:port>   设置clickhousecmd访问的clickhouse server的地址
    #                                 （仅用于debug）
    # -u <user_name> -p <password>    输入accessid(user_name)和
    #                                 accesskey(password)
    # -M  以CSV格式显示返回结果
    # -e <"command;[command;]...">    串行执行console、安全命令或是sql、
    #                                 DT、MR语句，执行结束后退出clickhousecmd。
    #                                 -e后面可以接多个命令，需要用”;”分割。
    #                                 当某个命令执行失败后会中止，
    #                                 不再执行后面的命令。
    # -f <"file_path;">               串行执行一个文本文件中的console、安全
    #                                 命令或是sql、DT、MR语句，执行结束后退出
    #                                 clickhousecmd。文本文件中可以包括多个命令，
    #                                 需要用”;”分割。当某个命令执行失败后会
    #                                 中止，不再执行后面的命令。
    def __run_job(self):
        clickhouse_sql_file = os.path.join(
                self.__job_work_dir, 
                task_util.CONSTANTS.CLICKHOUSE_SQL_FILE_NAME)
        if task_util.StaticFunction.write_content_to_file(
                clickhouse_sql_file,
                self.__clickhouse_sql) != task_util.FileCommandRet.FILE_COMMAND_SUCC:
            err_log = ("write clickhouse sql file[%s] failed!" % clickhouse_sql_file)
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False

        ck_user = ""
        ck_pwd = ""
        if "__ck_user" in self._config_map and "__ck_password" in self._config_map:
            if self._config_map["__ck_user"].strip() != "" and self._config_map["__ck_password"].strip() != "":
                ck_user = "--user " + self._config_map["__ck_user"]
                ck_pwd = "--password " + self._config_map["__ck_password"]

        cmd = task_util.CONSTANTS.CLICKHOUSE_CMD_STR % (
                self.__ck_cmd, 
                self._config_map["__ck_host"],
                self._config_map["__ck_port"],
                ck_user,
                ck_pwd,
                clickhouse_sql_file)
        self._add_error_log(cmd)
        stdout_file = os.path.join(self.__job_work_dir, "stdout.log")
        stderr_file = os.path.join(self.__job_work_dir, "stderr.log")
        run_back_ret = self.__no_block_cmd.run_in_background(
                cmd, 
                stdout_file, 
                stderr_file)
        if run_back_ret != 0:
            err_log = ("clickhouse sql run cmd[%s] failed![ret:%s]" % (cmd, run_back_ret))
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False

        return True

if __name__ == "__main__":
    print("please run unit test in common/db_manabger")
