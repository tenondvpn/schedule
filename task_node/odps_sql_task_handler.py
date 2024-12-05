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

class OdpsSqlTaskHandler(task_handle_base.TaskHandleBase):
    """
        启动odps-sql任务，非阻塞
        每一种任务都需要重新创建实例，线程不安全
    """
    def __init__(self, config):
        task_handle_base.TaskHandleBase.__init__(self, config)
        self.__odps_cmd = config.get("node", "odps_sql")
        self.__job_work_dir = None
        self.__odps_sql = None
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

            if self._task_type != task_util.TaskType.ODPS_TYPE:
                err_log = (
                        "this is just for odps_sql[%d],but now[%d]" % \
                        (task_util.TaskType.APSARA_JOB, self._task_type))
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
                err_log = ("odps sql has no sql template!")
                self._log.warn(err_log)
                self._add_error_log(err_log)
                break
            self.__odps_sql = task_util.StaticFunction.replace_str_with_regex(
                    self._template, 
                    self._conf_replace_pattern, 
                    self._config_map)
            if self.__odps_sql is None:
                err_log = (
                        "replace tpl content[%s] failed!" % self._template)
                self._log.error(err_log)
                self._add_error_log(err_log)
                break
        
            # 执行odps-sql，非阻塞执行，需要定时获取执行状态
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
            return False

        if not self._handle_config():
            return False

        self.__job_work_dir = self._get_work_dir()
        if self.__job_work_dir is None:
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
        ret, instance_list = self.__get_instance_id_list()
        if not ret:
            err_log = ("__get_instance_id failed!")
            self._log.error(err_log)
            self._add_error_log(err_log)
            return err_log

        if instance_list is None:
            return "get no instance!"

        result_list = []
        for instance in instance_list:
            tmp_list = []
            tmp_list.append(instance)
            status = self.__check_odps_instances_status(tmp_list)
            result_map = {}
            result_map["instance"] = instance
            result_map["status"] = task_util.global_status_info_map[status]
            result_list.append(result_map)
        return str(result_list)

    def __get_task_status(self, task_info):
        instance_num = self.__get_instance_num()
        if instance_num <= 0:
            err_log = ("this task has no instance")
            self._log.error(err_log)
            self._add_error_log(err_log)
            return task_util.TaskState.TASK_RUNNING

        try_count = 0
        ret = False
        instance_list = None
        while try_count < 30:
            if task_util.CONSTANTS.GLOBAL_STOP:
                return False

            ret, instance_list = self.__get_instance_id_list()
            if not ret:
                break

            if instance_list is not None and len(instance_list) > 0:
                break

            try_count += 1
            time.sleep(1)

        if try_count >= 60:
            self._add_error_log('\nget none instnce in 600s')
            ret, instance_list = False, None

        status = task_util.TaskState.TASK_FAILED
        if ret:
            if instance_list is None:
                return task_util.TaskState.TASK_RUNNING

            status = self.__check_odps_instances_status(instance_list)

        if status == task_util.TaskState.TASK_RUNNING:
            return task_util.TaskState.TASK_RUNNING

        if status == task_util.TaskState.TASK_SUCCEED:
            if len(instance_list) < instance_num:
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

        ret, instance_list = self.__get_instance_id_list()
        if instance_list is None:
            return True

        if ret:
            for instance in instance_list:
                cmd = task_util.CONSTANTS.ODPS_SQL_CMD_STR % (
                        self.__odps_cmd, 
                        self._config_map["_odps_project"],
                        self._config_map["_odps_endpoint"],
                        self._config_map["_priority"],
                        self._config_map["_odps_access_id"],
                        self._config_map["_odps_access_key"],
                        "kill " + instance)
                status, stdout, stderr = self._run_command(
                        self.__job_work_dir,
                        cmd, 
                        None, 
                        None)
                self._add_error_log("%s\n%s\n%s\n%s" % (
                        cmd, status, stdout, stderr))
        return True

    # TODO：确认下odps通过ret_code是否可以确定任务运行成功，
    #       如果可以则不用检查instance的status
    # odpscmd [OPTION]
    # 参数    说明
    # --help/-h                       显示odpscmd的帮助信息
    # --project=<prj_name>            指定登录odpscmd后进入的默认project
    # --endpoint=<http://host:port>   设置odpscmd访问的odps server的地址
    #                                 （仅用于debug）
    # -u <user_name> -p <password>    输入accessid(user_name)和
    #                                 accesskey(password)
    # -M  以CSV格式显示返回结果
    # -e <"command;[command;]...">    串行执行console、安全命令或是sql、
    #                                 DT、MR语句，执行结束后退出odpscmd。
    #                                 -e后面可以接多个命令，需要用”;”分割。
    #                                 当某个命令执行失败后会中止，
    #                                 不再执行后面的命令。
    # -f <"file_path;">               串行执行一个文本文件中的console、安全
    #                                 命令或是sql、DT、MR语句，执行结束后退出
    #                                 odpscmd。文本文件中可以包括多个命令，
    #                                 需要用”;”分割。当某个命令执行失败后会
    #                                 中止，不再执行后面的命令。
    def __run_job(self):
        odps_sql_file = os.path.join(
                self.__job_work_dir, 
                task_util.CONSTANTS.ODPS_SQL_FILE_NAME)
        if task_util.StaticFunction.write_content_to_file(
                odps_sql_file,
                self.__odps_sql) != task_util.FileCommandRet.FILE_COMMAND_SUCC:
            err_log = ("write odps sql file[%s] failed!" % odps_sql_file)
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False

        cmd = task_util.CONSTANTS.ODPS_SQL_CMD_FILE_STR % (
                self.__odps_cmd, 
                self._config_map["_odps_project"],
                self._config_map["_odps_endpoint"],
                self._config_map["_priority"],
                self._config_map["_odps_access_id"],
                self._config_map["_odps_access_key"],
                odps_sql_file)
        self._add_error_log(cmd)
        stdout_file = os.path.join(self.__job_work_dir, "stdout.log")
        stderr_file = os.path.join(self.__job_work_dir, "stderr.log")
        run_back_ret = self.__no_block_cmd.run_in_background(
                cmd, 
                stdout_file, 
                stderr_file)
        if run_back_ret != 0:
            err_log = ("odps sql run cmd[%s] failed![ret:%s]" % (cmd, run_back_ret))
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False

        return True

    def __get_instance_num(self):
        if self._template is None:
            return 0

        return self._sql_count

    def __get_instance_id_list(self):
        """ 
            odpscmd的返回数据：stderr.log
            ID = 20150515020029665gktwv5sb1
            Log view:
            http://webconsole.odps.aliyun-inc.com:8080/logview/?h=http://servi
            ce.odps.aliyun-inc.com/api&p=aliyun_searchlog&i=20150515020029665g
            ktwv5sb1&token=T0J5WGZManZybTJMK0NSK2tMQTFheERaRVdJPSxPRFBTX09CTzo
            1Mjg2MywxNDMyMjYwMDI5LHsiU3RhdGVtZW50IjpbeyJBY3Rpb24iOlsib2RwczpSZ
            WFkIl0sIkVmZmVjdCI6IkFsbG93IiwiUmVzb3VyY2UiOlsiYWNzOm9kcHM6Kjpwcm9
            qZWN0cy9hbGl5dW5fc2VhcmNobG9nL2luc3RhbmNlcy8yMDE1MDUxNTAyMDAyOTY2N
            WdrdHd2NXNiMSJdfV0sIlZlcnNpb24iOiIxIn0=
            ID = 20150515020035513gnwlfv44
            Log view:
            http://webconsole.odps.aliyun-inc.com:8080/logview/?h=http://servi
            ce.odps.aliyun-inc.com/api&p=aliyun_searchlog&i=20150515020035513g
            nwlfv44&token=YVM3c01RNW1JZ2hDU3BVNWxyYXk1RU5PbTAwPSxPRFBTX09CTzo1
            Mjg2MywxNDMyMjYwMDM1LHsiU3RhdGVtZW50IjpbeyJBY3Rpb24iOlsib2RwczpSZW
            FkIl0sIkVmZmVjdCI6IkFsbG93IiwiUmVzb3VyY2UiOlsiYWNzOm9kcHM6Kjpwcm9q
            ZWN0cy9hbGl5dW5fc2VhcmNobG9nL2luc3RhbmNlcy8yMDE1MDUxNTAyMDAzNTUxM2
            dud2xmdjQ0Il19XSwiVmVyc2lvbiI6IjEifQ==

            获取ID是通过匹配 ID = 并检查下一行的值为Log view:
            返回结果不会与之冲突
        """
        stderr_file = os.path.join(self.__job_work_dir, "stderr.log")
        if not os.path.exists(stderr_file):
            err_log = ("std out file is not exists.[%s]" % stderr_file)
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False, None
        stderr_read_fd = open(stderr_file, "r")
        try:
            ID_CHECK = "ID = "
            LOG_VIEW_CHECK = "Log view:"
            prev_line = ''
            instance_id_list = []
            while not task_util.CONSTANTS.GLOBAL_STOP:
                line = stderr_read_fd.readline()
                if line is None or line == '':
                    break

                line = line.strip()
                if line == '':
                    continue

                if not self._stop_task:
                    if line.upper().find('FAILED') != -1 \
                            or line.upper().find('EXCEPTION') != -1:
                        return False, None

                if line == LOG_VIEW_CHECK or line == "OK":
                    if prev_line.startswith(ID_CHECK):
                        instance_id_list.append(
                                prev_line.split('=')[1].strip())
                prev_line = line
            if len(instance_id_list) <= 0:
                return True, None
            return True, instance_id_list
        except Exception as ex:
            err_log = ("get instance list failed![ex:%s][trace:%s]" % (
                    str(ex), traceback.format_exc()))
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False, None
        finally:
            stderr_read_fd.close()

    # 检查odps-sql的每一个instance是否success，如果有一个失败则失败
    def __check_odps_instances_status(self, instance_id_list):
        for instance in instance_id_list:
            cmd = task_util.CONSTANTS.ODPS_SQL_CMD_STR % (
                    self.__odps_cmd, 
                    self._config_map["_odps_project"],
                    self._config_map["_odps_endpoint"],
                    self._config_map["_priority"],
                    self._config_map["_odps_access_id"],
                    self._config_map["_odps_access_key"],
                    "status " + instance)
            status, stdout, stderr = self._run_command(
                    self.__job_work_dir,
                    cmd, 
                    None, 
                    None)
            if not status:
                err_log = ("get instance status failed![stderr:%s][stdout:%s]" % (
                        stderr, stdout))
                self._log.error(err_log)
                self._add_error_log(err_log)
                return task_util.TaskState.TASK_RUNNING

            if stdout.find("Running") != -1:
                return task_util.TaskState.TASK_RUNNING

            if stdout.find("Waiting") != -1:
                return task_util.TaskState.TASK_WAITING

            if stdout.find("Ready") != -1:
                return task_util.TaskState.TASK_READY

            if stdout.find("Success") != -1:
                continue

            if stdout.find("Failed") != -1:
                return task_util.TaskState.TASK_FAILED

            # 没有状态，则任务还在执行
            return task_util.TaskState.TASK_RUNNING

        # 成功了
        return task_util.TaskState.TASK_SUCCEED

if __name__ == "__main__":
    print("please run unit test in common/db_manabger")
