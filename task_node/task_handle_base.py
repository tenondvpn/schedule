###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import sys
import datetime
import shutil
import re
import os
import traceback
import time
import logging

import horae.models
import django.db

sys.path.append('../common')
import task_util
import no_block_sys_cmd
import node_sql_manager

class TaskHandleBase(object):
    """
        任务处理基类
    """
    def __init__(self, config):        
        self._log = logging
        self._config_map = {}
        self._conf_replace_pattern = '\$\{[^}]*\}'
        self._replace_pair_map = {}
        self.__disks = config.get('node', 'disks').split(';')

        self.__package_path = "/root/databaas/packages"
        self.__rerun_path = config.get("node", "rerun_path").strip()
        self._sys_cmd = no_block_sys_cmd.NoBlockSysCommand()
        self._sql_manager = node_sql_manager.SqlManager()
        self._job_status = task_util.TaskState.TASK_FAILED
        self._old_job_status = task_util.TaskState.TASK_FAILED
        self._stop_task = False
        self._error_list = []
        self._sql_count = 0
        self.__config = config

    def stop_task(self):
        self._stop_task = True
        self._job_status = task_util.TaskState.TASK_STOPED_BY_USER
        return True

    def _run_command(self, work_dir, cmd, stdout_file, stderr_file):
        stdout, stderr, retcode = self._sys_cmd.run_many(cmd)
        if stdout.strip() != '' and stdout_file is not None:
            stdout_file = os.path.join(
                    work_dir, 
                    stdout_file)
            ret = task_util.StaticFunction.write_content_to_file(
                    stdout_file, 
                    stdout, 
                    "a")
            if ret != task_util.FileCommandRet.FILE_COMMAND_SUCC:
                err_log = "write file failed![file:%s][content:%s]" % (
                        stdout_file, stdout)
                self._log.error(err_log)
                self._add_error_log(err_log)

        if stderr.strip() != '' and stderr_file is not None:
            stderr_file = os.path.join(
                    work_dir, 
                    stderr_file)
            ret = task_util.StaticFunction.write_content_to_file(
                    stderr_file, 
                    stderr, 
                    "a")
            if ret != task_util.FileCommandRet.FILE_COMMAND_SUCC:
                err_log = "write file failed![file:%s][content:%s]" % (
                        stderr_file, stderr)
                self._log.error(err_log)
                self._add_error_log(err_log)
        if retcode != self._except_ret:
            return False, stdout, stderr
        return True, stdout.decode("utf-8"), stderr.decode("utf-8")
    
    def _append_default_config(self):
        if '_schedule_id' not in self._config_map:
            self._config_map['_schedule_id'] = str(self._schedule_id)

        if '_pocessor_id' not in self._config_map:
            self._config_map['_pocessor_id'] = str(self._pid)

        if "--master" not in self._config_map:
            self._config_map['--master'] = "yarn"

        if "--driver-memory" not in self._config_map:
            self._config_map['--driver-memory'] = "1g"

        if "--executor-memory" not in self._config_map:
            self._config_map['--executor-memory'] = "1g"

        if "--executor-cores" not in self._config_map:
            self._config_map['--executor-cores'] = "1"

        if "--py-files" not in self._config_map:
            self._config_map['--py-files'] = ""

        if "--files" not in self._config_map:
            self._config_map['--files'] = ""

        return True

    def _init(self, work_dir):
        return (self._handle_config()
                and self._config_replace_time_by_run_time(work_dir))

    # 从配置中读取配置的kv对，并写入map
    def _handle_config(self):
        if self._config is None:
            self._config = ""

        config_list = self._config.split("\n")
        if len(config_list) <= 0:
            err_log = ("no more config info [ready_task_id:%s]" %
                    self._ready_task_id)
            self._log.error(err_log)
            self._add_error_log(err_log)
            return True

        for config in config_list:
            if config.strip() == '':
                continue

            find_pos = config.find('=')
            if find_pos == -1:
                err_log = ("%s %s error config info "
                        "[ready_task_id:%d][conf:%s]" % (
                        __file__, sys._getframe().f_lineno, 
                        self._ready_task_id, config))
                self._log.error(err_log)
                self._add_error_log(err_log)
                continue

            # 去除两端空格
            key = config[0: find_pos].strip()
            if key == '':
                continue

            value = config[find_pos + 1: len(config)].strip()
            if len(value) >= task_util.CONSTANTS.MAX_CONFIG_VALUE_LENGTH:
                err_log = ("config value [%s] extended max len[%s]" % (
                        value, 
                        task_util.CONSTANTS.MAX_CONFIG_VALUE_LENGTH))
                self._log.error(err_log)
                self._add_error_log(err_log)
                return False

            self._config_map[key] = value
        # 设置系统默认配置
        return self._append_default_config()

    def _get_replace_map(self, sub_day, sub_hour, sub_min, work_dir):
        timespan = datetime.timedelta(
                days=sub_day, 
                hours=sub_hour, 
                minutes=sub_min)
        tmp_run_time = (self.__run_time_datetime - timespan).strftime(
                "%Y%m%d%H%M")
        year = tmp_run_time[0 : 4]
        month = tmp_run_time[4 : 6]
        day = tmp_run_time[6 : 8]
        hour = tmp_run_time[8 : 10]
        minute = tmp_run_time[10 : 12]
        self._replace_pair_map = {
                '%year%': year,
                '%yyyy%': year,
                '%Y%': year,
                '%month%': month,
                '%mm%': month,
                '%m%': month,
                '%day%': day,
                '%dd%': day,
                '%d%': day,
                '%hour%': hour,
                '%hh%': hour,
                '%H%': hour,
                '%minute%': minute,
                '%MM%': minute,
                '%M%': minute,
                '%work_dir%': work_dir,
                '@-\d+day': '',
                '@-\d+hour': '',
                '@-\d+min': '',
            }

    # 将配置中的时间根据run_time进行替换
    # 比如date = %year%%month%%day%@-1day, run_time = '201505071130'
    # date将被替换为20150506
    # 注意：这个操作会将一个配置的值的时间全部统一替换
    # 例如 run_time = '201505071130'：
    # /home/%year%_%month%_%day%@-1day/%year%_%month%_%day%_%hour%@-1hour.log
    # 会被替换为
    # /home/2015_05_06/2015_05_06_10.log
    def _config_replace_time_by_run_time(self, work_dir):
        oper_day_pattern = '.*@-(\d+)day.*'
        oper_hour_pattern = '.*@-(\d+)hour.*'
        oper_min_pattern = '.*@-(\d+)min.*'
        for config_key in self._config_map:
            # 计算需要减去的天数和小时数
            sub_day = 0
            regex_match = re.match(
                    oper_day_pattern, 
                    self._config_map[config_key])
            if regex_match is not None:
                sub_day = int(regex_match.group(1))
            sub_hour = 0
            regex_match = re.match(
                    oper_hour_pattern, 
                    self._config_map[config_key])
            if regex_match is not None:
                sub_hour = int(regex_match.group(1))

            sub_min = 0
            regex_match = re.match(
                    oper_min_pattern, 
                    self._config_map[config_key])
            if regex_match is not None:
                sub_min = int(regex_match.group(1))

            self._get_replace_map(sub_day, sub_hour, sub_min, work_dir)

            for replace_key in self._replace_pair_map:
                # 替换对应的值
                replace_result, number = re.subn(
                        replace_key, 
                        self._replace_pair_map[replace_key], 
                        self._config_map[config_key]) 
                if number > 0:
                    self._config_map[config_key] = replace_result
        return True

    def _prepair_work_dir(self):
        choose = ''
        for disk in self.__disks:
            disk = disk.strip()
            if disk == '':
                continue
            try:
                vfs = os.statvfs(disk)
                available = vfs.f_bavail * vfs.f_bsize
                print("invalid disk: %s %d, need: %d", disk, available, task_util.CONSTANTS.WORK_DIR_SPACE_VALID)
                if available > task_util.CONSTANTS.WORK_DIR_SPACE_VALID:
                    choose = disk
                    break
            except Exception as ex:
                err_log = (
                        "get disk failed[%s][%s] failed!" % (
                        str(ex), traceback.format_exc()))
                self._log.error(err_log)
                self._add_error_log(err_log)
                continue

        # 如果磁盘不可用，则直接退出程序
        if choose == '':
            task_util.CONSTANTS.GLOBAL_STOP = True
            time.sleep(10)
            sys.exit(1)

        tmp_work_dir = "%s/data_platform/%s/" % (choose, str(self._schedule_id))
        if os.path.exists(tmp_work_dir):
            rerun = self._sql_manager.get_last_rerun(self._schedule_id)
            if rerun is not None:
                back_dir = "%s/%s" % (self.__rerun_path, rerun.id)
                cmd = "rm -rf %s && mv %s %s" % (back_dir, tmp_work_dir, back_dir)
                stdout, stderr, return_code = self._sys_cmd.run_once(cmd)
                if return_code != 0:
                    err_log = ("%s %s run command failed[cmd:%s]" % (
                        __file__, sys._getframe().f_lineno, cmd))
                    self._log.error(err_log)
                    self._add_error_log(err_log)

            if os.path.exists(tmp_work_dir):
                shutil.rmtree(tmp_work_dir)

        os.makedirs(tmp_work_dir)
        return tmp_work_dir

    def _get_work_dir(self):
        return self._work_dir
    
    def __download_from_git(self, work_dir, git_url):
        self._add_error_log("git url: " + git_url)
        git_split = git_url.split('/')
        if len(git_split) <= 0:
            return False
        
        name_split = git_split[len(git_split) - 1].split('.')
        if len(name_split) != 2:
            return False

        cmd = ("cd %s && git clone %s && cp -rf ./%s/* ./" % (work_dir, git_url, name_split[0]))
        self._log.info("download package: " + cmd)
        stdout, stderr, return_code = self._sys_cmd.run_once(cmd)
        if return_code != 0:
            err_log = ("%s %s clone file failed[cmd:%s]" % (
                    __file__, sys._getframe().f_lineno, cmd))
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False
        
        return True

    def __download_from_oss(self, work_dir):
        package_name = "%d-%s.tar.gz" % (self._pid, self._version_id)
        package_file_path = os.path.join(
                self.__package_path, package_name)
        local_file_path = os.path.join(work_dir, package_name)
        admin_sshpass_cmd=self.__config.get("node", "admin_sshpass_cmd")
        tar_cmd = "cp %s %s && cd %s && tar -zxvf %s" % (package_file_path, local_file_path, work_dir, package_name)
        if admin_sshpass_cmd is not None and admin_sshpass_cmd.strip() != "":
            tar_cmd = admin_sshpass_cmd.format(package_file_path, local_file_path) + " && cd %s && tar -zxvf %s" % (work_dir, package_name)

        self._log.info("download package: " + tar_cmd)
        stdout, stderr, return_code = self._sys_cmd.run_once(tar_cmd)
        if return_code != 0:
            err_log = ("%s %s tar file failed[cmd:%s]" % (
                    __file__, sys._getframe().f_lineno, tar_cmd))
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False
        return True
    
    # 从oss下载工作包，放入工作目录，并解压
    def _download_package(self, work_dir):
        upload_info = horae.models.UploadHistory.objects.get(
                    id=self._version_id)
        if upload_info.type == 1:
            return self.__download_from_git(work_dir, upload_info.git_url)
        else:
            return self.__download_from_oss(work_dir)
       
    def _write_task_status_to_db(
            self, 
            task_status, 
            old_status, 
            task_handler=None, 
            work_dir=None,
            try_times=3,
            cpu=0,
            mem=0,
            ret_code=0):
        # 这个函数调用十分重要，所以加上重试机制，防止网络抖动对业务的影响
        sql_list = []
        now_format_time = task_util.StaticFunction.get_now_format_time(
                "%Y-%m-%d %H:%M:%S")
        # 下面的sql都需要加上status=old_status，
        # 增强容错性
        # 写入ready_task,
        ready_task_sql = ''
        work_dir_field = ""
        if work_dir is not None and work_dir.strip() != "":
            work_dir_field = ("work_dir = '%s'," % work_dir)

        task_handler_field = ""
        if task_handler is not None and task_handler.strip() != '':
            task_handler_field = ("task_handler = '%s'," % task_handler)

        update_starttime_field = ""
        update_inittime_field = ""
        if task_status == task_util.TaskState.TASK_RUNNING and old_status == task_util.TaskState.TASK_READY:
            now_time = task_util.StaticFunction.get_now_format_time(
                "%Y-%m-%d %H:%M:%S")
            update_starttime_field = (" start_time='%s', " % now_time)
            update_inittime_field = (" init_time='%s', " % now_time)

        ready_task_sql = ("update horae_readytask set status = %d, "
                        "update_time = '%s', %s %s %s ret_code=%d "
                        " where id = %d and status = %d;" % (
                        task_status, 
                        now_format_time, 
                        task_handler_field,
                        work_dir_field,
                        update_inittime_field,
                        ret_code,
                        self._ready_task_id, 
                        old_status))
        sql_list.append(ready_task_sql)
        # 更新schedule
        schedule_sql = ("update horae_schedule set status = %d, "
                "end_time = '%s', %s ret_code=%d where id = %d and status = %d;" % (
                task_status, 
                now_format_time,
                update_starttime_field,
                ret_code,
                self._schedule_id, 
                old_status))
        sql_list.append(schedule_sql)
        run_history_sql = ("update horae_runhistory set status = %d, "
                "end_time = '%s', schedule_id = %d, %s %s "
                "cpu = %d, mem = %d, ret_code=%d where task_id = '%s' "
                "and status = %d and run_time = '%s' ;" % (
                task_status, 
                now_format_time,
                self._schedule_id,
                task_handler_field,
                update_starttime_field,
                cpu,
                mem,
                ret_code,
                self._task_id,
                old_status,
                self._task_run_time))
        sql_list.append(run_history_sql)

        tried_times = 0
        while tried_times < try_times:
            tried_times = tried_times + 1
            try:
                if not self._sql_manager.batch_execute_with_affect_one(
                        None, 
                        None, 
                        sql_list):
                    err_log = ("%s %s batch_execute_with_transaction "
                            "failed will retied:%d" % (
                            __file__, sys._getframe().f_lineno, tried_times))
                    self._log.warn(err_log)
                    self._add_error_log(err_log)
                    time.sleep(1)
                    continue
            except django.db.OperationalError as ex:
                django.db.close_old_connections()
                self._log.error("execute sql failed![ex:%s][trace:%s]!" % (
                        str(ex),
                        traceback.format_exc()))
                time.sleep(1)
                continue
            except Exception as ex:
                self._log.error("execute sql failed![ex:%s][trace:%s]!" % (
                        str(ex), traceback.format_exc()))
                time.sleep(1)
                continue

            return True
        
        return False

    def _update_run_history_end_time(self, schedule_id):
        try:
            run_history = horae.models.RunHistory.objects.get(
                    schedule_id=schedule_id)
            run_history.end_time = \
                    task_util.StaticFunction.get_now_format_time(
                    "%Y-%m-%d %H:%M:%S")
            run_history.save()
            return True
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self._log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return False
        except Exception as ex:
            self._log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return False

    def _init_task(self, task_info):
        self._error_list = []
        if len(task_info) < \
                node_sql_manager.ConstantSql.ALL_READY_TASK_SQL_PARAM_NUM:
            err_log = ("%s %s start task task_info must "
                    "has %d params,but now[%d]" % (
                    __file__, sys._getframe().f_lineno, 
                    node_sql_manager.ConstantSql.ALL_READY_TASK_SQL_PARAM_NUM, 
                    len(task_info)))
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False

        self._ready_task_id = task_info[0]
        self._pipeline_id = task_info[1]
        self._schedule_id = task_info[2]
        self._task_type = task_info[3]
        self._task_id = task_info[4]
        self._task_run_time = task_info[5]
        self._pid = task_info[6]
        self._config = task_info[7]
        self._template = task_info[8]
        self._sql_count = 0
        if self._template is not None \
                and self._template.strip() != '':
            self._template = self._template.replace('\r', ' ')
            sql_temp = self._template.strip()
            if not sql_temp.endswith(';'):
                sql_temp = sql_temp + ';'
                self._template = sql_temp

            sql_list = sql_temp.split('\n')
            for sql in sql_list:
                sql = sql.strip()
                if sql != '' \
                        and sql.endswith(';') \
                        and not sql.startswith('set'):
                    self._sql_count = self._sql_count + 1

        self._priority = task_info[9]
        self._except_ret = task_info[10]
        self._over_time = int(task_info[11])
        self._task_status = int(task_info[12])
        self._task_handler = task_info[13]
        self._log.info("get task handler: %s" % self._task_handler)
        self._owner_id = task_info[14]
        if task_info[15] is None or task_info[15].strip() =='':
            self._work_dir = None
        else:
            self._work_dir = task_info[15]

        self._version_id = task_info[18]
        self._init_time = task_info[19]
        self._task_name = task_info[20]
        if self._task_run_time is None:
            return True

        if len(self._task_run_time) != task_util.CONSTANTS.RUN_TIME_LENTGH:
            return False

        # 经过测试，发现strptime这个方法有时会失败，暂不确定原因
        while not task_util.CONSTANTS.GLOBAL_STOP:
            try:
                self.__run_time_datetime = datetime.datetime.strptime(
                        self._task_run_time, 
                        "%Y%m%d%H%M")
            except Exception as ex:
                err_log = ("%s %s datetime no strptime[%s][%s]" % (
                        __file__, sys._getframe().f_lineno, 
                        str(ex), traceback.format_exc()))
                self._log.error(err_log)
                self._add_error_log(err_log)
                time.sleep(1)
                continue
            break

        return True

    def _add_error_log(self, log_str):
        if log_str is not None and log_str != '':
            self._error_list.append(log_str)

    def _write_error_log_to_file(self, file_name):
        if len(self._error_list) > 0:
            error_log = '\n'.join(self._error_list)
            if task_util.StaticFunction.write_content_to_file(
                    file_name, 
                    error_log,
                    'a') != task_util.FileCommandRet.FILE_COMMAND_SUCC:
                return False
        return True
