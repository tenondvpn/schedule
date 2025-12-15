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

import horae.models
import django.core.exceptions

import task_handle_base
import node_sql_manager

sys.path.append('../common')
import task_util
import no_block_sys_cmd
import qq_mail
import dingding_group

class ScriptTaskHandler(task_handle_base.TaskHandleBase):
    """
        启动script任务，非阻塞
        每一种任务都需要重新创建实例，线程不安全
    """
    def __init__(self, config, task_creator):
        task_handle_base.TaskHandleBase.__init__(self, config)
        self.__daemon_port = config.get("node", "daemon_port")
        self.__local_ip = task_util.StaticFunction.get_local_ip()
        self.__python_cmd = config.get("node", "python")
        self.__no_block_cmd = no_block_sys_cmd.NoBlockSysCommand()
        self.__job_type = task_util.ScriptJobType.PYTHON_SCRIPT
        self.__v100_job_id = ""
        self.__v100_user_id = ""
        self.__sql_manager = node_sql_manager.SqlManager()
        self.__task_creator = task_creator

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

            if self._task_type not in (
                    task_util.TaskType.SCRIPT_TYPE,
                    task_util.TaskType.SPARK_TYPE):
                err_log = ("this is just for script [%d] and spark[%d], but now[%d]" %
                           (task_util.TaskType.SCRIPT_TYPE,
                            task_util.TaskType.SPARK_TYPE, self._task_type))
                self._log.error(err_log)
                self._add_error_log(err_log)
                break

            # 准备工作路径
            self.__job_work_dir = self._prepair_work_dir()
            self._add_error_log("work_ip: %s\nwork_dir: %s\n" % (
                task_util.StaticFunction.get_local_ip(),
                self.__job_work_dir))
            if not self._download_package(self.__job_work_dir):
                err_log = ("download job package failed!")
                self._log.error(err_log)
                self._add_error_log(err_log)
                break

            # 初始化，包括配置中的时间转化，下载运行包，
            if not self._init(self.__job_work_dir):
                err_log = ("script job handle config failed!")
                self._log.error(err_log)
                self._add_error_log(err_log)
                break

            # run.conf.tpl转化为run.conf
            if not self.__handle_run_conf():
                break

            self.__handle_run_py()

            # 先停止之前的任务
            self.__stop_task(task_info)

            ret = self.__run_job()
            # 此时表示线程退出，不要修改db状态
            if ret is None:
                return True

            if not ret:
                break

            self._job_status = task_util.TaskState.TASK_RUNNING
            self._log.info("scuccess run job: %s", self.__v100_job_id)
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
            err_log = ("script job handle config failed!")
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False

        ret = False
        if self._task_handler is not None and self._task_handler.startswith("jb-aiinference"):
            ret = self.__stop_v100_job(task_info)
        else:
            ret = self.__stop_python_task(task_info)

        err_log_file = os.path.join(
            self.__job_work_dir,
            "trace.log")
        self._write_error_log_to_file(err_log_file)
        return ret

    def get_task_status(self, task_info):
        if not self._init_task(task_info):
            return task_util.TaskState.TASK_FAILED

        self.__job_work_dir = self._get_work_dir()
        if self.__job_work_dir is None:
            return task_util.TaskState.TASK_FAILED

        if not self._init(self.__job_work_dir):
            err_log = ("script job handle config failed!")
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False
        
        ret = task_util.TaskState.TASK_RUNNING
        ret_code = 0
        self._update_run_history_end_time(self._schedule_id)
        if self._task_handler is not None and self._task_handler.startswith("jb-aiinference"):
            ret = self.__get_v100_job_status(task_info)
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
        else:
            ret = self.__get_python_status(task_info)

        err_log_file = os.path.join(
            self.__job_work_dir,
            "trace.log")
        self._write_error_log_to_file(err_log_file)
        return ret
        
    def __get_python_status(self, task_info):
        return self.__get_task_status(task_info)
    
    def __pending_monitor(self, task_info):
        pending_timeout = 0
        if "--v100_pending_timeout" in self._config_map:
            pending_timeout = int(self._config_map["--v100_pending_timeout"])
        else:
            return
            
        if pending_timeout <= 0:
            return
        
        now_dm = datetime.datetime.now()
        use_time = now_dm - self._init_time
        use_time_sec = use_time.days * 24 * 3600 + use_time.seconds
        if use_time_sec < pending_timeout:
            return
        
        if not self.__task_creator.set_schedule_timeout(self._schedule_id):
            return
        
        pipeline = None
        try:
            pipeline = horae.models.Pipeline.objects.get(id=self._pipeline_id)
        except django.db.OperationalError as ex:
            django.db.close_old_connections()
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex),
                    traceback.format_exc()))
            return
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                    str(ex), traceback.format_exc()))
            return
        
        if pipeline.monitor_way == -1:
            return
        
        read_list, write_list = self.__sql_manager.get_owner_id_list(self._pipeline_id)
        users = self.__sql_manager.get_user_info(write_list)
        names = []
        receivers = []
        dingding_receivers = []
        dingding_names = []
        for user in users:
            if user["email"] is not None and user["email"].strip() != "":
                if user["email"] not in receivers:
                    receivers.append(user["email"])

                if user["name"] not in names:
                    names.append(user["name"])

            if user["dingding"] is not None and user["dingding"].strip() != "":
                if user["dingding"] not in dingding_receivers:
                    dingding_receivers.append(user["dingding"])

                if user["name"] not in dingding_names:
                    dingding_names.append(user["name"])
            
        if pipeline.monitor_way == 0 or pipeline.monitor_way == 2:
            header = "Databaas监控报警"
            sub_header = "流程: %s, 任务: %s 提交V100任务超时！" % (pipeline.name, self._task_name)
            content = ("<b>%s:</b>\n <br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;流程：<b>%s</b><br> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;任务：<b>%s</b><br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;运行时间点：<b>%s</b> <br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<font color='red'>提交V100任务超时，提交任务时间：%s，超时时间：%d 秒，设置超时：%d 秒!</font>\n<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;请登录平台检查: <b>http://10.109.112.6:8000/pipeline/history/%d/</b>" % 
                (", ".join(names), pipeline.name, self._task_name, self._task_run_time, 
                 self._init_time.strftime('%Y-%m-%d %H:%M:%S'),
                 use_time_sec,
                 pending_timeout,
                 self._pipeline_id))
            qq_mail.send_mail(receivers, header, sub_header, content)

        if pipeline.monitor_way == 1 or pipeline.monitor_way == 2:
            #dingding
            content = "%s：\n\n    任务异常: 提交V100任务超时！\n    提交任务时间：%s，超时时间：%d 秒，设置超时：%d 秒!\n    流程: %s\n    任务: %s\n    运行时间点: %s\n    请登录平台检查: http://10.109.112.6:8000/pipeline/history/%d/" % (
                ", ".join(dingding_names), 
                self._init_time.strftime('%Y-%m-%d %H:%M:%S'),
                use_time_sec,
                pending_timeout, pipeline.name, self._task_name, self._task_run_time, self._pipeline_id)
            
            for dingding_url in dingding_receivers:
                dingding_group.send_dingding_group(dingding_url, content)

    def __get_v100_job_log(self, pod_name):
        try:
            # 要请求的 URL zjcp.zhejianglab.cn:38080
            # 要发送的数据，使用字典表示
            self.__v100_user_id = "585398485541453824"
            if "--v100_job_userid" in self._config_map:
                self.__v100_user_id = self._config_map["--v100_job_userid"]


            # http://prod-cn.your-api-server.com/api/open/job-service/log/info
            t = time.time()
            start_time = int(round(t * 1000)) - 500000
            end_time = int(round(t * 1000))
            data = {
                "bizType": "DROS",
                "jobPodName": pod_name,
                "loginName": "liuyang2167@zhejianglab.com",
                "start": str(start_time),
                "end": str(end_time)
            }

            node_req_url = "http://zjcp.zhejianglab.cn:38080/api/open/job-service/log/info?bizType=DROS&jobPodName=%s&start=%d&end=%d" % (pod_name, start_time, end_time)  #目标 URL
            # 将字典转换为 JSON 格式
            data_json = json.dumps(data).encode('utf-8')
            # 创建一个请求对象，设置请求类型为 POST，并添加 Content-Type 头
            request = urllib.request.Request(node_req_url, data=data_json, method='GET')
            request.add_header('Content-Type', 'application/json')
            request.add_header('accessToken', self.__get_access_token())
            # 发送请求并读取响应
            response = urllib.request.urlopen(request, timeout=3).read()
            response_json = json.loads(response.decode('utf-8'))
            if response_json["code"] != 200:
                return
            
            self._add_error_log(response_json["data"]["logContent"])
        except urllib.error.HTTPError as ex:
            self._log.error('get job status error:' + str(ex) + traceback.format_exc())
        except urllib.error.URLError as ex:
            self._log.error('get job status error:' + str(ex) + traceback.format_exc())
        except Exception as ex:
            self._log.error('get job status error:' + str(ex) + traceback.format_exc())

    def __get_v100_job_monitor(self, task_info):
        try:
            # 要请求的 URL zjcp.zhejianglab.cn:38080
            # 要发送的数据，使用字典表示
            self.__v100_user_id = "585398485541453824"
            if "--v100_job_userid" in self._config_map:
                self.__v100_user_id = self._config_map["--v100_job_userid"]

            data = {
                "bizType": "DROS",
                "jobId": self._task_handler,
                "userId": self.__v100_user_id,
                "loginName": "liuyang2167@zhejianglab.com"
            }

            # http://prod-cn.your-api-server.com/api/open/open-job/v1/getJobMetrics
            node_req_url = "http://zjcp.zhejianglab.cn:38080/api/open/open-job/v1/getJobMetrics?bizType=DROS&userId=%s&jobId=%s&loginName=%s" % (self.__v100_user_id, self._task_handler, "liuyang2167@zhejianglab.com")  #目标 URL
            # 将字典转换为 JSON 格式
            data_json = json.dumps(data).encode('utf-8')
            # 创建一个请求对象，设置请求类型为 POST，并添加 Content-Type 头
            request = urllib.request.Request(node_req_url, data=data_json, method='GET')
            request.add_header('Content-Type', 'application/json')
            request.add_header('accessToken', self.__get_access_token())
            # 发送请求并读取响应
            response = urllib.request.urlopen(request, timeout=3).read()
            response_json = json.loads(response.decode('utf-8'))
            if response_json["code"] != 200:
                return
            
            for node in response_json["data"]["zoneType"]["nodes"]:
                for pod in node["pods"]:
                    self.__get_v100_job_log(pod["podName"])

        except urllib.error.HTTPError as ex:
            self._log.error('get job status error:' + str(ex) + traceback.format_exc())
        except urllib.error.URLError as ex:
            self._log.error('get job status error:' + str(ex) + traceback.format_exc())
        except Exception as ex:
            self._log.error('get job status error:' + str(ex) + traceback.format_exc())
    
    def __get_v100_job_status(self, task_info):
        try:
            # 要请求的 URL zjcp.zhejianglab.cn:38080
            # 要发送的数据，使用字典表示
            self.__v100_user_id = "585398485541453824"
            if "--v100_job_userid" in self._config_map:
                self.__v100_user_id = self._config_map["--v100_job_userid"]

            data = {
                "bizType": "DROS",
                "jobId": self._task_handler,
                "userId": self.__v100_user_id
            }
            node_req_url = "http://zjcp.zhejianglab.cn:38080/api/open/open-job/origin/v1/getJobDetail?bizType=DROS&userId=%s&jobId=%s" % (self.__v100_user_id, self._task_handler)  #目标 URL
            # 将字典转换为 JSON 格式
            data_json = json.dumps(data).encode('utf-8')
            # 创建一个请求对象，设置请求类型为 POST，并添加 Content-Type 头
            request = urllib.request.Request(node_req_url, data=data_json, method='GET')
            request.add_header('Content-Type', 'application/json')
            request.add_header('accessToken', self.__get_access_token())
            # 发送请求并读取响应
            response = urllib.request.urlopen(request, timeout=3).read()
            response_json = json.loads(response.decode('utf-8'))
            # 获取 accessToken 的值
            status = response_json['data']['jobMeta']['jobStatus']
            self._add_error_log(status + "\n")
            self.__get_v100_job_monitor(task_info)
            if status == "SUCCEEDED":
                return task_util.TaskState.TASK_SUCCEED
            elif status == "FAILED":
                reason = response_json['data']['jobErrorInfo']['errorMsg']
                self._add_error_log(reason + "\n")
                return task_util.TaskState.TASK_FAILED
            elif status == "PENDING":
                # monitor
                self.__pending_monitor(task_info)
            
            return task_util.TaskState.TASK_RUNNING
        except urllib.error.HTTPError as ex:
            self._log.error('get job status error:' + str(ex) + traceback.format_exc())
        except urllib.error.URLError as ex:
            self._log.error('get job status error:' + str(ex) + traceback.format_exc())
        except Exception as ex:
            self._log.error('get job status error:' + str(ex) + traceback.format_exc())

        return task_util.TaskState.TASK_RUNNING
    
    def __stop_python_task(self, task_info):
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
    
    def __stop_v100_job(self, task_info):
        try:
            self.__v100_user_id = "585398485541453824"
            if "--v100_job_userid" in self._config_map:
                self.__v100_user_id = self._config_map["--v100_job_userid"]

            # 要请求的 URL http://prod-cn.your-api-server.com/api/open/open-job/user/v1/deleteJob
            node_req_url = ("http://zjcp.zhejianglab.cn:38080/api/open/open-job/user/v1/deleteJob?userId=%s&bizType=DROS&jobId=%s"
                             % (self.__v100_user_id, self._task_handler))
            # 创建一个请求对象，设置请求类型为 DELETE，并添加 Content-Type 头
            request = urllib.request.Request(node_req_url, data=None, method='DELETE')
            request.add_header('Content-Type', 'application/json')
            request.add_header('accessToken', self.__get_access_token())
            # 发送请求并读取响应
            print("send req: %s" % node_req_url)
            self._add_error_log("send req: %s\n" % node_req_url)
            response = urllib.request.urlopen(request, timeout=3).read()
            res_json = json.loads(response.decode('utf-8'))
            print("stop response: ")
            print(res_json)
            self._add_error_log("stop res: %s\n" % response.decode('utf-8'))
            return True
        except urllib.error.HTTPError as ex:
            self._log.error('error:' + str(ex) + traceback.format_exc())
        except urllib.error.URLError as ex:
            self._log.error('error:' + str(ex) + traceback.format_exc())
        finally:
            pass

        return False

    def __handle_run_py(self):
        if 'script_name' in self._config_map \
                and self._config_map['script_name'].strip() != '' \
                and self._config_map['script_name'].strip() != \
                task_util.CONSTANTS.SCRIPT_DEFSULT_PYTHON_FILE:
            cmd = 'mv %s/%s %s/%s' % (
                self.__job_work_dir,
                self._config_map['script_name'].strip(),
                self.__job_work_dir,
                task_util.CONSTANTS.SCRIPT_DEFSULT_PYTHON_FILE)
            self.__no_block_cmd.run_once(cmd)

        if '_out' in self._config_map \
                and self._config_map['_out'].strip() != '':
            cmd = 'mv %s/%s %s/%s' % (
                self.__job_work_dir,
                'run.conf',
                self.__job_work_dir,
                self._config_map['_out'].strip())
            self.__no_block_cmd.run_once(cmd)

    def __get_proceeding(self):
        return "script no proceeding."

    def __handle_run_conf(self):
        # run.conf.tpl转化为run.conf
        tpl_in_file = os.path.join(
            self.__job_work_dir,
            task_util.CONSTANTS.SCRIPT_DEFSULT_TPL_CONF_NAME)
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
            task_util.CONSTANTS.SCRIPT_DEFSULT_CONF_NAME)

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
            return task_util.TaskState.TASK_RUNNING

        status = task_util.TaskState.TASK_FAILED
        ret_code = 0
        if http_res.startswith("error"):
            err_log = ("run task failed:%s[res:%s]" % (
                daemon_req_url, http_res))
            self._log.error(err_log)
            self._add_error_log(err_log)
        else:
            result = http_res.split(",")
            if len(result) == 2:
                status = int(result[0])
                ret_code = int(result[1])
            else:
                status = int(http_res)

        if status == task_util.TaskState.TASK_TIMEOUT:
            err_log = ("task time out[%s]" % str(task_info))
            self._log.info(err_log)
            self._add_error_log(err_log)
            return task_util.TaskState.TASK_TIMEOUT

        if status in (
                task_util.TaskState.TASK_FAILED,
                task_util.TaskState.TASK_SUCCEED):
            if not self._write_task_status_to_db(
                    status,
                    task_util.TaskState.TASK_RUNNING,
                    ret_code=ret_code):
                err_log = ("write_start_task_status_to_db failed!")
                self._log.warn(err_log)
                self._add_error_log(err_log)
                status = task_util.TaskState.TASK_FAILED

        return status
    
    def __run_script_job(self):
        prev_cmd = ""
        if "--prev_command" in self._config_map:
            prev_cmd = self._config_map["--prev_command"].strip()
            prev_cmd += " && "

        if self._task_type == task_util.TaskType.SCRIPT_TYPE:
            cmd = prev_cmd + "cd %s && %s %s" % (
                self.__job_work_dir,
                self.__python_cmd,
                task_util.CONSTANTS.SCRIPT_DEFSULT_PYTHON_FILE)
        elif self._task_type == task_util.TaskType.SPARK_TYPE:
            py_files = ""
            if self._config_map["--py-files"].strip() != "":
                py_files = "--py-files %s" % self._config_map["--py-files"].strip()

            files = "--files run.conf"
            if self._config_map["--files"].strip() != "":
                files = "--files %s,run.conf" % self._config_map["--files"].strip()

            cmd = ("cd %s && spark-submit "
                   "--master %s "
                   "--driver-memory %s "
                   "--executor-memory %s "
                   "--executor-cores %s "
                   "%s "
                   "%s "
                   "./run.py" % (
                self.__job_work_dir,
                self._config_map["--master"],
                self._config_map["--driver-memory"],
                self._config_map["--executor-memory"],
                self._config_map["--executor-cores"],
                py_files,
                files))
        else:
            err_log = ("wrong script type:%d] " % self._task_type)
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False

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

    def __run_local_docker(self):
        print("run local docker")
        default_conf_content = '[run]\\\\n'
        for key in self._config_map:
            if key == "--dockerfile_str" or key == "--prev_command":
                continue

            val = self._config_map[key]
            val.replace(' ', '')
            default_conf_content += "%s=%s\\\\n" % (key, val)

        if default_conf_content == '':
            return True
        
        default_docker_str = (
            "from grobid/grobid:0.8.0\n"
            "RUN mkdir -p /workspace\n"
            "RUN echo \"root:datapipeline\" | chpasswd\n"
            "RUN apt-get update\n"
            "RUN apt-get install git expect libgl1-mesa-glx -y --force-yes\n"
            "RUN pip install numpy clickhouse-connect pymupdf oss2 pdfplumber pandas frontend -i https://pypi.tuna.tsinghua.edu.cn/simple\n"
            "RUN python3 -m pip install grobid-client-python\n"
            "RUN pip install opencv-python timm==0.5.4 python-Levenshtein albumentations pypdf orjson==3.3.0 PyPDF2 transformers==4.38.2 paddleocr paddlepaddle langid -i https://pypi.tuna.tsinghua.edu.cn/simple\n"
            "RUN pip install tensorflow -i https://pypi.tuna.tsinghua.edu.cn/simple\n"
            "RUN pip install fasttext langdetect -i https://pypi.tuna.tsinghua.edu.cn/simple\n"
            "COPY builder.py /usr/local/lib/python3.8/dist-packages/google/protobuf/internal/builder.py\n"
            "COPY ./ /workspace/")
        
        if "--dockerfile_str" in self._config_map:
            default_docker_str = self._config_map["--dockerfile_str"].strip()

        docker_out_file = os.path.join(
            self.__job_work_dir,
            "Dockerfile")
        if task_util.StaticFunction.write_content_to_file(
                docker_out_file,
                default_docker_str) != \
                task_util.FileCommandRet.FILE_COMMAND_SUCC:
            err_log = ("write docker file failed![%s]" %
                       docker_out_file)
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False
        
        docker_image = "docker_image_%d" % self._schedule_id
        build_cmd = "cd %s && docker build --rm -t %s ." % (
            self.__job_work_dir,
            docker_image) + " && "
        
        if "--docker_image" in self._config_map:
            docker_image = self._config_map["--docker_image"]
            build_cmd = ""

        if "--docker_command" not in self._config_map:
            return False
        
        docker_command = self._config_map["--docker_command"]
            
        self._log.info(build_cmd)
        # stdout, stderr, return_code = self.__no_block_cmd.run_once(cmd)
        # if return_code != 0:
        #     err_log = ("%s %s run command failed[cmd:%s], return_code: %d" % (
        #         __file__, sys._getframe().f_lineno, cmd, return_code))
        #     self._log.error(err_log)
        #     self._add_error_log(err_log)
        #     self._log.info(stdout)
        #     self._log.error(stderr)
        #     print(err_log)
        #     return False

        run_docker_cmd = build_cmd + ("""docker run --network=host --add-host geocloud.oss-cn-hangzhou-zjy-d01-a.ops.cloud.zhejianglab.com:10.200.4.114 --add-host gitee.zhejianglab.com:10.102.1.52 %s %s %s""" % (docker_image, docker_command, default_conf_content))
        print(run_docker_cmd)
        self._log.info(run_docker_cmd)
        stdout_file = os.path.join(self.__job_work_dir, "stdout.log")
        stderr_file = os.path.join(self.__job_work_dir, "stderr.log")
        cmd_base64 = base64.b64encode(run_docker_cmd.encode('utf-8'))
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
                http_res = str(urllib.request.urlopen(daemon_req_url, timeout=3).read(), 'utf-8')
                if http_res != 'OK':
                    self._log.error("run task failed:%s[res:%s]" % (
                        daemon_req_url, http_res))
                    err_log = ("run task failed:%s" % daemon_req_url)
                    self._log.error(err_log)
                    self._add_error_log(err_log)
                    return False
                print("success")
                return True
            except Exception as ex:
                err_log = ("daemon server fail[%s][ex:%s][trace:%s]" % (
                    daemon_req_url, str(ex), traceback.format_exc()))
                self._log.error(err_log)
                self._add_error_log(err_log)
                print("error")
                time.sleep(1)
                continue

    def __get_access_token(self):
        try:
            # 要请求的 URL
            node_req_url = "http://zjcp.zhejianglab.cn:38080/api/open/credentials"  #目标 URL
            # 要发送的数据，使用字典表示
            data = {
                'appKey': '6a0f59bd46ec4e44bfd3b8cbe70f5395',
                'appSecret': 'ddc6224168ea47bda71680badd338599'
            }

            # 将字典转换为 JSON 格式
            data_json = json.dumps(data).encode('utf-8')
            # 创建一个请求对象，设置请求类型为 POST，并添加 Content-Type 头
            request = urllib.request.Request(node_req_url, data=data_json, method='POST')
            request.add_header('Content-Type', 'application/json')
            # 发送请求并读取响应
            response = urllib.request.urlopen(request, timeout=3).read()
            response_json = json.loads(response.decode('utf-8'))
            # 获取 accessToken 的值
            return response_json['data']['accessToken']
        except urllib.error.HTTPError as ex:
            self._log.error('error:' + str(ex) + traceback.format_exc())
        except urllib.error.URLError as ex:
            self._log.error('error:' + str(ex) + traceback.format_exc())

        return ""

    def __run_docker_v100(self):
        try:
            job_name = "databaas_%d_%d" % (self._schedule_id, int(time.time()))
            if "--v100_job_name" in self._config_map:
                job_name = self._config_map["--v100_job_name"]
            
            if "--v100_job_image" not in self._config_map:
                return False
            
            image = self._config_map["--v100_job_image"]
            if "--v100_job_command" not in self._config_map:
                return False

            job_command = self._config_map["--v100_job_command"]
            gpu = 0
            if "--v100_job_gpu" in self._config_map:
                gpu = int(self._config_map["--v100_job_gpu"])

            cpu = 1
            if "--v100_job_cpu" in self._config_map:
                cpu = int(self._config_map["--v100_job_cpu"])

            memory = 2
            if "--v100_job_memory" in self._config_map:
                memory = int(self._config_map["--v100_job_memory"])
            
            self.__v100_user_id = "585398485541453824"
            if "--v100_job_userid" in self._config_map:
                self.__v100_user_id = self._config_map["--v100_job_userid"]

            default_conf_content = '[run]\\n'
            for key in self._config_map:
                if key.strip() == "--v100_job_command":
                    continue

                val = self._config_map[key]
                val.replace(' ', '')
                default_conf_content += "%s=%s\\n" % (key, val)

            if default_conf_content == '':
                return True
            
            job_command += " \"" + default_conf_content + "\""
            # 要请求的 URL
            node_req_url = "http://zjcp.zhejianglab.cn:38080/api/open/open-job/origin/v2/createJob?userId=585398485541453824&bizType=DROS"  #目标 URL
            # 要发送的数据，使用字典表示
            data = {
                "userId": self.__v100_user_id,
                "jobMeta": {
                    "jobName": job_name,
                    "describe": "一个推理作业",
                    "bizType": "DROS",
                    "jobType": "AI_INFERENCE",
                    "jobSpotType": "normal",
                    "subMissionId": "pr-7986679722762338304",
                    "subMissionName": "GeoCloud数据网络及计算引擎默认子任务"
                },
                "jobInfo": {
                    "networkType": "default",
                    "image": image,
                    "volumes": [
                        {
                            "volumeId": "vol-8003538313461092352",
                            "subPath": None,
                            "mountPath": "/DATA/",
                            "readOnly": False
                        }
                    ],
                    "command": job_command
                },
                "jobResource": {
                    "zoneType": "AI_GPU",
                    "spec": "GPU_V100_32GB",
                    "resourceType": "PUBLIC",
                    "jobStartCount": 2,
                    "gpu": gpu,
                    "cpu": cpu,
                    "memory": memory
                }
            }
            
            # 将字典转换为 JSON 格式
            data_json = json.dumps(data).encode('utf-8')
            # 创建一个请求对象，设置请求类型为 POST，并添加 Content-Type 头
            request = urllib.request.Request(node_req_url, data=data_json, method='POST')
            request.add_header('Content-Type', 'application/json')
            access_token = self.__get_access_token()
            if access_token == "":
                return False

            request.add_header('accessToken', self.__get_access_token())
            # 发送请求并读取响应
            response = urllib.request.urlopen(request, timeout=3).read()
            res_json = json.loads(response.decode('utf-8'))
            if res_json["code"] == 200:
                self.__v100_job_id = res_json["data"]["jobId"]
                self._task_handler = self.__v100_job_id
                return True
            
            self._log.error("commit v100 job failed code: %d, msg: %s" % (res_json["code"], res_json["msg"]))
            return False
        except urllib.error.HTTPError as ex:
            self._log.error('error:' + str(ex) + traceback.format_exc())
        except urllib.error.URLError as ex:
            self._log.error('error:' + str(ex) + traceback.format_exc())

        return False
    
    def __run_job(self):
        script_task_type = ""
        if "--script_task_type" in self._config_map:
            script_task_type = self._config_map["--script_task_type"].strip()

        ret = False
        if script_task_type == "docker_local":
            self.__job_type = task_util.ScriptJobType.LOCAL_DOCKER_JOB
            ret = self.__run_local_docker()
        elif script_task_type == "docker_v100":
            self.__job_type = task_util.ScriptJobType.V100_DOCKER_JOB
            ret = self.__run_docker_v100()
        else:
            ret = self.__run_script_job()

        self._log.info("run task ret: %d" % ret)
        return ret
