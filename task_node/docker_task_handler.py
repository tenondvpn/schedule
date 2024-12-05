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
import re
import task_handle_base
import datetime
sys.path.append('../common')
import task_util
import no_block_sys_cmd
import yaml
import json
import configparser

class DockerTaskHandler(task_handle_base.TaskHandleBase):
    """
        启动docker任务，非阻塞
        每一种任务都需要重新创建实例，线程不安全
    """
    def __init__(self, config):
        task_handle_base.TaskHandleBase.__init__(self, config)
        self.__job_work_dir = None
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

            if self._task_type != task_util.TaskType.DOCKER_TYPE:
                err_log = (
                        "this is just for docker[%d],but now[%d]" % \
                        (task_util.TaskType.DOCKER_TYPE, self._task_type))
                self._log.error(err_log)
                self._add_error_log(err_log)
                break

            # 准备工作路径
            self.__job_work_dir = self._prepair_work_dir()
            self.__k8s_config =  self.__job_work_dir + "./k8s_kubeconfig.yml"
            self.__k8s_pod = self.__job_work_dir + "./docker_pod.yml"
            self._add_error_log("work_ip: %s\nwork_dir: %s\n" % (
                    task_util.StaticFunction.get_local_ip(), 
                    self.__job_work_dir))

            # if not self._download_package(self.__job_work_dir):
            #     err_log = ("download job package failed!")
            #     self._log.error(err_log)
            #     self._add_error_log(err_log)
            #     break

            # 初始化，包括配置中的时间转化，盘古路径处理，
            if not self._init(self.__job_work_dir):
                err_log = ("docker job handle config failed!")
                self._log.error(err_log)
                self._add_error_log(err_log)
                break

            # run.conf.tpl转化为run.conf
            if not self.__handle_run_conf():
                break

            # 执行kubectl，非阻塞执行，需要定时获取执行状态
            if not self.__run_job():
                err_log = ("docker run job failed![%s]" % str(task_info))
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
        
        self.__k8s_config =  self.__job_work_dir + "./k8s_kubeconfig.yml"
        self.__k8s_pod = self.__job_work_dir + "./docker_pod.yml"
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
        
        self.__k8s_config =  self.__job_work_dir + "./k8s_kubeconfig.yml"
        self.__k8s_pod = self.__job_work_dir + "./docker_pod.yml"
        ret = self.__check_docker_status(task_info)
        # 查看k8s pod日志写到stdout
        cmd = task_util.CONSTANTS.DOCKER_CONF_CMD_LOGS % (
            self.__docker_cmd,
            self.__k8s_config,
            self._config_map["__metadata_labels_run"])
        self._add_error_log(cmd)
        stdout_file = os.path.join(self.__job_work_dir, "stdout.log")
        self.__no_block_cmd.run_in_background(
            cmd,
            stdout_file,
            None)
        err_log_file = os.path.join(
                self.__job_work_dir, 
                "trace.log")
        self._write_error_log_to_file(err_log_file)
        if not self._write_task_status_to_db(
                ret,
                task_util.TaskState.TASK_RUNNING):
            err_log = ("write_start_task_status_to_db failed!")
            self._log.warn(err_log)
            self._add_error_log(err_log)
            status = task_util.TaskState.TASK_FAILED
            self._set_task_status_failed()

        return ret

    def get_proceeding(self, task_info):
        if not self._init_task(task_info):
            return False
 
        if not self._handle_config():
            return False

        self.__job_work_dir = self._get_work_dir()
        if self.__job_work_dir is None:
            return False
        ret = self.__check_docker_status(task_info)
        err_log_file = os.path.join(
                self.__job_work_dir, 
                "trace.log")
        self._write_error_log_to_file(err_log_file)
        return ret
    
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
        task_handle_base.TaskHandleBase.stop_task(self)
        status = self.__check_docker_status(task_info)
        self.__docker_cmd = "/usr/local/bin/kubectl"
        if "__kubectl_path" in self._config_map:
            self.__docker_cmd = self._config_map["__kubectl_path"]
        cmd = task_util.CONSTANTS.DOCKER_DELETE_CMD_FILE_STR % (
                self.__docker_cmd,
                self.__k8s_config,
                self._config_map["__metadata_labels_run"])
        status, stdout, stderr = self._run_command(
                self.__job_work_dir,
                cmd,
                None,
                None)
        self._add_error_log("%s\n%s\n%s\n%s" % (
                cmd, status, stdout, stderr))
        return True

    def __replace_config(self):
        # ret, k8s_kubeconfig_template_content = task_util.StaticFunction.get_all_content_from_file(self.__job_work_dir + "./k8s_kubeconfig_template.yml")
        # if ret != task_util.FileCommandRet.FILE_COMMAND_SUCC:
        #     self._add_error_log("open file failed: %s" % self.__k8s_config)
        #     return False
        k8s_kubeconfig_template_content = ('''
apiVersion: v1
clusters:
- cluster:
    server: ${__k8s_endpoint}
    certificate-authority-data: ${__certificate_authority_data}
  name: ${__clusters_name}
contexts:
- context:
    cluster: ${__contexts_cluster}
    user: "${__contexts_user}"
  name: ${__contexts_name}
current-context: ${__current_context}
kind: Config
preferences: {}
users:
- name: "${__users_name}"
  user:
    client-certificate-data: ${__users_client_certificate_data}
    client-key-data: ${__users_client_key_data}''')
        des_kubeconfig = task_util.StaticFunction.replace_str_with_regex(
            k8s_kubeconfig_template_content, 
            self._conf_replace_pattern, 
            self._config_map)
        ret = task_util.StaticFunction.write_content_to_file(self.__k8s_config, des_kubeconfig)
        if ret != task_util.FileCommandRet.FILE_COMMAND_SUCC:
            self._add_error_log("write file failed: %s" % self.__k8s_config)
            return False
        
        # ret, k8s_pod_template_content = task_util.StaticFunction.get_all_content_from_file(self.__job_work_dir + "./pod_template.yaml")
        # if ret != task_util.FileCommandRet.FILE_COMMAND_SUCC:
        #     self._add_error_log("open file failed: %s" % "./pod_template.yaml")
        #     return False

#         k8s_pod_template_content = ('''
# apiVersion: v1
# kind: Pod
# metadata:
# creationTimestamp: null
# labels:
#     run: ${__metadata_labels_run}
# name: ${__metadata_labels_run}
# spec:
# containers:
# - image: ${__k8s_docker_image}
#     name: ${__metadata_labels_run}
#     resources: {}
# - image: ${__k8s_docker_image}
#     name: ${__metadata_labels_run}
#     resources: {}
# dnsPolicy: ClusterFirst
# restartPolicy: Never
# status: {}
#         ''')
#         des_pod = task_util.StaticFunction.replace_str_with_regex(
#             k8s_pod_template_content, 
#             self._conf_replace_pattern, 
#             self._config_map)
#         self._log.info("des_pod: " + des_pod + ", work dir: " + self.__k8s_pod)
#         print("des_pod: " + des_pod + ", work dir: " + self.__k8s_pod)
#         ret = task_util.StaticFunction.write_content_to_file(self.__k8s_pod, des_pod)
#         if ret != task_util.FileCommandRet.FILE_COMMAND_SUCC:
#             self._add_error_log("write file failed: %s" % (self.__k8s_pod))
#             return False
        
        # ret = task_util.StaticFunction.write_content_to_file(self.__job_work_dir + "./tmep.yml", des_pod)
        # if ret != task_util.FileCommandRet.FILE_COMMAND_SUCC:
        #     self._add_error_log("write file failed: %s" % (self.__k8s_pod))
        #     return False

        # cmd = "mv " + self.__job_work_dir + "./tmep.yml" + " " + self.__k8s_pod
        # stdout, stderr, run_back_ret = self.__no_block_cmd.run_once(
        #     cmd)
        
        # if run_back_ret != 0:
        #     self._add_error_log("write file failed: %s" % cmd)
        #     self._add_error_log(stderr)
        #     return False
        
        return True

    def __read_config_as_string(self):
        config_file = os.path.join(
            self.__job_work_dir,
            task_util.CONSTANTS.SCRIPT_DEFSULT_CONF_NAME)
        """从配置文件中读取并返回格式化的字符串"""
        config = configparser.ConfigParser()
        config.read(config_file)

        # 构建结果字符串
        result_parts = []
        for key, value in config['run'].items():
            result_parts.append(f"{key}={value}")

        result = "&&".join(result_parts)
        return result

    def __run_job(self):
        self.__docker_cmd = "/usr/local/bin/kubectl"
        if "__kubectl_path" in self._config_map:
            self.__docker_cmd = self._config_map["__kubectl_path"]

        if not self.__replace_config():
            return False
        
        # docker_config_file = self.__k8s_pod
        # if not os.path.exists(docker_config_file):
        #     err_log = ("docker config file is not exists.[%s]" % docker_config_file)
        #     self._log.error(err_log)
        #     self._add_error_log(err_log)
        #     return False
        
        image_name = self._config_map["__k8s_docker_image"]
        config_content = self.__read_config_as_string()
        # 如果需要过滤掉空字符串
        cmd = task_util.CONSTANTS.DOCKER_RUN_CMD_GENERATE_YAML_FILE_STR % (
            self.__docker_cmd,
            self.__k8s_config,
            self._config_map["__metadata_labels_run"],
            image_name,
            config_content,
            self.__k8s_pod)
        self._add_error_log(cmd)
        stdout_file = os.path.join(self.__job_work_dir, "stdout.log")
        stderr_file = os.path.join(self.__job_work_dir, "stderr.log")
        '''
        stdout, stderr, run_back_ret = self.__no_block_cmd.run_once(
            cmd)
        '''
        run_back_ret = self.__no_block_cmd.run_without_log(cmd)
        if run_back_ret != 0:
            err_log = ("docker run cmd[%s] failed![ret:%s]" % (cmd, run_back_ret))
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False

        k8s_command =  self._config_map["__k8s_command"]
        cmd = f"""sed -i '/image:/a\\    command: ["sh", "-c", "{k8s_command} $CONFIG; sleep 60"]' {self.__k8s_pod}"""

        self._add_error_log(cmd)
        stdout_file = os.path.join(self.__job_work_dir, "stdout.log")
        stderr_file = os.path.join(self.__job_work_dir, "stderr.log")
        run_back_ret = self.__no_block_cmd.run_without_log(cmd)

        if run_back_ret != 0:
            err_log = ("docker sh cmd[%s] failed![ret:%s]" % (cmd, run_back_ret))
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False

        cmd = task_util.CONSTANTS.DOCKER_APPLY_CMD_FILE_STR % (
            self.__docker_cmd,
            self.__k8s_config,
            self.__k8s_pod)
        self._add_error_log(cmd)
        stdout_file = os.path.join(self.__job_work_dir, "trace.log")
        stderr_file = os.path.join(self.__job_work_dir, "stderr.log")
        run_back_ret = self.__no_block_cmd.run_in_background(
            cmd,
            stdout_file,
            stderr_file)
        if run_back_ret != 0:
            err_log = ("docker run cmd[%s] failed![ret:%s]" % (cmd, run_back_ret))
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False

        return True

    # 检查docker是否success，如果有一个失败则失败
    """ 
        describe pod 的返回数据Status
    """
    def __check_docker_status(self, task_info):
        self.__docker_cmd = "/usr/local/bin/kubectl"
        if "__kubectl_path" in self._config_map:
            self.__docker_cmd = self._config_map["__kubectl_path"]

        cmd = task_util.CONSTANTS.DOCKER_CHECK_STATUS_CMD_FILE_STR % (
                self.__docker_cmd,
                task_util.CONSTANTS.DOCKER_CONF_DIR + task_util.CONSTANTS.DOCKER_CONF_NAME,
                self._config_map["__metadata_labels_run"])
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
            return task_util.TaskState.TASK_FAILED

        # 使用正则表达式提取 Status 后面的状态
        match = re.search(r'Status:\s+(\w+)', stdout)
        if match:
            pod_status = match.group(1)
            if pod_status == "Running":
                return task_util.TaskState.TASK_RUNNING
            elif pod_status == "Pending":
                return task_util.TaskState.TASK_RUNNING
            elif pod_status == "Failed":
                return task_util.TaskState.TASK_FAILED
            elif pod_status == "Error":
                return task_util.TaskState.TASK_FAILED
            elif pod_status == "Succeeded":
                return task_util.TaskState.TASK_SUCCEED
            elif pod_status == "Completed":
                return task_util.TaskState.TASK_SUCCEED
            else:
                # 未知状态，记录日志
                unknown_status_log = f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Unknown status detected![stdout:{stdout}]"
                self._log.warning(unknown_status_log)
                self._add_error_log(unknown_status_log)
                return task_util.TaskState.TASK_RUNNING

if __name__ == "__main__":
    print("please run unit test in common/db_manabger")
