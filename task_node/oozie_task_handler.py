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
import requests
import json
from io import StringIO
from xml.sax.saxutils import escape

from hdfs import Client
from jinja2 import Template

import task_handle_base

sys.path.append('../common')
import task_util

HDFS_WEB_URL = 'http://emr2-header-1.ipa.aidigger.com:50070'
HDFS_URL = "hdfs://emr2-header-1.ipa.aidigger.com:8020"
HDFS_WF_DIR = '/apps/noah/'


class HdfsHandler(object):

    def __init__(self, hdfs_web_url=HDFS_WEB_URL):
        self.client = Client(hdfs_web_url, timeout=2000)
        self.hdfs_url = HDFS_URL

    def close(self):
        try:
            self.client._session.close()
        except Exception as e:
            pass

    def upload_hdfs_file(self, content, hdfs_path):
        content = content.encode(encoding='utf-8')
        self.client.write(hdfs_path, overwrite=True, data=content)
        self.close()
        return self.hdfs_url + hdfs_path

    def read_hdfs_file(self, hdfs_path):
        with self.client.read(hdfs_path) as reader:
            content = reader.read().decode('utf-8')
        self.close()
        return content

    def makedir(self, hdfs_dir):
        self.client.makedirs(hdfs_dir)
        self.close()

    def get_hdfs_path(self, local_path):
        return os.path.join(self.hdfs_url, local_path)


class JobHdfsHandler(HdfsHandler):
    def __init__(self, job_info, hdfs_web_url):
        super(JobHdfsHandler, self).__init__(hdfs_web_url)
        self.job_id = job_info['id']
        self.template_path = './conf/workflow.xml.j2'
        self.job_info = job_info

    def get_hdfs_dir(self):
        namespace = "production"
        hdfs_dir = os.path.join(HDFS_WF_DIR, namespace, str(self.job_id))
        return hdfs_dir

    @property
    def workflow_path(self):
        return os.path.join(self.get_hdfs_dir(), 'workflow.xml')

    @property
    def config_path(self):
        return os.path.join(self.get_hdfs_dir(), 'configs.json')

    def mk_hdfs_dir(self):
        hdfs_dir = self.get_hdfs_dir()
        self.makedir(hdfs_dir)
        return hdfs_dir

    def download_workflow_xml_content(self):
        return self.read_hdfs_file(self.workflow_path)

    def download_config_content(self):
        return self.read_hdfs_file(self.config_path)

    def upload_workflow_xml(self):
        context = self.get_workflow_xml_context()
        workflow_content = self.generate_content_from_tmpl(self.template_path, context)
        return self.upload_hdfs_file(workflow_content, self.workflow_path)

    def upload_config(self):
        config_content = json.dumps(self.job_info['configs'])
        return self.upload_hdfs_file(config_content, self.config_path)

    @classmethod
    def generate_content_from_tmpl(cls, template_path, configs):
        with open(template_path) as f:
            t = Template(f.read())
            content = t.render(**configs)
        return content

    def get_config_content(self):
        return json.dumps(self.job_info['configs'])

    def get_workflow_xml_context(self):
        config = {
            'work_name': self.job_info['work_name'],
            'config_path': self.config_path,
            'hdfs_config_path': self.get_hdfs_path(self.config_path),
        }
        return config

    def generate_upload_workflow_files(self):
        self.mk_hdfs_dir()
        self.upload_config()
        self.upload_workflow_xml()


class OozieTaskHandler(task_handle_base.TaskHandleBase):
    """
        启动script任务，非阻塞
        每一种任务都需要重新创建实例，线程不安全
    """

    def __init__(self, config):
        task_handle_base.TaskHandleBase.__init__(self, config)
        self.__oozie_url = 'http://emr2-header-2.ipa.aidigger.com:11000/oozie'
        self.__default_dict = {'user.name': 'noah'}
        self.__timeout = 2000
        self.__oozie_job_id = None

    def run_task(self, task_info):
        self._job_status = task_util.TaskState.TASK_FAILED
        self._old_job_status = task_util.TaskState.TASK_READY
        while True:
            if not self._init_task(task_info):
                err_log = ("init task failed!")
                self._log.error(err_log)
                self._add_error_log(err_log)
                break

            if self._task_type != task_util.TaskType.OOZIE_TYPE:
                err_log = ("this is just for oozie[%d], but now[%d]" %
                           (task_util.TaskType.OOZIE_TYPE, self._task_type))
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
        task_handler = None
        if self.__oozie_job_id is not None:
            task_handler = "%s" % self.__oozie_job_id

        if not self._write_task_status_to_db(
                self._job_status,
                self._old_job_status,
                task_handler=task_handler,
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
            return False

        task_handle_base.TaskHandleBase.stop_task(self)
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
        self.__job_work_dir = self._get_work_dir()
        if self.__job_work_dir is None:
            return task_util.TaskState.TASK_FAILED

        ret = self.__get_task_status(task_info)
        err_log_file = os.path.join(
            self.__job_work_dir,
            "trace.log")
        self._write_error_log_to_file(err_log_file)
        return ret

    def __get_proceeding(self):
        return "oozie no proceeding."

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
        url = '%s/v1/job/%s?action=kill' % (self.__oozie_url, self._task_handler)
        resp = requests.put(url, timeout=self.__timeout)
        if resp.status_code != 200:
            err_log = ("stop oozie task failed[%s]" % url)
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False

        return True

    def __get_task_status(self, task_info):
        action = '/v1/job/%s?show=info&timezone=GMT' % self._task_handler
        url = self.__oozie_url + action
        resp = requests.get(url, timeout=self.__timeout)
        if resp.status_code != 200:
            err_log = ("task time out[%s]" % str(task_info))
            self._log.info(err_log)
            self._add_error_log(err_log)
            return task_util.TaskState.TASK_RUNNING

        status = resp.json()['status']
        if status == 'PREP':
            status = task_util.TaskState.TASK_RUNNING
        elif status == 'RUNNING':
            status = task_util.TaskState.TASK_RUNNING
        elif status == 'SUCCEEDED':
            status = task_util.TaskState.TASK_SUCCEED
        else:
            self._add_error_log(json.dumps(resp.json(), sort_keys=True, indent=2))
            status = task_util.TaskState.TASK_FAILED

        if status in (
                task_util.TaskState.TASK_FAILED,
                task_util.TaskState.TASK_SUCCEED):
            if not self._write_task_status_to_db(
                    status,
                    task_util.TaskState.TASK_RUNNING):
                err_log = ("write_start_task_status_to_db failed!")
                self._log.warn(err_log)
                self._add_error_log(err_log)
                status = task_util.TaskState.TASK_FAILED

        return status

    def __xml_config_gen(self, dic):
        sio = StringIO()
        print('<?xml version="1.0" encoding="UTF-8"?>', file=sio)
        print("<configuration>", file=sio)
        # if dic's key contains <,>,& then it will be escaped and if dic's value contains ']]>' then ']]>' will be stripped
        for k, v in dic.items():
            print("<property>\n  <name>%s</name>\n  <value><![CDATA[%s]]></value>\n</property>\n" \
                  % (escape(k), v.replace(']]>', '')), file=sio)
        print("</configuration>", file=sio)
        sio.flush()
        sio.seek(0)
        return sio.read()

    def __run_job(self):
        if self._task_type != task_util.TaskType.OOZIE_TYPE:
            err_log = ("wrong script type:%d] " % self._task_type)
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False
        job_info = {
            "id": self._schedule_id,
            "configs": {"executor": "oozie", "stage": "oss2hive", "debug": False, "isStreaming": False,
                 "source": "wetao_sku_search", "type": "common", "name": "WetaoSearchSkuOss2Hive", "tdate": "2018-09-16"},
            "work_name": "WetaoSearchSkuOss2Hive"
        }
        job_hdfs_handler = JobHdfsHandler(job_info, HDFS_WEB_URL)
        job_hdfs_handler.generate_upload_workflow_files()
        configs = {
            'oozie.wf.application.path': job_hdfs_handler.workflow_path,
            'user.name': 'noah-%s' % self._owner_id,
        }

        url = '%s/v1/jobs' % self.__oozie_url
        default_args = self.__default_dict.copy()
        default_args.update(configs)
        xml = self.__xml_config_gen(default_args)
        resp = requests.post(
            url, data=xml, headers={'Content-Type': 'application/xml;charset=UTF-8'},
            timeout=self.__timeout)
        if resp.status_code != 201:
            self._log.error("run task failed:%s[res:%s]" % (
                url, resp.status_code))
            err_log = ("run oozie task failed:%s" % url)
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False

        self.__oozie_job_id = resp.json()['id']
        url = '%s/v1/job/%s?action=start' % (self.__oozie_url, self.__oozie_job_id)
        resp = requests.put(url, timeout=self.__timeout)
        if resp.status_code != 200:
            self._log.error("run task failed:%s[res:%s]" % (
                url, resp.status_code))
            err_log = ("run oozie task failed:%s" % url)
            self._log.error(err_log)
            self._add_error_log(err_log)
            return False

        url = '\n\noozie任务执行状态：%s/v1/job/%s?show=info&timezone=GMT\n\n' % (
            self.__oozie_url, self.__oozie_job_id)
        self._add_error_log(url)
        err_log_file = os.path.join(self.__job_work_dir, "trace.log")
        self._write_error_log_to_file(err_log_file)
        return True
