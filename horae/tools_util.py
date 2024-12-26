###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import os
import string
import re
import socket
import datetime
import time

import crontab


# 任务状态
class TaskState(object):
    TASK_WAITING = 0  # 等待中
    TASK_RUNNING = 1  # 正在执行
    TASK_SUCCEED = 2  # 执行成功
    TASK_FAILED = 3  # 执行失败
    TASK_TIMEOUT = 4  # 任务超时
    TASK_READY = 5  # 任务已经写入ready_task，等待调度
    TASK_STOPED_BY_USER = 6  # 任务被用户停止
    TASK_PREV_FAILED = 7  # 上游任务失败


# 错误码
class HttpStatus(object):
    SUCCESS = 0  # 成功
    FAILED = 1  # 失败
    PIPELINE_NOT_EXISTS = 2  # 流程不存在
    PIPELINE_EXISTS = 3  # 流程已经存在
    TASK_NOT_EXISTS = 4  # 任务不存在
    TASK_EXISTS = 5  # 任务已经存在
    PROCESSOR_NOT_EXISTS = 6  # 算子不存在
    PROCESSOR_EXISTS = 7  # 算子已经存在
    AUTHORISATION_FAILED = 8  # 权限失败


# 任务类型
class TaskType(object):
    SCRIPT_TYPE = 1
    SPARK_TYPE = 2
    OOZIE_TYPE = 3


# 每种任务最大执行数
class MaxTaskNum(object):
    MAX_SCRIPT_TASK_NUM = 30
    MAX_SPARK_TASK_NUM = 30
    MAX_OOZIE_TASK_NUM = 30


class EachUserTaskLimitNum(object):
    SCRIPT_TASK_NUM = 10  # 每个用户最大执行任务数
    SPARK_TASK_NUM = 10  # 每个用户最大执行任务数
    OOZIE_TASK_NUM = 10  # 每个用户最大执行任务数


class PipelineType(object):
    CREATE_BY_ARK_TOOLS = 0  # 在前端工具创建
    CREATE_BY_AUTO_WORK = 1  # 后端动态创建


class FileCommandRet(object):
    FILE_COMMAND_SUCC = 0  # 操作文件成功
    FILE_IS_NOT_EXISTS = 1  # 文件不存在
    FILE_PATH_IS_NOT_EXISTS = 2  # 文件路径不存在（不包括文件名）
    FILE_READ_ERROR = 3  # 读取文件失败
    FILE_WRITE_ERROR = 4  # 写文件失败


class MonitorWay(object):
    NO_MONITOR = 0
    MONITOR_TEL_ONLY = 1
    MONITOR_EMAIL_ONLY = 2
    MONITOR_ALL = 3


# 全局变量（注意，此处的object和C不同，
# 不是常量，而是可改变常量，需要用户保证其正确性）
class CONSTANTS(object):
    GLOBAL_STOP = False  # 全局停止标志
    HOLD_FOR_ZOOKEEPER_LOCK = True  # 获取zookeeper权限标志
    RUN_TIME_FORMAT = "%Y%m%d%H%M"
    TPL_CONFIG_NAME = "_tpl"  # tpl输入文件名
    RUN_TIME_LENTGH = 12  # 运行时间格式长度，统一为YYYYmmddHHMM的格式
    DEFAULT_INSTANCE_COUNT = 24  # 默认处理实例数
    TASK_PRIORITY_MIN = 5  # 最小优先级
    TASK_PRIORITY_MAX = 10  # 最大优先级
    SCRIPT_DEFSULT_TPL_CONF_NAME = "run.conf.tpl"  # script默认配置tpl
    SCRIPT_DEFSULT_CONF_NAME = "run.conf"  # script默认配置文件名
    SCRIPT_DEFSULT_PYTHON_FILE = "run.py"  # python默认执行文件名

    CT_TIME_SPLIT_NUM = 5

    MAX_RANDOM_TASKID_MIN = 2000000000
    MAX_RANDOM_TASKID_MAX = 2147483647
    WORK_DIR_SPACE_VALID = 1024 * 1024 * 1024
    TASK_RETRY_MAX_TIME_PERIOD = 3 * 60 * 60  # 重试最大时间间隔3小时
    TASK_RETRY_STEP_INC_TIME = 60  # 每次重试递增休眠时间60s
    TASK_RETRY_STEP_INC_RATIO = 1.5  # 每次重试递增休眠时间系数,
    # 即此次休眠时间是上次休眠的1.5倍
    PIPELINE = "pipeline"
    PROCESSOR = "processor"
    PROJECT = "project"
    MAX_RESTART_TASK_NUM = 2500  # 最多手动启动的任务数
    MAX_RUNNING_TASK_NUM = 2500  # 最多执行任务数
    HTTP_RESPONSE_WAIT = 'WAIT'
    PROJECT_MAX_LENGTH = 16
    PROJECT_DEFAULT_NAME = '默认项目'


class RunTimeLength(object):
    DAY_RUN_TIME_LEN = 8
    HOUR_RUN_TIME_LEN = 10
    MINIUTE_RUN_TIME_LEN = 12


class UserPermissionType(object):
    NO_AUTH = 0
    READ = 1
    WRITE = 2
    CONFIRMING = 3  # 申请权限确认中
    READ_STR = "read"
    WRITE_STR = "write"


# 权限操作动作类型
class AuthAction(object):
    GRANT_AUTH_TO_OTHER = 0  # 授予权限给其他用户
    CONFIRM_APPLY_AUTH = 1  # 用户确认其他用户的权限申请
    REJECT_APPLY_AUTH = 2  # 用户拒绝其他用户的权限申请
    APPLY_AUTH = 3  # 用户申请权限
    TAKE_BACK_AUTH = 4  # 用户收回授予的权限


# 任务状态
class TaskState(object):
    TASK_WAITING = 0  # 等待中
    TASK_RUNNING = 1  # 正在执行
    TASK_SUCCEED = 2  # 执行成功
    TASK_FAILED = 3  # 执行失败
    TASK_TIMEOUT = 4  # 任务超时
    TASK_READY = 5  # 任务已经写入ready_task，等待调度
    TASK_STOPED_BY_USER = 6  # 任务被用户停止
    TASK_PREV_FAILED = 7  # 上游任务失败


class PROCESSOR_TOP_TYPE(object):
    USER_OWNER_PROC = -1
    SHARED_PROC = -2
    PUBLIC_PROC = -3


global_processor_top_type_map = {
    PROCESSOR_TOP_TYPE.USER_OWNER_PROC: "我创建的",
    PROCESSOR_TOP_TYPE.SHARED_PROC: "共享给我的",
    PROCESSOR_TOP_TYPE.PUBLIC_PROC: "标准库"
}

# 任务类型与任务限制数对应
global_task_limit_map = {
    TaskType.SCRIPT_TYPE: MaxTaskNum.MAX_SCRIPT_TASK_NUM,
}

# 每个用户最大任务类型与任务限制数对应
global_each_user_task_limit_map = {
    TaskType.SCRIPT_TYPE: EachUserTaskLimitNum.SCRIPT_TASK_NUM,
    TaskType.SPARK_TYPE: EachUserTaskLimitNum.SPARK_TASK_NUM,
}

global_status_info_map = {
    TaskState.TASK_WAITING: "TASK_WAITING",
    TaskState.TASK_RUNNING: "TASK_RUNNING",
    TaskState.TASK_SUCCEED: "TASK_SUCCEED",
    TaskState.TASK_FAILED: "TASK_FAILED",
    TaskState.TASK_TIMEOUT: "TASK_TIMEOUT",
    TaskState.TASK_READY: "TASK_READY",
    TaskState.TASK_STOPED_BY_USER: "TASK_STOPED_BY_USER",
    TaskState.TASK_PREV_FAILED: "TASK_PREV_FAILED"
}

# 配置中支持的时间格式
global_time_format_list = [
    '%year%',
    '%yyyy%',
    '%Y%',
    '%month%',
    '%mm%',
    '%m%',
    '%day%',
    '%dd%',
    '%d%',
    '%hour%',
    '%hh%',
    '%H%',
    '%minute%',
    '%MM%',
    '%M%',
]


class Singleton(object):
    def __new__(cls, *args, **kw):
        if not hasattr(cls, "_instance"):
            orig = super(Singleton, cls)
            cls._instance = orig.__new__(cls)

        return cls._instance


class AutoLock(object):
    def __init__(self, lock):
        self.__lock = lock
        self.__lock.acquire()

    def __del__(self):
        self.__lock.release()


class StaticFunction(object):
    @staticmethod
    def strip_with_one_space(in_str):
        """
            将字符串中的\t以及多余空格全部替换为一个空格间隔
        """
        return ' '.join(filter(lambda x: x, in_str.split(' ')))

    @staticmethod
    def strip_with_nothing(in_str):
        """
            将字符串中的\t以及多余空格全部去除
        """
        return ''.join(filter(lambda x: x, in_str.split(' ')))

    @staticmethod
    def get_local_ip():
        """
            获取本地ip地址
        """
        tried_num = 0
        while tried_num < 3:
            try:
                return socket.gethostbyname(socket.gethostname())
            except Exception as ex:
                tried_num += 1
                time.sleep(1)
                continue
        raise Exception("get local ip failed after tried 3 times")

    @staticmethod
    def get_all_content_from_file(file_path, param="r"):
        """
            读取文件，并返回所有数据
            file_path： 需要读取的文件路径
            param： 读文件参数
            return: 失败 None 成功：文件内容
        """
        if not os.path.exists(file_path):
            return FileCommandRet.FILE_PATH_IS_NOT_EXISTS, ''

        fd = open(file_path, param)
        try:
            return FileCommandRet.FILE_COMMAND_SUCC, fd.read()
        except Exception as ex:
            return FileCommandRet.FILE_WRITE_ERROR, ''
        finally:
            fd.close()

    @staticmethod
    def get_file_content_with_start_and_len(file_path, start, len, param="r"):
        if not os.path.exists(file_path):
            return FileCommandRet.FILE_PATH_IS_NOT_EXISTS, ''

        fd = open(file_path, param)
        try:
            fd.seek(start)
            return FileCommandRet.FILE_COMMAND_SUCC, fd.read(len)
        except Exception as ex:
            return FileCommandRet.FILE_WRITE_ERROR, ''
        finally:
            fd.close()

    @staticmethod
    def write_content_to_file(file_path, content, param="w"):
        """
            将内容写入文件
            content： 写入内容
            file_path： 文件路径
            param： 写文件参数
            return: FileCommandRet
        """
        path = os.path.dirname(os.path.abspath(file_path))
        if not os.path.exists(path):
            return FileCommandRet.FILE_PATH_IS_NOT_EXISTS

        fd = open(file_path, param)
        try:
            fd.write(content)
            return FileCommandRet.FILE_COMMAND_SUCC
        except Exception as ex:
            return FileCommandRet.FILE_WRITE_ERROR
        finally:
            fd.close()

    @staticmethod
    def replace_str_with_regex(src_content, pattern, kv_pair_map):
        """
            通过正则表达式pattern，将输入内容按照kv_pair_map，替换关键字内容
            src_content: 需要替换的源字符串
            pattern: 匹配的正则表达式
            kv_pair_map: 替换kv对map
            return: 失败: None, 成功：正确替换内容

            Basic Example:
                src_content = 
                    " {
                        "part": "${part}",
                        "sep": "${sep}",
                        "/SetLimit/core": "unlimited",
                        "/SetEnv/LD_LIBRARY_PATH": "/usr/ali/java/jre/",
                        "table": "${table}",
                       }
                    "
                pattern = '\$\{[^}]*\}'
                kv_pair_map = {
                    "part" : "test_part",
                    "sep" : "test_sep",
                }

                print(task_util.StaticFunction.replace_str_with_regex(
                        src_content, 
                        pattern, 
                        kv_pair_map))
                输出：
                    " {
                        "part": "test_part",
                        "sep": "test_sep",
                        "/SetLimit/core": "unlimited",
                        "/SetEnv/LD_LIBRARY_PATH": "/usr/ali/java/jre/",
                        "table": "",  # 如果kv_pair_map中没有此关键字，替换为空
                       }
                    "
        """
        try:
            template = string.Template(src_content)
            tmp_content = template.safe_substitute(kv_pair_map)
            regex_object = re.compile(pattern)
            return regex_object.sub("", tmp_content)
        except Exception as ex:
            return None

    @staticmethod
    def get_path_sub_regex_pattern(full_path, parent_dir):
        if len(full_path) <= len(parent_dir):
            return None
        if parent_dir[-1] != '/':
            parent_dir = parent_dir + '/'
        find_pos = full_path.find("/", len(parent_dir))
        if find_pos == -1:
            return full_path[len(parent_dir): len(full_path)]
        return full_path[len(parent_dir): find_pos]

    @staticmethod
    def get_now_format_time(format):
        now_time = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
        return now_time.strftime(format)

    @staticmethod
    def remove_empty_file(file_path):
        if not os.path.exists(file_path):
            return False

        if os.path.isdir(file_path):
            return False

        if os.path.getsize(file_path) <= 0:
            os.remove(file_path)
        return True

    @staticmethod
    def get_next_ct_run_time(ct_time):
        if ct_time.strip() == '':
            return None

        crontab_job = crontab.CronTab(tab='').new(command='/usr/bin/echo')
        crontab_job.setall(ct_time.strip())
        if not crontab_job.is_valid():
            return None
        now_time = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
        return crontab_job.schedule(now_time)
