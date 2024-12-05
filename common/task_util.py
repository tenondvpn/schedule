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
import ipaddress

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
    TASK_V100_PENDING = 8  # v100 pending 状态
    TASK_V100_RUNING = 9  # v100 runing 状态

# 任务类型
class TaskType(object):
    SCRIPT_TYPE = 1
    SPARK_TYPE = 2
    OOZIE_TYPE = 3
    ODPS_TYPE = 4
    SHELL_TYPE = 5
    DOCKER_TYPE = 6
    CLICKHOUSE_TYPE = 7
    V100_TYPE = 8
    LOCAL_DOCKER_TYPE = 9

class ScriptJobType(object):
    PYTHON_SCRIPT = 1
    LOCAL_DOCKER_JOB = 2
    V100_DOCKER_JOB = 3

# 每种任务最大执行数
class MaxTaskNum(object):
    MAX_SCRIPT_TASK_NUM = 30
    MAX_SPARK_TASK_NUM = 30
    MAX_OOZIE_TASK_NUM = 30
    MAX_ODPS_TASK_NUM = 30
    MAX_SHELL_TASK_NUM = 30
    MAX_DOCKER_TASK_NUM = 30
    MAX_CLICKHOUSE_TASK_NUM = 30
    MAX_V100_TASK_NUM = 300
    MAX_LOCAL_DOCKER_TASK_NUM = 30

class EachUserTaskLimitNum(object):
    SCRIPT_TASK_NUM = 10  # 每个用户最大执行任务数
    SPARK_TASK_NUM = 10  # 每个用户最大执行任务数
    OOZIE_TASK_NUM = 10  # 每个用户最大执行任务数
    ODPS_TASK_NUM = 10  # 每个用户最大执行任务数
    SHELL_TASK_NUM = 10  # 每个用户最大执行任务数
    DOCKER_TASK_NUM = 10  # 每个用户最大执行任务数
    CLICKHOUSE_TASK_NUM = 10  # 每个用户最大执行任务数
    V100_TASK_NUM = 300  # 每个用户最大执行任务数
    LOCAL_DOCKER_TASK_NUM = 10  # 每个用户最大执行任务数

class PipelineType(object):
    CREATE_BY_ARK_TOOLS = 0  # 在前端工具创建
    CREATE_BY_AUTO_WORK = 1  # 后端动态创建


class FileCommandRet(object):
    FILE_COMMAND_SUCC = 0  # 操作文件成功
    FILE_IS_NOT_EXISTS = 1  # 文件不存在
    FILE_PATH_IS_NOT_EXISTS = 2  # 文件路径不存在（不包括文件名）
    FILE_READ_ERROR = 3  # 读取文件失败
    FILE_WRITE_ERROR = 4  # 写文件失败


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

# 全局变量（注意，此处的object和C不同，
# 不是常量，而是可改变常量，需要用户保证其正确性）
class CONSTANTS(object):
    GLOBAL_STOP = False  # 全局停止标志
    HOLD_FOR_ZOOKEEPER_LOCK = True  # 获取zookeeper权限标志
    RUN_TIME_FORMAT = "%Y%m%d%H%M"
    TPL_CONFIG_NAME = "_tpl"  # tpl输入文件名
    RUN_TIME_LENTGH = 12  # 运行时间格式长度，统一为YYYYmmddHHMM的格式
    PATH_LOCAL_START_WITH = '/'  # 本地路径只支持绝对路径，不支持相对路径
    TASK_PRIORITY_MIN = 5  # 最小优先级
    TASK_PRIORITY_MAX = 10  # 最大优先级
    SCRIPT_DEFSULT_TPL_CONF_NAME = "run.conf.tpl"  # script默认配置tpl
    SCRIPT_DEFSULT_CONF_NAME = "run.conf"  # script默认配置文件名
    SCRIPT_DEFSULT_PYTHON_FILE = "run.py"  # python默认执行文件名
    SHELL_DEFULT_RUN_FILE = "run.sh"  # python默认执行文件名
    SHELL_DEFSULT_TPL_CONF_NAME = "run.conf.tpl"  # script默认配置tpl
    SHELL_DEFSULT_CONF_NAME = "run.conf"  # script默认配置文件名

    CT_TIME_SPLIT_NUM = 5
    ODPS_MAP_REDUCE_JAR_CONF_NAME = '_jar'  # odps-mr任务的jar包名配置

    ODPS_SQL_CMD_STR = (""" %s --project=%s --endpoint=%s """
                       """--instance-priority=%s -u %s -p %s -e"%s" """)
    ODPS_XLIB_CMD_STR = (""" %s --project %s --endpoint %s  """
                        """ --access-id %s --access-key """
                        """ %s xlib "execfile('%s')" """)
    ODPS_SQL_CMD_FILE_STR = (""" %s --project=%s --endpoint=%s """
                       """--instance-priority=%s -u %s -p %s -f %s """)
    ODPS_SQL_FILE_NAME = "odps.sql"
    ODPS_SPARK_TPL_CONF_NAME = "spark.conf.tpl"  # odps_spark默认配置tpl
    ODPS_SPARK_CONF_NAME = "spark.conf"  # odps_spark默认配置tpl

    CLICKHOUSE_CMD_STR = (""" %s --host %s --port %s %s %s --multiquery < %s""")
    CLICKHOUSE_SQL_FILE_NAME = "ck.sql"

    MAX_RANDOM_TASKID_MIN = 2000000000
    MAX_RANDOM_TASKID_MAX = 2147483647
    WORK_DIR_SPACE_VALID = 1024 * 1024 * 1024
    TASK_RETRY_MAX_TIME_PERIOD = 3 * 60 * 60  # 重试最大时间间隔
    TASK_RETRY_STEP_INC_TIME = 200  # 每次重试递增休眠时间60s
    TASK_RETRY_STEP_INC_RATIO = 3  # 每次重试递增休眠时间系数,
    # 即此次休眠时间是上次休眠的1.5倍
    MAX_CONFIG_VALUE_LENGTH = 4096000

    HTTP_RESPONSE_WAIT = 'WAIT'
    HTTP_RETRY_MSG = 'RETRY'
    TASK_DISPATCHER_IS_RUNNING = False

    MAX_FILE_DOWNLOAD_SIZE = 1024 * 1024 * 1024  # 下载文件最大size，1G

    DOCKER_CONF_DIR = "/home/merak/tools/"  # docker默认配置目录
    DOCKER_CONF_NAME = "config"  # docker默认配置：config
    DOCKER_POD_CONF_NAME = "pod.yaml"  # docker默认配置：config
    DOCKER_RUN_CMD_FILE_STR = (""" %s --kubeconfig=%s run %s --image=%s """)
    DOCKER_RUN_CMD_GENERATE_YAML_FILE_STR = (""" %s --kubeconfig=%s run %s --image=%s --restart=Never --dry-run=client --env CONFIG='%s' -o yaml > %s """)
    DOCKER_EXEC_CMD_STR = (""" %s --kubeconfig=%s exec %s -- /bin/bash -c %s """)
    DOCKER_APPLY_CMD_FILE_STR = (""" %s --kubeconfig=%s apply -f %s """)
    DOCKER_DELETE_CMD_FILE_STR = (""" %s --kubeconfig=%s delete pod %s """)
    DOCKER_CHECK_STATUS_CMD_FILE_STR = (""" %s --kubeconfig=%s describe pod %s """)
    DOCKER_CONF_CMD_LOGS = (""" %s --kubeconfig=%s logs %s --all-containers=true""")

    V100_MAX_PENDING_COUNT = 3

# 任务类型与任务限制数对应
global_task_limit_map = {
    TaskType.SCRIPT_TYPE: MaxTaskNum.MAX_SCRIPT_TASK_NUM,
    TaskType.SPARK_TYPE: MaxTaskNum.MAX_SPARK_TASK_NUM,
    TaskType.OOZIE_TYPE: MaxTaskNum.MAX_OOZIE_TASK_NUM,
    TaskType.ODPS_TYPE: MaxTaskNum.MAX_ODPS_TASK_NUM,
    TaskType.SHELL_TYPE: MaxTaskNum.MAX_SHELL_TASK_NUM,
    TaskType.DOCKER_TYPE: MaxTaskNum.MAX_DOCKER_TASK_NUM,
    TaskType.CLICKHOUSE_TYPE: MaxTaskNum.MAX_CLICKHOUSE_TASK_NUM,
    TaskType.V100_TYPE: MaxTaskNum.MAX_V100_TASK_NUM,
    TaskType.LOCAL_DOCKER_TYPE: MaxTaskNum.MAX_LOCAL_DOCKER_TASK_NUM,
}

# 每个用户最大任务类型与任务限制数对应
global_each_user_task_limit_map = {
    TaskType.SCRIPT_TYPE: EachUserTaskLimitNum.SCRIPT_TASK_NUM,
    TaskType.SPARK_TYPE: EachUserTaskLimitNum.SPARK_TASK_NUM,
    TaskType.OOZIE_TYPE: EachUserTaskLimitNum.OOZIE_TASK_NUM,
    TaskType.ODPS_TYPE: EachUserTaskLimitNum.ODPS_TASK_NUM,
    TaskType.SHELL_TYPE: EachUserTaskLimitNum.SHELL_TASK_NUM,
    TaskType.DOCKER_TYPE: EachUserTaskLimitNum.DOCKER_TASK_NUM,
    TaskType.CLICKHOUSE_TYPE: EachUserTaskLimitNum.CLICKHOUSE_TASK_NUM,
    TaskType.V100_TYPE: EachUserTaskLimitNum.V100_TASK_NUM,
    TaskType.LOCAL_DOCKER_TYPE: EachUserTaskLimitNum.LOCAL_DOCKER_TASK_NUM,
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
        return socket.gethostbyname(socket.gethostname())

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
    def is_lan(ip):
        return False
        # try:
        #     res = ipaddress.ip_address(ip.strip()).is_private
        #     print(ip + ":" + str(res))
        #     return res
        # except Exception as e:
        #     print(ip + ": not valid")
        #     return False