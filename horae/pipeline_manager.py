###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import json
import configparser
import traceback
import datetime
import urllib.request
import uuid
import time
import ipaddress
import threading

import horae.models
import crontab
#import oss2
import django

from horae import tools_sql_manager
from horae import tools_util
from horae import zk_manager
from horae import graph_manager
from kafka import KafkaConsumer
from kafka import KafkaProducer

global_queue_lock = threading.Lock()
class KafkaRequestManager(threading.Thread):
    """
        1.管理长等待的http请求
        2.多线程执行http请求，并保存执行结果
    """
    def __init__(self, config, horae_logger):
        threading.Thread.__init__(self)
        self.__log = horae_logger
        self.__config = config
        self.__kafka_servers = config.get("tools", "kafka_servers")
        if self.__kafka_servers is None or self.__kafka_servers.strip() == "":
            self.__kafka_servers = ""

        self.__msg_map = {}

    def get_data(self, msg_id):
        global_queue_lock.acquire()
        try:
            if msg_id in self.__msg_map:
                data = self.__msg_map[msg_id]
                del self.__msg_map[msg_id]
                return data
            
            return None
        except:
            pass
        finally:
            global_queue_lock.release()

    def run(self):
        if self.__kafka_servers == "":
            return
        
        self.__log.info("KafkaRequestManager thread starting...")
        # 全局退出标示字段
        consumer = KafkaConsumer("all_message",
                         bootstrap_servers=self.__kafka_servers,
                         auto_offset_reset='latest')
        while True:
            try:
                data = consumer.poll(timeout_ms=100, max_records=1)  # 拉取消息，字典类型
                if data:
                    for key in data:
                        global_queue_lock.acquire()
                        try:
                            res_map = json.loads(data[key][0].value)
                            self.__msg_map[res_map["msg_id"]] = res_map["data"]
                            self.__log.info("get data: %s" % (data[key][0].value))
                            global_queue_lock.release()
                        except:
                            pass
                        finally:
                            global_queue_lock.release()
            except:
                pass

        self.__log.info("HttpRequestHandler thread exited!")

class PipelineManager(object):
    """
        流程管理接口实现
    """

    def __init__(self, horae_logger):
        global global_queue_lock
        config = configparser.ConfigParser()
        config.read("./conf/tools.conf")
        self.__zk_manager = zk_manager.ZookeeperManager(
            hosts=config.get("zk", "hosts"),
            logger=horae_logger)
        self.__kafka_servers = config.get("tools", "kafka_servers")
        if self.__kafka_servers is None or self.__kafka_servers.strip() == "":
            self.__kafka_servers = ""

        self.__admin_ip_dir = config.get("zk", "ip_dir")
        self.__package_path = config.get("tools", "package_path").strip()
        '''
        oss_id = config.get("tools", "oss_id").strip()
        oss_key = config.get("tools", "oss_key").strip()
        oss_host = config.get("tools", "oss_host").strip()
        self.__oss_dbname = config.get("tools", "oss_dbname").strip()
        self.__oss_package_dir = config.get("tools", "oss_prefix").strip()
        self.__auth = oss2.Auth(oss_id, oss_key)
        self.__bucket = oss2.Bucket(self.__auth, oss_host, self.__oss_dbname)
        '''
        self.__graph_mgr = graph_manager.GraphGroup(horae_logger)
        self.__log = horae_logger
        self.__sql_manager = tools_sql_manager.SqlManager(horae_logger)
        self.__admin_ip = None
        self.__admin_port = None
        self.__node_http_port = int(config.get("tools", "node_port"))
        self.__kafka_manager = KafkaRequestManager(config, horae_logger)
        print("__kafka_manager start.")
        self.__log.info("__kafka_manager start.")
        self.__kafka_manager.start()
        print("__kafka_manager started.")
        self.__log.info("__kafka_manager started.")
        if self.__zk_manager.watch_children(
                self.__admin_ip_dir,
                self.__watch_admin_ip_dir) is None:
            raise Exception("watch children error!")

    def __watch_admin_ip_dir(self, children):
        min_index = None
        print("__watch_admin_ip_dir ip addr coming.")
        for child in children:
            field_list = child.split(":")
            if len(field_list) != 3:
                continue

            if min_index is None:
                min_index = field_list[2].strip()
                self.__admin_ip = field_list[0].strip()
                self.__admin_port = int(field_list[1].strip())
                print("__watch_admin_ip_dir ip addr coming." + child + ', ' + self.__admin_ip + ', ' + field_list[1].strip())
                continue

            if field_list[2].strip() < min_index:
                min_index = field_list[2].strip()
                self.__admin_ip = field_list[0].strip()
                self.__admin_port = int(field_list[1].strip())

            print("__watch_admin_ip_dir ip addr coming." + child + ', ' + self.__admin_ip + ', ' + field_list[1].strip())

    def is_lan(self, ip):
        return False
        # try:
        #     res = ipaddress.ip_address(ip.strip()).is_private
        #     print(ip + ":" + str(res))
        #     return res
        # except Exception as e:
        #     print(ip + ": not valid")
        #     return False
        
    def get_project_tree(self, user_id, type=0):
        projects = self.__sql_manager.get_all_projects(type)
        tree_map = {}
        for project in projects:
            if project.parent_id not in tree_map:
                tree_map[project.parent_id] = []

            tree_map[project.parent_id].append({"id": project.id, "text": project.name, "is_project": 1})

        ret_list = []
        for item in tree_map[0]:
            ret_list.append(item)
            item["children"] = []
            self.rec_get_children(item["id"], tree_map, item["children"])

        return ret_list

    def rec_get_children(self, parent, tree_map, ret_children, remove_empty_child_node=False):
        if parent not in tree_map or len(tree_map[parent]) <= 0:
            return False

        for item in tree_map[parent]:
            item["children"] = []
            res = self.rec_get_children(item["id"], tree_map, item["children"], remove_empty_child_node)
            if item["is_project"] and remove_empty_child_node and not res:
                continue

            ret_children.append(item)

        if len(ret_children) <= 0:
            return False

        return True

    def search_pipeline(self, user_id, word, with_project, limit):
        pipelines = self.__sql_manager.search_pipeline(word, limit)
        if with_project != 1:
            ret_list = []
            for pipeline in pipelines:
                ret_list.append({"id": pipeline.id, "text": pipeline.name})

            res_map = {"status": 0, "msg": "OK", "pipes": ret_list}
            return res_map

        projects = self.__sql_manager.get_all_projects()
        tree_map = {}
        for project in projects:
            if project.parent_id not in tree_map:
                tree_map[project.parent_id] = []

            if project.id not in tree_map:
                tree_map[project.id] = []

            tree_map[project.parent_id].append(
                {"id": project.id, "text": project.name, "is_project": 1, "state": "open"})

        for pipeline in pipelines:
            tree_map[pipeline.project_id].append({
                "id": "%s-%s" % (pipeline.project_id, pipeline.id),
                "text": pipeline.name,
                "is_project": 0,
                "iconCls": "icon-file"
            })

        ret_list = []
        for item in tree_map[0]:
            item["children"] = []
            res = self.rec_get_children(item["id"], tree_map, item["children"], True)
            if res:
                ret_list.append(item)

        return ret_list

    def get_project_tree_async(self, user_id, tree_id, type):
        projects = self.__sql_manager.get_projects_with_parent_id(parent_id=tree_id, type=type)
        res_list = []
        for project in projects:
            res_list.append({"id": project.id, "text": project.name, "state": "closed", "is_project": 1})

        if int(tree_id) != 0:
            pipelines = self.__sql_manager.get_pipelines_with_project_id(project_id=tree_id)
            for pipeline in pipelines:
                res_list.append({
                    "id": "%s-%s" % (tree_id, pipeline.id),
                    "text": pipeline.name,
                    "state": "open",
                    "is_project": 0,
                    "iconCls": "icon-file"
                })

        return res_list

    def get_pipeline_info(self, pipeline_id):
        owner_users, pipeline = self.__sql_manager.get_pipeline_info(
            pipeline_id)
        if pipeline is None:
            return self.__get_default_ret_map(1, "db error!")
        pipeline_map = {}
        pipeline_map["id"] = pipeline.id
        pipeline_map["name"] = pipeline.name
        pipeline_map["owner_id"] = pipeline.owner_id
        pipeline_map["ct_time"] = pipeline.ct_time
        pipeline_map["update_time"] = pipeline.update_time.strftime(
            "%Y-%m-%d %H:%M:%S")
        pipeline_map["enable"] = pipeline.enable
        pipeline_map["type"] = pipeline.type
        pipeline_map["email_to"] = pipeline.email_to
        pipeline_map["description"] = pipeline.description
        pipeline_map["sms_to"] = pipeline.sms_to
        pipeline_map["tag"] = pipeline.tag
        pipeline_map["life_cycle"] = pipeline.life_cycle
        pipeline_map["monitor_way"] = pipeline.monitor_way
        pipeline_map["private"] = pipeline.private
        pipeline_map["project_id"] = pipeline.project_id
        try:
            project = horae.models.Project.objects.get(id=pipeline.project_id)
            pipeline_map["project_name"] = project.name
        except:
            pipeline_map["project_name"] = ''

        owner_list = []
        for owner in owner_users:
            user_map = {}
            user_map["id"] = owner.id
            if owner.first_name is not None and owner.first_name.strip() != "":
                user_map["username"] = owner.first_name
            else:
                user_map["username"] = owner.username

            owner_list.append(user_map)
        pipeline_map["owner_list"] = owner_list
        ret_map = {}
        ret_map["status"] = 0
        ret_map["info"] = "OK"
        ret_map["pipeline"] = pipeline_map
        return json.dumps(ret_map)

    def __get_default_ret_map(self, status, info):
        ret_map = {}
        ret_map["status"] = status
        ret_map["info"] = info
        return json.dumps(ret_map)

    def get_tasks_by_pipeline_id(self, pipeline_id):
        tasks = self.__sql_manager.get_tasks_by_pipeline_id(pipeline_id)
        if tasks is None:
            return self.__get_default_ret_map(
                1,
                "visit mysql failed! please check db!")
        task_dict = {}
        for task in tasks:
            task.next_task_ids = ''
            task.prev_task_ids = ''
            task_dict[task.id] = task

        edges = horae.models.Edge.objects.filter(pipeline_id=pipeline_id)
        for edge in edges:
            if edge.prev_task_id in task_dict:
                task_dict[edge.prev_task_id].next_task_ids += str(edge.next_task_id) + ","
                 
        task_list = []
        task_id_list = set()
        for task in tasks:
            task_id_list.add(str(task.id))

        latest_history = horae.models.RunHistory.objects.filter(pl_id=pipeline_id).order_by("-start_time")[:1]
        task_with_status = {}
        if latest_history is not None and len(latest_history) > 0:
            historys = horae.models.RunHistory.objects.filter(pl_id=pipeline_id, run_time=latest_history[0].run_time)
            for history in historys:
                task_with_status[history.task_id] = history.status


        for task in tasks:
            task_map = self.__get_task_map(task)
            self.__get_task_no_other_next(task_map, task_id_list)
            if task.id in task_with_status:
                task_map["status"] = task_with_status[task.id]
            else:
                task_map["status"] = -1

            task_list.append(task_map)

        ret_map = {}
        ret_map["status"] = 0
        ret_map["info"] = "OK"
        ret_map["tasks"] = task_list
        return json.dumps(ret_map)

    # def get_tasks_by_pipeline_id(self, pipeline_id):
    #     tasks = self.__sql_manager.get_tasks_by_pipeline_id(pipeline_id)
    #     if tasks is None:
    #         return self.__get_default_ret_map(
    #             1,
    #             "visit mysql failed! please check db!")

    #     task_list = []
    #     task_id_list = set()
    #     for task in tasks:
    #         task_id_list.add(str(task.id))

    #     for task in tasks:
    #         task_map = self.__get_task_map(task)
    #         self.__get_task_no_other_next(task_map, task_id_list)
    #         task_list.append(task_map)

    #     ret_map = {}
    #     ret_map["status"] = 0
    #     ret_map["info"] = "OK"
    #     ret_map["tasks"] = task_list
    #     return json.dumps(ret_map)

    def __get_task_map(self, task):
        task_map = {}
        task_map["id"] = task.id
        task_map["pl_id"] = task.pl_id
        task_map["pid"] = task.pid
        task_map["next_task_ids"] = task.next_task_ids
        task_map["prev_task_ids"] = task.prev_task_ids
        task_map["over_time"] = task.over_time
        task_map["name"] = task.name
        task_map["config"] = task.config
        task_map["retry_count"] = task.retry_count
        task_map["last_run_time"] = task.last_run_time
        task_map["description"] = task.description
        task_map["priority"] = task.priority
        task_map["server_tag"] = task.server_tag
        task_map["except_ret"] = task.except_ret
        task_map["version_id"] = task.version_id
        owner, pipeline = self.__sql_manager.get_pipeline_info(task.pl_id)
        if pipeline is not None:
            task_map["pipeline_name"] = pipeline.name
        else:
            task_map["pipeline_name"] = ''

        processor = self.__sql_manager.get_proessor_info(task.pid)
        if processor is not None:
            task_map["proc_name"] = processor.name
        else:
            task_map["proc_name"] = ''

        return task_map

    def __get_task_no_other_next(self, task_map, task_id_list):
        tmp_next_id_list = []
        next_task_ids = task_map["next_task_ids"].split(',')
        for next_task_id in next_task_ids:
            if next_task_id.strip() == '':
                continue

            if next_task_id.strip() in task_id_list:
                tmp_next_id_list.append(next_task_id.strip())
        next_id_str = ''
        if len(tmp_next_id_list) > 0:
            next_id_str = ','.join(tmp_next_id_list)

        task_map['next_task_ids'] = next_id_str

    def delete_edge(self, owner_id, from_task_id, to_task_id):
        status, info = self.__sql_manager.delete_edge_with_transaction(
            owner_id,
            from_task_id,
            to_task_id)
        return self.__get_default_ret_map(status, info)

    def add_edge(self, owner_id, from_task_id, to_task_id, edge):
        status, info = self.__sql_manager.add_edge_with_transaction(
            owner_id,
            from_task_id,
            to_task_id,
            edge)
        return self.__get_default_ret_map(status, info)

    def update_pipeline(
            self,
            pipeline_id,
            owner_id,
            lifecycle=None,
            name=None,
            ct_time=None,
            manager_id_list=None,
            monitor_way=None,
            tag=None,
            description=None,
            type=None,
            project_id=None):
        if ct_time is not None:
            ct_time = self.__change_ct_time(ct_time)
            if not self.__check_ct_time_valid(ct_time):
                return self.__get_default_ret_map(1, "流程调度时间格式错误!")

        status, info = self.__sql_manager.update_pipeline(
            pipeline_id,
            owner_id,
            lifecycle,
            name,
            ct_time,
            manager_id_list,
            monitor_way,
            tag,
            description,
            type,
            project_id)
        return self.__get_default_ret_map(status, info)

    def __change_ct_time(self, ct_time):
        tmp_ct_time = tools_util.StaticFunction.strip_with_one_space(ct_time)
        tmp_ct_time_list = tmp_ct_time.split(' ')
        if len(tmp_ct_time_list) != 5:
            raise Exception("调度时间格式不对[%s]" % ct_time)

        tmp_list = []
        if tmp_ct_time_list[0].find('/') != -1:
            tmp_split = tmp_ct_time_list[0].split('/')
            min_list = []
            begin_min = 0
            while begin_min < 60:
                min_list.append(str(begin_min))
                begin_min += int(tmp_split[1])

            tmp_list.append(','.join(min_list))
        else:
            tmp_list.append(tmp_ct_time_list[0])

        if tmp_ct_time_list[1].find('/') != -1:
            tmp_split = tmp_ct_time_list[1].split('/')
            min_list = []
            begin_min = 0
            while begin_min < 24:
                min_list.append(str(begin_min))
                begin_min += int(tmp_split[1])

            tmp_list.append(','.join(min_list))
        else:
            tmp_list.append(tmp_ct_time_list[1])

        for i in range(2, 5):
            if tmp_ct_time_list[i].find('/') != -1:
                raise Exception("调度时间格式不对[%s]" % ct_time)

            tmp_list.append(tmp_ct_time_list[i])

        return ' '.join(tmp_list)

    def __check_ct_time_valid(self, ct_time):
        if ct_time.find('-') != -1:
            return False

        crontab_job = crontab.CronTab(tab='').new(command='/usr/bin/echo')
        crontab_job.setall(ct_time.strip())
        return crontab_job.is_valid()

    def copy_pipeline(self, owner_id, src_pl_id, new_pl_name, project_id, use_type_src):
        if new_pl_name.strip() == '':
            return self.__get_default_ret_map(1, "流程名不能为空！")
        status, pl_id = self.__sql_manager.copy_pipeline(
            owner_id,
            src_pl_id,
            new_pl_name,
            project_id,
            use_type_src)
        if status != 0:
            return self.__get_default_ret_map(1, pl_id)
        ret_map = {}
        ret_map["status"] = 0
        ret_map["info"] = "OK"
        ret_map["pl_id"] = pl_id
        return json.dumps(ret_map)

    def delete_pipeline(self, owner_id, pipeline_id):
        status, info = self.__sql_manager.delete_pipeline(
            owner_id,
            pipeline_id)
        return self.__get_default_ret_map(status, info)

    def update_tasks(self, owner_id, task, old_task=None, template=None):
        status, task = self.__sql_manager.update_tasks(
            owner_id,
            task,
            old_task,
            template)
        if status != 0:
            return self.__get_default_ret_map(status, str(task))
        task_map = self.__get_task_map(task)
        ret_map = {}
        ret_map["status"] = 0
        ret_map["info"] = "OK"
        ret_map["task"] = task_map
        return json.dumps(ret_map)

        return self.__get_default_ret_map(status, info)

    def get_task_info(self, task_id):
        task = self.__sql_manager.get_task_info(task_id)
        if task is None:
            return self.__get_default_ret_map(
                1,
                "visit mysql failed! please check db!")
        task_map = self.__get_task_map(task)
        ret_map = {}
        ret_map["status"] = 0
        ret_map["info"] = "OK"
        ret_map["task"] = task_map
        return json.dumps(ret_map)

    def get_all_authed_pipeline_info(self, owner_id, task_id=None):
        pipelines = self.__sql_manager.get_all_authed_pipeline_info(owner_id, task_id)
        pipe_list = []
        for pipeline in pipelines:
            if pipeline["type"] != 0:
                continue

            pipe_map = {}
            pipe_map["id"] = pipeline["id"]
            pipe_map["name"] = pipeline["name"]
            pipe_list.append(pipe_map)

        ret_map = {}
        ret_map["status"] = 0
        ret_map["info"] = "OK"
        ret_map["pipelines"] = pipe_list
        return json.dumps(ret_map)

    def get_all_authed_processor(self, owner_id, type):
        processors = self.__sql_manager.get_all_authed_processor(
            owner_id,
            type)
        if processors is None:
            return self.__get_default_ret_map(1, "db error!")

        proc_list = []
        for proc in processors:
            tmp_map = {}
            tmp_map["id"] = proc.id
            tmp_map["name"] = proc.name
            proc_list.append(tmp_map)

        ret_map = {}
        ret_map["status"] = 0
        ret_map["info"] = "OK"
        ret_map["processors"] = proc_list
        return json.dumps(ret_map)

    def add_new_task_to_pipeline(self, owner_id, task, processor):
        status, task = self.__sql_manager.add_new_task_to_pipeline(
            owner_id,
            task,
            processor)
        if status != 0:
            return self.__get_default_ret_map(
                status,
                "创建任务失败！reason[%s]" % task)
        task_map = self.__get_task_map(task)
        ret_map = {}
        ret_map["status"] = 0
        ret_map["info"] = "OK"
        ret_map["task"] = task_map
        return json.dumps(ret_map)

    def create_new_pipeline(
            self,
            name,
            ct_time,
            owner_id,
            manager_id_list,
            monitor_way,
            tag,
            description,
            life_cycle,
            type,
            project_id):
        if ct_time is None \
                or name is None \
                or owner_id is None \
                or monitor_way is None:
            return self.__get_default_ret_map(
                1,
                "param ct_time, name, owner_id, monitor_way None!")

        if life_cycle is None:
            life_cycle = 365

        if ct_time.strip() != "":
            ct_time = self.__change_ct_time(ct_time)
            if not self.__check_ct_time_valid(ct_time):
                return self.__get_default_ret_map(1, "流程调度时间格式错误!")

        status, info = self.__sql_manager.create_new_pipeline(
            name,
            ct_time,
            owner_id,
            manager_id_list,
            monitor_way,
            tag,
            description,
            life_cycle,
            type,
            project_id)
        return self.__get_default_ret_map(status, info)

    def get_all_user_info(self):
        users = self.__sql_manager.get_all_user_info()
        if users is None:
            return self.__get_default_ret_map(1, "db error!")

        user_list = []
        for user in users:
            user_map = {}
            user_map["id"] = user.id
            if user.first_name is None or user.first_name.strip() == "":
                user_map["username"] = user.username
            else:
                user_map["username"] = user.first_name

            user_list.append(user_map)

        ret_map = {}
        ret_map["status"] = 0
        ret_map["info"] = "OK"
        ret_map["users"] = user_list
        return json.dumps(ret_map)

    def get_pipeline_with_project_tree(self, user_id, id):
        pipeline = horae.models.Pipeline.objects.get(id=id)
        parent_id = pipeline.project_id
        res_list = []
        res_list.append(
            {"id": "%s-%s" % (parent_id, pipeline.id),
             "text": pipeline.name,
             "is_project": 0,
             "state": "open",
             "iconCls": "icon-file"
             })
        while parent_id > 0:
            project = horae.models.Project.objects.get(id=parent_id)
            tmp_list = []
            tmp_list.append({"id": "%s" % project.id, "text": project.name, "is_project": 1, "children": res_list})
            res_list = tmp_list
            parent_id = project.parent_id

        res = {"status": 0, "msg": "OK", "node_id": "%s-%s" % (pipeline.project_id, pipeline.id), "data": res_list}
        return res

    def __get_datetime(self, run_time_str, hour=0, minute=0, second=0):
        run_time_len = len(run_time_str)
        if run_time_len < tools_util.RunTimeLength.DAY_RUN_TIME_LEN:
            raise Exception("run time error![%s]" % run_time_str)

        year = int(run_time_str[0: 4])
        month = int(run_time_str[4: 6])
        day = int(run_time_str[6: 8])
        if run_time_len >= tools_util.RunTimeLength.HOUR_RUN_TIME_LEN:
            hour = int(run_time_str[8: 10])

        if run_time_len >= tools_util.RunTimeLength.MINIUTE_RUN_TIME_LEN:
            minute = int(run_time_str[10: 12])
        return datetime.datetime(
            year,
            month,
            day,
            hour,
            minute,
            second,
            0)

    def __handle_no_ct_time(self, run_time_str):
        pos = run_time_str.find('-')
        if pos <= 0:
            return None

        run_time_list = []
        begin_run_time = run_time_str[0: pos].strip()
        end_run_time = run_time_str[pos + 1:].strip()
        begin_time_len = len(begin_run_time)
        end_time_len = len(end_run_time)
        if begin_time_len != end_time_len:
            return None

        begin_datetime = self.__get_datetime(begin_run_time)
        end_datetime = self.__get_datetime(end_run_time, 23, 59, 59)
        if begin_datetime > end_datetime:
            return None

        if begin_time_len == tools_util.RunTimeLength.DAY_RUN_TIME_LEN:
            time_delta = datetime.timedelta(days=1)
            run_time_num = 0
            while True:
                if begin_datetime >= end_datetime:
                    break
                run_time_list.append(begin_datetime.strftime("%Y%m%d0000"))

                if run_time_num >= tools_util.CONSTANTS.MAX_RESTART_TASK_NUM:
                    return None
                begin_datetime += time_delta
                run_time_num += 1
        elif begin_time_len == tools_util.RunTimeLength.HOUR_RUN_TIME_LEN:
            time_delta = datetime.timedelta(hours=1)
            run_time_num = 0
            while True:
                if begin_datetime >= end_datetime:
                    break
                run_time_list.append(begin_datetime.strftime("%Y%m%d%H00"))

                if run_time_num >= tools_util.CONSTANTS.MAX_RESTART_TASK_NUM:
                    return None
                begin_datetime += time_delta
                run_time_num += 1
        elif begin_time_len == tools_util.RunTimeLength.MINIUTE_RUN_TIME_LEN:
            time_delta = datetime.timedelta(minutes=1)
            run_time_num = 0
            while True:
                if begin_datetime >= end_datetime:
                    break
                run_time_list.append(begin_datetime.strftime("%Y%m%d%H%M"))

                if run_time_num >= tools_util.CONSTANTS.MAX_RESTART_TASK_NUM:
                    return None
                begin_datetime += time_delta
                run_time_num += 1
        else:
            return None
        return run_time_list

    def __handle_with_ct_time(self, ct_time, run_time_str):
        pos = run_time_str.find('-')
        if pos <= 0:
            return None

        run_time_list = []
        begin_run_time = run_time_str[0: pos].strip()
        end_run_time = run_time_str[pos + 1:].strip()
        begin_time_len = len(begin_run_time)
        end_time_len = len(end_run_time)
        if begin_time_len != end_time_len:
            return None

        begin_datetime = self.__get_datetime(begin_run_time)
        end_datetime = self.__get_datetime(end_run_time, 23, 59, 59)
        if begin_datetime > end_datetime:
            return None

        crontab_job = crontab.CronTab(tab='').new(command='/usr/bin/echo')
        crontab_job.setall(ct_time.strip())
        if not crontab_job.is_valid():
            self.__log.error("job set cron_express[%s] failed!" % ct_time)
            return None
        
        ct_iter = crontab_job.schedule(begin_datetime)
        run_time_list = []
        run_time_list.append(begin_datetime.strftime("%Y%m%d%H%M"))
        while True:
            now_time = ct_iter.get_next()
            if now_time > end_datetime:
                break

            run_time_list.append(now_time.strftime("%Y%m%d%H%M"))

        return run_time_list

    def __handle_section_runtime(self, pipeline_id, run_time_str):
        ct_time = ''
        try:
            pipeline = horae.models.Pipeline.objects.get(
                id=pipeline_id)
            ct_time = pipeline.ct_time
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return None

        if ct_time is None or ct_time.strip() == '':
            return self.__handle_no_ct_time(run_time_str)

        return self.__handle_with_ct_time(ct_time, run_time_str)

    def __hanlde_list_runtime(self, pipeline_id, run_time_str):
        ret_run_time_list = []
        run_time_list = run_time_str.split(',')
        ct_time = ''
        try:
            pipeline = horae.models.Pipeline.objects.get(
                id=pipeline_id)
            ct_time = pipeline.ct_time
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return None

        for run_time in run_time_list:
            run_time = run_time.strip()
            if run_time == '':
                continue

            if len(run_time) == tools_util.CONSTANTS.RUN_TIME_LENTGH:
                ret_run_time_list.append(run_time)
                continue

            if len(run_time) != tools_util.RunTimeLength.DAY_RUN_TIME_LEN and \
                    len(run_time) != tools_util.RunTimeLength.HOUR_RUN_TIME_LEN and \
                    len(run_time) != tools_util.RunTimeLength.MINIUTE_RUN_TIME_LEN:
                self.__log.error(
                    "run time string error[%s] failed!" % run_time)
                return None

            run_datetime = self.__get_datetime(run_time.strip())
            crontab_job = crontab.CronTab(tab='').new(command='/usr/bin/echo')
            crontab_job.setall(ct_time.strip())
            if not crontab_job.is_valid():
                self.__log.error(
                    "job set cron_express[%s] failed!" % ct_time)
                return None
            ct_iter = crontab_job.schedule(run_datetime)
            now_time = ct_iter.get_prev()
            now_time_format = now_time.strftime("%Y%m%d%H%M")
            run_formate_time = run_datetime.strftime("%Y%m%d%H%M")
            if now_time_format >= run_formate_time:
                ret_run_time_list.append(now_time.strftime("%Y%m%d%H%M"))
                continue

            now_time = ct_iter.get_next()
            now_time_format = now_time.strftime("%Y%m%d%H%M")
            if now_time_format >= run_formate_time:
                ret_run_time_list.append(now_time.strftime("%Y%m%d%H%M"))
                continue

            now_time = ct_iter.get_next()
            ret_run_time_list.append(now_time.strftime("%Y%m%d%H%M"))

        return ret_run_time_list

    def __get_run_time_list(self, pipeline_id, run_time_str):
        try:
            if run_time_str.find('-') != -1:
                return self.__handle_section_runtime(pipeline_id, run_time_str)

            return self.__hanlde_list_runtime(pipeline_id, run_time_str)
        except Exception as ex:
            self.__log.error("execute sql failed![ex:%s][trace:%s]!" % (
                str(ex), traceback.format_exc()))
            return None

    def run_tasks(self, owner_id, task_id_list, run_time_str, ordered=0):
        if self.__admin_ip is None or self.__admin_port is None:
            return self.__get_default_ret_map(1, "no admin server running ttttt!")

        req_json_list = []
        restart_task_num = 0
        for task_id in task_id_list:
            task = self.__sql_manager.get_task_info(int(task_id))
            if task is None:
                return self.__get_default_ret_map(
                    1,
                    "succsessors task is not exists!")

            run_time_list = self.__get_run_time_list(task.pl_id, run_time_str)
            if run_time_list is None:
                return self.__get_default_ret_map(
                    1,
                    "<h>执行时间[%s]错误: </h><br>"
                    "<h>1. 长度必须是8，10，12.</h><br>"
                    "<h>2. 不能超过当前时间!.</h><br>"
                    "<h>3. 执行任务数不能超过[%s]</h><br>" %
                    (run_time_str, tools_util.CONSTANTS.MAX_RESTART_TASK_NUM))

            if len(run_time_list) <= 0:
                continue

            if self.__sql_manager.check_pipeline_auth_valid(
                    task.pl_id,
                    owner_id) != tools_util.UserPermissionType.WRITE:
                return self.__get_default_ret_map(
                    1,
                    "对不起，你没有权限启动这个流程的任务！")

            for run_time in run_time_list:
                task_map = {}
                task_map["task_id"] = str(task_id)
                task_map["run_time"] = run_time
                req_json_list.append(task_map)
                restart_task_num += 1
                if restart_task_num >= \
                        tools_util.CONSTANTS.MAX_RESTART_TASK_NUM:
                    return self.__get_default_ret_map(
                        1,
                        "执行任务数超过了每次提交限制数[%d]" %
                        tools_util.CONSTANTS.MAX_RESTART_TASK_NUM)

        status, now_running_task_num = \
            self.__sql_manager.get_now_owner_scheduled_task_num(owner_id)
        if now_running_task_num + restart_task_num \
                >= tools_util.CONSTANTS.MAX_RUNNING_TASK_NUM:
            return self.__get_default_ret_map(
                1,
                "执行任务数超过了每次提交限制数[%d]" %
                tools_util.CONSTANTS.MAX_RUNNING_TASK_NUM)

        req_map = {}
        req_map["task_pair_list"] = req_json_list
        tmp_req_json = json.dumps(req_map)
        # 编码， 用于发送请求
        req_json = urllib.request.quote(tmp_req_json.encode('gbk'))
        node_req_url = ("http://%s:%s/restart_task?"
                        "task_pair_json=%s&ordered=%s" % (
                            self.__admin_ip,
                            self.__admin_port,
                            req_json,
                            ordered))
        self.__log.error(node_req_url)
        url_stream = "FAIL"
        try:
            url_stream = urllib.request.urlopen(node_req_url).read().decode('utf-8')
        except Exception as ex:
            url_stream = str(ex)

        if url_stream != "OK":
            return self.__get_default_ret_map(
                1,
                "执行任务失败，详情[%s] admin: %s" % (
                    url_stream, node_req_url))
        return self.__get_default_ret_map(0, "OK,%s" % tmp_req_json)

    def add_new_project(self, owner_id, project_name, writer_list, description, parent_id, type=0):
        if project_name.strip() == "":
            return 1, "项目名不能为空"

        length = len(project_name)
        utf8_length = len(project_name.encode('utf-8'))
        length = (utf8_length - length) / 2 + length

        if length > tools_util.CONSTANTS.PROJECT_MAX_LENGTH:
            return self.__get_default_ret_map(
                1,
                "项目名称英文不能超过%s个字符，中文不能超过%s个字符" %
                (tools_util.CONSTANTS.PROJECT_MAX_LENGTH,
                 tools_util.CONSTANTS.PROJECT_MAX_LENGTH / 2))

        status, info, id = self.__sql_manager.add_new_project(
            owner_id,
            project_name,
            writer_list,
            description,
            parent_id,
            type)

        ret_map = {}
        ret_map["status"] = status
        ret_map["info"] = info
        ret_map["id"] = id
        return json.dumps(ret_map)

    def delete_task_info(self, owner_id, task_id):
        status, info = self.__sql_manager.delete_task_info(
            owner_id,
            task_id)
        return self.__get_default_ret_map(status, info)

    def delete_project(self, owner_id, project_id):
        status, info = self.__sql_manager.delete_project(owner_id, project_id)
        return self.__get_default_ret_map(status, info)

    def get_task_run_logs(self, schedule_id, sub_path, rerun_id):
        '''
        if schedule_id == 0:
            return self.__get_default_ret_map(1, "task has not scheduled!")

        if self.__admin_ip is None or self.__admin_port is None:
            return self.__get_default_ret_map(1, "no admin server running!")
        url_stream = "FAIL"
        try:
            if sub_path is None:
                sub_path = ''
            node_req_url = (
                    "http://%s:%s/list_work_dir?"
                    "schedule_id=%s&path=%s&rerun_id=%s" % (
                        self.__admin_ip,
                        self.__admin_port,
                        schedule_id,
                        sub_path,
                        rerun_id))
            url_stream = urllib.request.urlopen(node_req_url).read().decode('utf-8')
        except Exception as ex:
            return self.__get_default_ret_map(1, str(ex))
        '''
        if rerun_id > 0:
            run_history = self.__sql_manager.get_rerunhsitory_with_id(rerun_id)
        else:
            run_history = self.__sql_manager.get_runhsitory_with_id(
                int(schedule_id))
        if run_history is None:
            return self.__get_default_ret_map(1, 'error: has no ready_task info with'
                       ' schedule_id:%s' % schedule_id)
        if self.is_lan(run_history.run_server) and self.__kafka_servers != "":
            msg_id = int(round(time.time() * 100000))
            data = {}
            data["msg_id"] = msg_id
            data["cmd"] = "list_work_dir"
            data["path"] = sub_path
            data["schedule_id"] = str(schedule_id)
            data["rerun_id"] = str(rerun_id)
            data["response"] = False
            producer = KafkaProducer(bootstrap_servers=self.__kafka_servers, api_version=(0, 10, 1), value_serializer=lambda m: json.dumps(m).encode())
            producer.send(run_history.run_server, data)
            print("send data: ")
            print(data)
            try_times = 0
            res_data = None
            while try_times < 20:
                res_data = self.__kafka_manager.get_data(msg_id)
                if res_data is not None:
                    self.__log.info("success get res data: %s" % res_data)
                    break
                    
                self.__log.info("waiting get res data: %s" % "0")
                time.sleep(0.1)
                try_times += 1
            if res_data is None: 
                ret_map = {}
                ret_map["status"] = 1
                ret_map["info"] = "ERROR"
                ret_map["log_file_list"] = ""
            else:
                ret_map = {}
                ret_map["status"] = 0
                ret_map["info"] = "ok"
                ret_map["log_file_list"] = res_data

            return json.dumps(ret_map)
        else:
            node_req_url = (
                "http://%s:%s/list_work_dir?"
                "schedule_id=%s&path=%s&rerun_id=%s" % (
                run_history.run_server,
                self.__node_http_port,
                schedule_id,
                sub_path,
                rerun_id))
            url_stream = urllib.request.urlopen(node_req_url).read().decode('utf-8')
            if url_stream.startswith("error") \
                    and not url_stream.startswith("error.log"):
                return self.__get_default_ret_map(1, url_stream)

            ret_map = {}
            ret_map["status"] = 0
            ret_map["info"] = "OK"
            ret_map["log_file_list"] = url_stream
            return json.dumps(ret_map)

    def get_task_log_content(self, schedule_id, file_name, file_offset, str_len, rerun_id):
        '''
        if schedule_id == 0:
            return self.__get_default_ret_map(1, "task has not scheduled!")

        if self.__admin_ip is None or self.__admin_port is None:
            return self.__get_default_ret_map(1, "no admin server running!")
        url_stream = "FAIL"

        file_name = file_name.strip()
        if file_name.startswith('/') or file_name.find('../') >= 0:
            return self.__get_default_ret_map(
                1,
                "filename startswith error![%s]" % file_name)
        try:
            node_req_url = ("http://%s:%s/get_file_content?"
                            "schedule_id=%s&file=%s&start=%s&len=%s&rerun_id=%s" % (
                                self.__admin_ip, self.__admin_port, schedule_id,
                                file_name, file_offset, str_len, rerun_id))
            url_stream = urllib.request.urlopen(node_req_url).read().decode('utf-8')
        except Exception as ex:
            return self.__get_default_ret_map(1, str(ex))
        '''
        if rerun_id > 0:
            run_history = self.__sql_manager.get_rerunhsitory_with_id(rerun_id)
        else:
            run_history = self.__sql_manager.get_runhsitory_with_id(
                int(schedule_id))
        if run_history is None:
            return self.__get_default_ret_map(
                1,
                'error: has no ready_task info with schedule_id:%s' % schedule_id)
        if self.is_lan(run_history.run_server) and self.__kafka_servers != "":
            msg_id = int(round(time.time() * 100000))
            data = {}
            data["msg_id"] = msg_id
            data["cmd"] = "get_file_content"
            data["file"] = file_name
            data["schedule_id"] = str(schedule_id)
            data["rerun_id"] = str(rerun_id)
            data["start"] = str(file_offset)
            data["len"] = str(str_len)
            data["response"] = False
            producer = KafkaProducer(bootstrap_servers=self.__kafka_servers, api_version=(0, 10, 1), value_serializer=lambda m: json.dumps(m).encode())
            producer.send(run_history.run_server, data)
            print("send data: ")
            print(data)
            try_times = 0
            res_data = None
            while try_times < 20:
                res_data = self.__kafka_manager.get_data(msg_id)
                if res_data is not None:
                    self.__log.info("success get res data: %s" % res_data)
                    break
                    
                self.__log.info("waiting get res data: %s" % "0")
                time.sleep(0.1)
                try_times += 1
            return res_data
        else:
            node_req_url = ("http://%s:%s/get_file_content?schedule_id=%s"
                        "&file=%s&start=%d&len=%d&rerun_id=%s" % (
                        run_history.run_server,
                        self.__node_http_port,
                        schedule_id,
                        file_name,
                        file_offset,
                        str_len,
                        rerun_id))
            url_stream = urllib.request.urlopen(node_req_url).read().decode('utf-8')
            if url_stream.startswith("error"):
                return self.__get_default_ret_map(1, url_stream)
            return url_stream

    def get_log_content_tail(self, schedule_id, file_name, rerun_id):
        '''
        if schedule_id == 0:
            return self.__get_default_ret_map(1, "task has not scheduled!")

        if self.__admin_ip is None or self.__admin_port is None:
            return self.__get_default_ret_map(1, "no admin server running!")
        url_stream = "FAIL"
        try:
            node_req_url = ("http://%s:%s/get_file_tail?"
                            "schedule_id=%s&file=%s&rerun_id=%s" % (
                                self.__admin_ip,
                                self.__admin_port,
                                schedule_id,
                                file_name,
                                rerun_id))
            url_stream = urllib.request.urlopen(node_req_url).read().decode('utf-8')
        except Exception as ex:
            return self.__get_default_ret_map(1, str(ex))
        '''
        if rerun_id > 0:
            run_history = self.__sql_manager.get_rerunhsitory_with_id(rerun_id)
        else:
            run_history = self.__sql_manager.get_runhsitory_with_id(
                int(schedule_id))

        if run_history is None:
            return self.__get_default_ret_map(1, "获取执行状态信息失败!")
        if self.is_lan(run_history.run_server) and self.__kafka_servers != "":
            msg_id = int(round(time.time() * 100000))
            data = {}
            data["msg_id"] = msg_id
            data["cmd"] = "get_file_tail"
            data["file"] = file_name
            data["schedule_id"] = str(schedule_id)
            data["rerun_id"] = str(rerun_id)
            data["response"] = False
            producer = KafkaProducer(bootstrap_servers=self.__kafka_servers, api_version=(0, 10, 1), value_serializer=lambda m: json.dumps(m).encode())
            producer.send(run_history.run_server, data)
            try_times = 0
            res_data = None
            while try_times < 20:
                res_data = self.__kafka_manager.get_data(msg_id)
                if res_data is not None:
                    self.__log.info("success get res data: %s" % res_data)
                    break
                    
                self.__log.info("waiting get res data: %s" % "0")
                time.sleep(0.1)
                try_times += 1
            ret_map = {}
            ret_map["status"] = 0
            ret_map["info"] = "OK"
            ret_map["file_content"] = res_data
            try:
                return json.dumps(ret_map)
            except Exception as ex:
                self.__log.error("read file tail failed.ex[%s], trace[%s]" % (
                    str(ex), traceback.format_exc()))
                return self.__get_default_ret_map(1, "can't read such file")
        else:
            node_req_url = ("http://%s:%s/get_file_tail?schedule_id=%s"
                        "&file=%s&rerun_id=%s" % (
                        run_history.run_server,
                        self.__node_http_port,
                        schedule_id,
                        file_name,
                        rerun_id))
            url_stream = urllib.request.urlopen(node_req_url).read().decode('utf-8')
            if url_stream.startswith("error"):
                return self.__get_default_ret_map(1, url_stream)

            ret_map = {}
            ret_map["status"] = 0
            ret_map["info"] = "OK"
            ret_map["file_content"] = url_stream
            try:
                return json.dumps(ret_map)
            except Exception as ex:
                self.__log.error("read file tail failed.ex[%s], trace[%s]" % (
                    str(ex), traceback.format_exc()))
                return self.__get_default_ret_map(1, "can't read such file")

    def run_pipeline(self, owner_id, pipeline_id, run_time_str, ordered=0):
        if self.__admin_ip is None or self.__admin_port is None:
            return self.__get_default_ret_map(1, "no admin server running ttt1!")

        if self.__sql_manager.check_pipeline_auth_valid(
                pipeline_id,
                owner_id) != tools_util.UserPermissionType.WRITE:
            return self.__get_default_ret_map(
                1,
                "this owner has no auth to run pipeline")
        run_time_list = self.__get_run_time_list(pipeline_id, run_time_str)
        if run_time_list is None or len(run_time_list) <= 0:
            return self.__get_default_ret_map(
                1,
                "<h>执行时间[%s]错误: </h><br>"
                "<h>1. 长度必须是8，10，12.</h><br>"
                "<h>2. 不能超过当前时间!.</h><br>"
                "<h>3. 执行任务数不能超过[%s]</h><br>" %
                (run_time_str, tools_util.CONSTANTS.MAX_RESTART_TASK_NUM))

        tasks = self.__sql_manager.get_tasks_by_pipeline_id(pipeline_id)
        req_json_list = []
        restart_task_num = 0
        for task in tasks:
            for run_time in run_time_list:
                task_map = {}
                task_map["task_id"] = str(task.id)
                task_map["run_time"] = run_time
                req_json_list.append(task_map)
                restart_task_num += 1
                if restart_task_num >= \
                        tools_util.CONSTANTS.MAX_RESTART_TASK_NUM:
                    return self.__get_default_ret_map(
                        1,
                        "restart task extend max num[%d]" %
                        tools_util.CONSTANTS.MAX_RESTART_TASK_NUM)

        status, now_running_task_num = \
            self.__sql_manager.get_now_owner_scheduled_task_num(owner_id)
        if now_running_task_num + restart_task_num \
                >= tools_util.CONSTANTS.MAX_RUNNING_TASK_NUM:
            return self.__get_default_ret_map(
                1,
                "running task extend max num[%d]" %
                tools_util.CONSTANTS.MAX_RUNNING_TASK_NUM)
        req_map = {}
        req_map["task_pair_list"] = req_json_list
        tmp_req_json = json.dumps(req_map)
        # 编码， 用于发送请求
        req_json = urllib.request.quote(tmp_req_json.encode('gbk'))
        node_req_url = ("http://%s:%s/restart_task?"
                        "task_pair_json=%s&ordered=%s" % (
                            self.__admin_ip,
                            self.__admin_port,
                            req_json,
                            ordered))
        url_stream = "FAIL"
        try:
            url_stream = urllib.request.urlopen(node_req_url).read().decode('utf-8')
        except Exception as ex:
            url_stream = str(ex)

        if url_stream != "OK":
            return self.__get_default_ret_map(
                1,
                "restart task failed! %s" % (
                    url_stream))
        return self.__get_default_ret_map(0, "OK,%s" % tmp_req_json)

    def stop_task(self, owner_id, task_id, run_time):
        task = self.__sql_manager.get_task_info(task_id)
        if task is None:
            return self.__get_default_ret_map(1, "task is not exists!")

        if self.__sql_manager.check_pipeline_auth_valid(
                task.pl_id,
                owner_id) != tools_util.UserPermissionType.WRITE:
            return self.__get_default_ret_map(
                1,
                "对不起，你没有权限停止这个流程的任务！")
        schedule = None
        try:
            schedule = horae.models.Schedule.objects.get(
                task_id=task_id,
                run_time=run_time)
        except Exception as ex:
            return self.__get_default_ret_map(
                1,
                "get schedule info failed!")

        unique_id = uuid.uuid1()
        node_req_url = ("http://%s:%s/stop_task?"
                        "unique_id=%s&schedule_id=%s" % (
                            self.__admin_ip,
                            self.__admin_port,
                            unique_id,
                            schedule.id))
        while not tools_util.CONSTANTS.GLOBAL_STOP:
            url_stream = "FAIL"
            try:
                url_stream = urllib.request.urlopen(node_req_url).read().decode('utf-8')
            except Exception as ex:
                url_stream = str(ex)

            if url_stream == tools_util.CONSTANTS.HTTP_RESPONSE_WAIT:
                time.sleep(1)
                continue

            if url_stream != "OK":
                return self.__get_default_ret_map(
                    1,
                    "停止任务失败! %s, %s" % (
                        url_stream, node_req_url))
            break

        return self.__get_default_ret_map(0, "OK")

    def run_task_with_all_successors(self, owner_id, task_id, run_time, ordered=0):
        if self.__admin_ip is None or self.__admin_port is None:
            return self.__get_default_ret_map(1, "no admin server running ttt2!")

        task = self.__sql_manager.get_task_info(task_id)
        if task is None:
            return self.__get_default_ret_map(1, "task is not exists!")

        run_time_list = self.__get_run_time_list(task.pl_id, run_time)
        if run_time_list is None or len(run_time_list) != 1:
            return self.__get_default_ret_map(
                1,
                "<h>执行时间[%s]错误: </h><br>"
                "<h>1. 长度必须是8，10，12.</h><br>"
                "<h>2. 不能超过当前时间!.</h><br>"
                "<h>3. 执行任务数不能超过[%s]</h><br>" %
                (run_time, tools_util.CONSTANTS.MAX_RESTART_TASK_NUM))
        run_time = run_time_list[0]

        if self.__sql_manager.check_pipeline_auth_valid(
                task.pl_id,
                owner_id) != tools_util.UserPermissionType.WRITE:
            return self.__get_default_ret_map(
                1,
                "this owner has no auth to run task")

        graph = self.__graph_mgr.get_graph(str(task_id))
        node_list = []
        succsessors = self.__graph_mgr.get_all_successors_recurrence(
            graph,
            str(task_id),
            node_list)
        node_list.append(str(task_id))
        req_json_list = []
        run_task_set = set()
        for tmp_task_id in node_list:
            succ_task = self.__sql_manager.get_task_info(int(tmp_task_id))
            if succ_task is None:
                return self.__get_default_ret_map(
                    1,
                    "succsessors task is not exists!")

            if succ_task.pl_id != task.pl_id:
                continue

            tmp_str = "%s_%s" % (tmp_task_id, run_time)
            if tmp_str in run_task_set:
                continue

            run_task_set.add(tmp_str)

            task_map = {}
            task_map["task_id"] = tmp_task_id
            task_map["run_time"] = run_time
            req_json_list.append(task_map)

        status, now_running_task_num = \
            self.__sql_manager.get_now_owner_scheduled_task_num(owner_id)
        if now_running_task_num + len(req_json_list) \
                >= tools_util.CONSTANTS.MAX_RUNNING_TASK_NUM:
            return self.__get_default_ret_map(
                1,
                "running task extend max num[%d]" %
                tools_util.CONSTANTS.MAX_RUNNING_TASK_NUM)

        req_map = {}
        req_map["task_pair_list"] = req_json_list
        tmp_req_json = json.dumps(req_map)
        # 编码， 用于发送请求
        req_json = urllib.request.quote(tmp_req_json.encode('gbk'))
        node_req_url = ("http://%s:%s/restart_task?"
                        "task_pair_json=%s&ordered=%s" % (
                            self.__admin_ip,
                            self.__admin_port,
                            req_json,
                            ordered))
        url_stream = "FAIL"
        try:
            url_stream = urllib.request.urlopen(node_req_url).read().decode('utf-8')
        except Exception as ex:
            url_stream = str(ex)

        if url_stream != "OK":
            return self.__get_default_ret_map(
                1,
                "restart task failed! %s" % (
                    url_stream))
        return self.__get_default_ret_map(0, "OK,%s" % tmp_req_json)

    def set_task_success(self, user_id, task_id, run_time):
        try:
            db_task = horae.models.Task.objects.get(id=task_id)
            if self.__sql_manager.check_pipeline_auth_valid(
                    db_task.pl_id,
                    user_id) != tools_util.UserPermissionType.WRITE:
                return 1, "对不起，你没有权限修改这个流程的任务！"

            with django.db.transaction.atomic():
                horae.models.ReadyTask.objects.filter(
                    task_id=task_id, run_time=run_time).update(
                    status=tools_util.TaskState.TASK_SUCCEED)
                horae.models.Schedule.objects.filter(
                    task_id=task_id, run_time=run_time).update(
                    status=tools_util.TaskState.TASK_SUCCEED)
                horae.models.RunHistory.objects.filter(
                    task_id=task_id, run_time=run_time).update(
                    status=tools_util.TaskState.TASK_SUCCEED)
            return {'status': 0, 'msg': '设置成功'}
        except Exception as ex:
            self.__log.error('set task success fail, %s' % traceback.format_exc())
            return {'status': 1, 'msg': str(ex)}

    def run_one_by_one_task(self, owner_id, task_pair_list, ordered=0):
        if self.__admin_ip is None or self.__admin_port is None:
            return self.__get_default_ret_map(1, "no admin server running ttt3!")

        if len(task_pair_list) >= \
                tools_util.CONSTANTS.MAX_RESTART_TASK_NUM:
            return self.__get_default_ret_map(
                1,
                "执行任务数超过了每次提交限制数[%d]" %
                tools_util.CONSTANTS.MAX_RESTART_TASK_NUM)

        req_json_list = []
        for task_pair in task_pair_list:
            task_id = task_pair[0]
            run_time = task_pair[1]
            task = self.__sql_manager.get_task_info(task_id)
            if task is None:
                return self.__get_default_ret_map(1, "task is not exists!")

            run_time_list = self.__get_run_time_list(task.pl_id, run_time)
            if run_time_list is None or len(run_time_list) != 1:
                return self.__get_default_ret_map(
                    1,
                    "<h>执行时间[%s]错误: </h><br>"
                    "<h>1. 长度必须是8，10，12.</h><br>"
                    "<h>2. 不能超过当前时间!.</h><br>"
                    "<h>3. 执行任务数不能超过[%s]</h><br>" %
                    (run_time, tools_util.CONSTANTS.MAX_RESTART_TASK_NUM))
            run_time = run_time_list[0]

            if self.__sql_manager.check_pipeline_auth_valid(
                    task.pl_id,
                    owner_id) != tools_util.UserPermissionType.WRITE:
                return self.__get_default_ret_map(
                    1,
                    "对不起，你没有权限启动这个流程的任务！")

            task_map = {}
            task_map["task_id"] = str(task_id)
            task_map["run_time"] = run_time
            req_json_list.append(task_map)

        status, now_running_task_num = \
            self.__sql_manager.get_now_owner_scheduled_task_num(owner_id)
        if now_running_task_num + len(req_json_list) \
                >= tools_util.CONSTANTS.MAX_RUNNING_TASK_NUM:
            return self.__get_default_ret_map(
                1,
                "执行任务数超过了每次提交限制数[%d]" %
                tools_util.CONSTANTS.MAX_RUNNING_TASK_NUM)

        req_map = {}
        req_map["task_pair_list"] = req_json_list
        tmp_req_json = json.dumps(req_map)
        # 编码， 用于发送请求
        req_json = urllib.request.quote(tmp_req_json.encode('gbk'))
        node_req_url = ("http://%s:%s/restart_task?"
                        "task_pair_json=%s&ordered=%s" % (
                            self.__admin_ip,
                            self.__admin_port,
                            req_json,
                            ordered))
        url_stream = "FAIL"
        try:
            url_stream = urllib.request.urlopen(node_req_url).read().decode('utf-8')
        except Exception as ex:
            url_stream = str(ex)

        if url_stream != "OK":
            return self.__get_default_ret_map(
                1,
                "执行任务失败，详情[%s]" % (
                    url_stream))
        return self.__get_default_ret_map(0, "OK")

    def get_retry_history_list(self, user_id, schedule_id):
        run_history = horae.models.RunHistory.objects.get(schedule_id=schedule_id)
        rerun_histories = horae.models.RerunHistory.objects.filter(
            schedule_id=schedule_id).order_by("-end_time")
        res_list = []
        res_list.append({
            "rerun_id": 0,
            "start_time": run_history.end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": run_history.status
        })

        for rerun_his in rerun_histories:
            res_list.append({
                "rerun_id": rerun_his.id,
                "start_time": rerun_his.end_time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": rerun_his.status
            })

        return {"status": 0, "msg": "OK", "his_list": res_list}

    def pipeline_off_or_on_line(self, owner_id, pipeline_id, on_line, reason):
        status, info = self.__sql_manager.pipeline_off_or_on_line(
            owner_id,
            pipeline_id,
            on_line)
        return self.__get_default_ret_map(status, info)

    def copy_pipeline(self, owner_id, src_pl_id, new_pl_name, project_id, use_type_src):
        if new_pl_name.strip() == '':
            return self.__get_default_ret_map(1, "DAG流名不能为空！")
        status, pl_id = self.__sql_manager.copy_pipeline(
            owner_id,
            src_pl_id,
            new_pl_name,
            project_id,
            use_type_src)
        if status != 0:
            return self.__get_default_ret_map(1, pl_id)
        ret_map = {}
        ret_map["status"] = 0
        ret_map["info"] = "OK"
        ret_map["pl_id"] = pl_id
        return json.dumps(ret_map)

    def copy_task(self, owner_id, src_task_id, dest_pl_id):
        status, task = self.__sql_manager.copy_task(
            owner_id,
            src_task_id,
            dest_pl_id)
        if status != 0:
            return self.__get_default_ret_map(
                status,
                "拷贝任务失败！reason[%s]" % task)
        task_map = self.__get_task_map(task)
        ret_map = {}
        ret_map["status"] = 0
        ret_map["info"] = "OK"
        ret_map["task"] = task_map
        return json.dumps(ret_map)
