###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import time
import random
import threading
import traceback

import networkx as nx
from horae import tools_util
import django.db

class GraphGroup(threading.Thread):
    """ 
        1.生成由任务节点所产生的DAG流图
        2.获取生成的DAG流图的所有子图
        3.插入边时验证是否有圈和自循环
        4.线程不安全
        5.获取子图之后，原始图将无效
    """
    mutex = threading.Lock()
    def __init__(self, logger):
        if not hasattr(self, "a"):
            GraphGroup.mutex.acquire()
            # tools_util.AutoLock(GraphGroup.mutex)
            if not hasattr(self, "a"):
                threading.Thread.__init__(self)
                self.__graph = nx.DiGraph()
                self.__sub_graph_list = []
                self.__log = logger
                self.__TASK_WITH_PIPEID_SQL = (
                        "select a.id, a.pl_id, a.next_task_ids, "
                        "a.prev_task_ids, a.pid, a.server_tag, "
                        "b.type, a.retry_count "
                        "from ( "
                        "   select id, pl_id, next_task_ids, prev_task_ids, "
                        "   pid, retry_count, server_tag from horae_task "
                        ")a "
                        "left outer join ( "
                        "    select id, type from horae_processor "
                        ") b "
                        "on a.pid = b.id where b.type is not null;")

                self.__graph_lock = threading.Lock()

                # 每天5点从db刷新一次
                # 每天更新一次
                self.__refresh_circle = 86400
                self.__prev_timestamp = int(time.time() / 86400) * 86400 - 28800
                self.__prev_timestamp += 5 * 3600 + self.__refresh_circle
                self.__create_graph()
                self.start()
                self.a = "a"  # 必须在后面
            GraphGroup.mutex.release()

    def __new__(cls, *args, **kw):
        if not hasattr(cls, "_instance"):
            orig = super(GraphGroup, cls)
            cls._instance = orig.__new__(cls)

        return cls._instance

    def run(self):
        self.__log.info("GraphGroup thread starting...")
        while not tools_util.CONSTANTS.GLOBAL_STOP:
            begin_time = time.time()
            self.__log.info("GraphGroup handle data started")
            self.__create_graph()
            self.__prev_timestamp += self.__refresh_circle
            # 暂时采用60分钟刷一次，后期可根据情况，确定刷新周期
            use_time = time.time() - begin_time
            self.__log.info("GraphGroup handle data "
                    "exit.use time[%f]" % use_time)
            time.sleep(60 * 60)

        self.__log.info("GraphGroup thread ended!")

    def add_edge(self, from_node, to_node):
        """
            向图中添加一条边
        """
        # tools_util.AutoLock(self.__graph_lock)
        self.__graph_lock.acquire()
        # 不能自循环 
        ret = False
        while True:
            if from_node == to_node:
                break

            from_graph = self.__get_graph_by_node(from_node)
            if from_graph is None:
                to_graph = self.__get_graph_by_node(to_node)
                if to_graph is None:
                    self.__log.warn("to_graph is none![node:%s]" % to_node)
                    break
                to_graph.add_edge(from_node, to_node)
                ret = True
                break

            if from_graph.has_node(to_node):
                if from_graph.has_edge(to_node, from_node):
                    self.__log.warn("graph has edge(%s, %s)" % (
                            to_node, from_node))
                    break

                if from_graph.has_edge(from_node, to_node):
                    self.__log.warn("graph has edge(%s, %s)" % (
                            from_node, to_node))
                    ret = True
                    break
                from_graph.add_edge(from_node, to_node)
                # 不能有圈，圈的检查性能非常差，注意！！！
                if len(list(nx.cycles.simple_cycles(from_graph))) != 0:
                    from_graph.remove_edge(from_node, to_node)
                    self.__log.warn("graph has circle arter add edge (%s, %s)" % (
                            from_node, to_node))
                    break
                ret = True
                break

            to_graph = self.__get_graph_by_node(to_node)
            if to_graph is None:
                from_graph.add_edge(from_node, to_node)
                ret = True
                break

            tmp_graph = self.__merge_two_graph(from_graph, to_graph)
            tmp_graph.add_edge(from_node, to_node)
            ret = True
            break
            
        self.__graph_lock.release()
        return ret

    def remove_edge(self, from_node, to_node):
        # tools_util.AutoLock(self.__graph_lock)
        self.__graph_lock.acquire()
        graph = self.__get_graph_by_node(from_node)
        if graph is None:
            self.__graph_lock.release()
            return True

        if not graph.has_edge(from_node, to_node):
            self.__graph_lock.release()
            return True

        self.__delete_graph_from_sub_graph_list(from_node)
        
        graph.remove_edge(from_node, to_node)
        tmp_graph_list = []
        self.__graph = graph
        self.__get_sub_graphs(tmp_graph_list)
        if len(tmp_graph_list) <= 0:
            self.__graph_lock.release()
            return True

        for graph in tmp_graph_list:
            self.__sub_graph_list.append(graph)

        self.__graph_lock.release()
        return True

    def add_node(self, node):
        # tools_util.AutoLock(self.__graph_lock)
        self.__graph_lock.acquire()
        graph = self.__get_graph_by_node(node)
        if graph is not None:
            self.__graph_lock.release()
            return False
        graph = nx.DiGraph()
        graph.add_node(node)
        self.__sub_graph_list.append(graph)
        self.__graph_lock.release()
        return True

    def remove_node(self, node):
        # tools_util.AutoLock(self.__graph_lock)
        self.__graph_lock.acquire()
        graph = self.__get_graph_by_node(node)
        if graph is None:
            self.__graph_lock.release()
            return False
        self.__delete_graph_from_sub_graph_list(node)

        graph.remove_node(node)
        tmp_graph_list = []
        self.__graph = graph
        self.__get_sub_graphs(tmp_graph_list)
        if len(tmp_graph_list) <= 0:
            self.__graph_lock.release()
            return True

        for graph in tmp_graph_list:
            self.__sub_graph_list.append(graph)
        self.__graph_lock.release()
        return True

    def get_graph(self, node):
        return self.__get_graph_by_node(node)

    def get_all_successors_recurrence(self, graph, begin_node, nodes_list):
        if begin_node is None:
            return

        successors = list(graph.successors(begin_node))
        if len(successors) <= 0:
            return

        for suc_node in successors:
            nodes_list.append(suc_node)
            self.get_all_successors_recurrence(graph, suc_node, nodes_list)

    def get_all_preccessors_recurrence(self, graph, begin_node, nodes_list):
        if begin_node is None:
            return

        preccessors = list(graph.predecessors(begin_node))
        if preccessors is None or len(preccessors) <= 0:
            return

        for pre_node in preccessors:
            nodes_list.append(pre_node)
            self.get_all_preccessors_recurrence(graph, pre_node, nodes_list)

    def __delete_graph_from_sub_graph_list(self, node):
        sub_graph_len = len(self.__sub_graph_list)
        for i in range(0, sub_graph_len):
            if self.__sub_graph_list[i].has_node(node):
                del self.__sub_graph_list[i]
                return True
        return False

    def __merge_two_graph(self, from_graph, to_graph):
        nodes = list(to_graph.nodes())
        if len(nodes) <= 0:
            return None
        sub_graph_len = len(self.__sub_graph_list)
        for i in range(0, sub_graph_len):
            if self.__sub_graph_list[i].has_node(nodes[0]):
                del self.__sub_graph_list[i]
                break

        tmp_graph = from_graph
        for node in nodes:
            tmp_graph.add_node(node)

        for edge in to_graph.edges():
            tmp_graph.add_edge(edge[0], edge[1])
        return tmp_graph

    def __get_graph_by_node(self, node):
        for tmp_graph in self.__sub_graph_list:
            if tmp_graph.has_node(node):
                return tmp_graph
        return None

    def __create_graph(self):
        # tools_util.AutoLock(self.__graph_lock)
        self.__graph_lock.acquire()
        task_map = {}
        if not self.__get_all_tasks(task_map):
            self.__log.error("get tasks from db failed!")
            self.__graph_lock.release()
            return False

        self.__graph.clear()
        self.__sub_graph_list = []

        for task_key in task_map:
            # 将节点及其下游节点放入图中
            next_tag_list = task_map[task_key][2].replace(
                    ' ', 
                    '').split(",")
            for next_task_key in next_tag_list:
                if next_task_key not in task_map:
                    continue

                if next_task_key != '' and next_task_key != '0':
                    self.__graph.add_edge(task_key, next_task_key)
            self.__graph.add_node(task_key)
        self.__get_sub_graphs(self.__sub_graph_list)
        self.__graph_lock.release()
        return True

    def __get_sub_graphs(self, graph_list):
        """
            递归获取子图
        """
        while self.__graph.number_of_nodes() > 0:
            for node in list(self.__graph.nodes()):
                new_graph = nx.DiGraph()
                pred_node_list = list(self.__graph.predecessors(node))
                if len(pred_node_list) == 0:
                    self.__get_all_successor(new_graph, node)
                    if new_graph.number_of_nodes() > 0:
                        graph_list.append(new_graph)
                    break

        #if self.__graph.number_of_nodes() > 0:
        #    self.__get_sub_graphs(graph_list)

    def __remove_isolate_node(self, node):
        if len(list(self.__graph.predecessors(node))) != 0:
            return

        if len(list(self.__graph.successors(node))) != 0:
            return
        
        self.__graph.remove_node(node)

    def __get_all_presuccessor(self, new_graph, suc_node):
        presuccessors = list(self.__graph.predecessors(suc_node))
        if len(presuccessors) == 0:
            if len(list(self.__graph.successors(suc_node))) == 0:
                if not new_graph.has_node(suc_node):
                    new_graph.add_node(suc_node)

                if self.__graph.has_node(suc_node):
                    self.__graph.remove_node(suc_node)
                return
            else:
                return self.__get_all_successor(new_graph, suc_node)

        for pre_node in presuccessors:
            if not new_graph.has_edge(pre_node, suc_node):
                new_graph.add_edge(pre_node, suc_node)

            if self.__graph.has_edge(pre_node, suc_node):
                self.__graph.remove_edge(pre_node, suc_node)
                self.__remove_isolate_node(pre_node)
                self.__remove_isolate_node(suc_node)

            if self.__graph.has_node(pre_node):
                if len(list(self.__graph.successors(pre_node))) != 0:
                    self.__get_all_successor(new_graph, pre_node)

            if self.__graph.has_node(pre_node):
                if len(list(self.__graph.predecessors(pre_node))) != 0:
                    self.__get_all_presuccessor(new_graph, pre_node)

    def __get_all_successor(self, new_graph, pre_node):
        successors = list(self.__graph.successors(pre_node))
        if len(successors) == 0:

            if len(list(self.__graph.predecessors(pre_node))) == 0:
                if not new_graph.has_node(pre_node):
                    new_graph.add_node(pre_node)

                if self.__graph.has_node(pre_node):
                    self.__graph.remove_node(pre_node)
                return
            else:
                return self.__get_all_presuccessor(new_graph, pre_node)
            
        for succ_node in successors:
            if not new_graph.has_edge(pre_node, succ_node):
                new_graph.add_edge(pre_node, succ_node)

            if self.__graph.has_edge(pre_node, succ_node):
                self.__graph.remove_edge(pre_node, succ_node)
                self.__remove_isolate_node(pre_node)
                self.__remove_isolate_node(succ_node)

            if self.__graph.has_node(succ_node):
                if len(list(self.__graph.successors(succ_node))) != 0:
                    self.__get_all_successor(new_graph, succ_node)

            if self.__graph.has_node(succ_node):
                if len(list(self.__graph.predecessors(succ_node))) != 0:
                    self.__get_all_presuccessor(new_graph, succ_node)

    # 从schedule，task表获取所有的可调用的任务
    def __get_all_tasks(self, tasks_map):
        try:
            cursor = django.db.connection.cursor()
            cursor.execute(self.__TASK_WITH_PIPEID_SQL)
            task_records = cursor.fetchall()
            for task in task_records:
                tasks_map[str(task[0])] = task
            return True
        except Exception as ex:
            self.__log.error("execute sql failed![%s][ex:%s][trace:%s]!" % (
                    self.__TASK_WITH_PIPEID_SQL,
                    str(ex),
                    traceback.format_exc()))
            return False

if __name__ == "__main__":
    print(time.time())
    test_group = GraphGroup()
    for i in range(0, 100):
        frm = random.randint(0, 120);
        to = random.randint(0, 120);
        if (frm == to):
            continue
        test_group.add_eage(frm, to, True)
    print(time.time())
