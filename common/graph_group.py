###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import time
import random

import networkx as nx

class GraphGroup(object):
    """ 
        1.生成由任务节点所产生的流程图
        2.获取生成的流程图的所有子图
        3.插入边时验证是否有圈和自循环
        4.线程不安全
        5.获取子图之后，原始图将无效
    """
    def __init__(self):
        self.__graph = nx.DiGraph()

    def add_eage(self, from_node, to_node, check_circle=False):
        """
            向图中添加一条边
        """
        # 不能自循环 
        if from_node == to_node:
            return False

        if self.__graph.has_edge(from_node, to_node):
            return False
        # 不能相同两个节点自循环
        if self.__graph.has_edge(to_node, from_node):
            return False;
        self.__graph.add_edge(from_node, to_node)

        if check_circle:
            # 不能有圈，圈的检查性能非常差，注意！！！
            if len(list(nx.cycles.simple_cycles(self.__graph))) != 0:
                self.__graph.remove_edge(from_node, to_node)
                return False
        return True

    def add_node(self, node):
        if self.__graph.has_node(node):
            return False
        self.__graph.add_node(node)
        return True

    def get_graph(self):
        return self.__graph

    def get_sub_graphs(self, graph_list):
        """
            递归获取子图
        """
        while self.__graph.number_of_nodes() > 0:
            for node in self.__graph.nodes():
                new_graph = nx.DiGraph();
                pred_node_list = list(self.__graph.predecessors(node))
                if len(pred_node_list) == 0:
                    self.__get_all_successor(new_graph, node)
                    if new_graph.number_of_nodes() > 0:
                        graph_list.append(new_graph)
                    break
        '''
        if self.__graph.number_of_nodes() > 0:
            self.get_sub_graphs(graph_list)
        '''

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
