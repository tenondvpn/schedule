
import os
import re
import datetime
import json
curPath = os.path.split(os.path.realpath(__file__))[0]

class TaskInfo(object):
    
    def __init__(self,name,running,finished,total):
        self.name = name
        self.running_cnt = running
        self.finished_cnt = finished
        self.total_cnt = total

class ResourcePerformance(object):

    @staticmethod
    def collect_odps_using(log_file):
        time_format = "%Y-%m-%d %H:%M:%S"
        ptn = re.compile("([\\w\\d_]+):([\\d]+)/([\\d]+)/([\\d]+)")
        time_ptn = re.compile("20\\d{2}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}") 
        pre_tasks = {}
        cur_tasks = {}
        input = open(log_file,"r")
        pre_time =  None
        total_resource_cpu_using = 0
        total_resource_mem_using = 0
        for line in input:
            match_ret = ptn.findall(line)
            if match_ret and len(match_ret)>0:
                for match_task in match_ret:
                    task_name = match_task[0]
                    running_cnt = int(match_task[1])
                    finished_cnt = int(match_task[2])
                    total_cnt = int(match_task[3])
                    cur_tasks[task_name] = TaskInfo(task_name,running_cnt,finished_cnt,total_cnt)
            time_ret = time_ptn.search(line)
            cur_time = None
            if time_ret:
                time_str = time_ret.group()
                cur_time = datetime.datetime.strptime(time_str,time_format)
            if not cur_time or not pre_time:
                pre_time = cur_time
                continue
            time_interval_time = cur_time - pre_time
            interval_sec = int(time_interval_time.total_seconds())
            if interval_sec<=0:
                continue
            for key in cur_tasks.keys():
                task_info = cur_tasks[key]
                pre_task = None if not pre_tasks.has_key(key) else pre_tasks[key] 
                if task_info.running_cnt == 0 and not pre_task:
                    continue
                running_cnt = task_info.running_cnt
                if running_cnt<=0:
                    continue
                total_resource_cpu_using = total_resource_cpu_using+running_cnt*interval_sec
                total_resource_mem_using = total_resource_mem_using+running_cnt*interval_sec
        input.close()
        return (total_resource_cpu_using,total_resource_mem_using)
    
    @staticmethod
    def collect_job_using(log_file):
        input = open(log_file,"r")
        append = False
        content = ""
        for line in input:
            content = content+line 
        input.close()
        pos = content.find('{')
        content = content[pos:]
        obj = json.loads(content)
        total_resource_cpu_using = 0
        total_resource_mem_using = 0
        for name, task in obj['tasks'].items():
            for instance in task['instances'].values():
                for history in instance['history']:
                    start_time = history['iStartTimeOnMatser']
                    end_time = history['iEndTimeOnMatser']
                    time_interval = end_time - start_time
                    if time_interval<=0:
                        continue
                    total_resource_cpu_using = total_resource_cpu_using+time_interval
                    total_resource_mem_using = total_resource_mem_using+time_interval
    
        return (total_resource_cpu_using,total_resource_mem_using)

if __name__ == "__main__":
    log_file = "%s/test/jstatus.log" % curPath
    log_file = "%s/test/jstatus_stdout.log" % curPath
    print(ResourcePerformance.collect_job_using(log_file))
