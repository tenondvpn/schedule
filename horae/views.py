# coding=utf-8
import sys
sys.setrecursionlimit(10000)
import os
import datetime
import configparser
import shutil
import hashlib

from django.shortcuts import render 
from horae.http_helper import *
from horae.http_decorators import add_visit_record
from django.contrib.auth.models import User
from django.contrib.auth.decorators import login_required
from django.contrib.auth import authenticate

from horae.tools_util import StaticFunction
from horae.models import Pipeline, Processor, Task, Edge
from horae.forms import PipelineForm, ProcessorForm, TaskForm
from horae.horae_interface import *
from horae import common_logger
from common.util import is_admin
from horae import no_block_sys_cmd
from clickhouse_driver import Client

horae_interface = HoraeInterface()
ck_client = Client(host='localhost', user='default', password='')


# log/user_log
logger = common_logger.get_logger(
    "view_log",
    "./log/view_log")

config = configparser.ConfigParser()
config.read("./conf/tools.conf")
local_package_path = config.get("tools", "package_path").strip()

UPLOAD_FILES_PATH = 'upload_files/file/'
# WORK_PATH = '/home/admin/doc/rh_ark_tools/upload_files/file/'
WORK_PATH = settings.BASE_DIR + '/upload_files/file/'
__no_block_cmd = no_block_sys_cmd.NoBlockSysCommand(logger)

CURRENT_URL = settings.SITE_URL
NOT_RETRY = 1
ONE_TIME = 2
FIVE_TIME = 3
ALWAYS = 4

@login_required(login_url='/login/')
def noah_index(request):
    return render(request, 'noah_index.html', {"pipe_id": -1})

def liviz_index(request):
    return render(request, 'liviz_js.html', {"pipe_id": -1})

@login_required(login_url='/login/')
def pindex(request):
    return render(request, 'pindex.html', {"pipe_id": -1})

@login_required(login_url='/login/')
@add_visit_record
def detail(request, pipe_id):
    return render(request, 'noah_index.html', {"pipe_id": pipe_id})

def port_filter(request):
    return render(request, 'port_filter.html')

@login_required(login_url='/login/')
def get_project_tree(request):
    user = request.user

    try:
        type = 0
        if "type" in request.GET:
            type = int(request.GET.get('type'))

        result = horae_interface.get_project_tree(user.id, type)
        return JsonHttpResponse(result)
    except Exception as ex:
        logger.error('get_project_tree  error:<%s>' % str(ex))
        return JsonHttpResponse({'status': 1, 'msg': str(ex)})

@login_required(login_url='/login/')
def search_pipeline(request):
    user = request.user

    try:
        word = request.POST.get("word")
        with_project = 0
        limit = 100
        if "with_project" in request.POST:
            with_project = int(request.POST.get("with_project"))

        if "limit" in request.POST:
            limit = int(request.POST.get("limit"))

        result = horae_interface.search_pipeline(user.id, word, with_project, limit)
        return JsonHttpResponse(result)
    except Exception as ex:
        logger.error('search_pipeline  error:<%s>, trace:%s' % (str(ex), traceback.format_exc()))
        return JsonHttpResponse({'status': 1, 'msg': str(ex)})

@login_required(login_url='/login/')
def get_project_tree_async(request):
    user = request.user

    tree_id = 0
    if "id" in request.GET:
        tree_id = request.GET.get("id")

    type = 0
    if "type" in request.GET:
        type = int(request.GET.get('type'))

    try:
        result = horae_interface.get_project_tree_async(user.id, tree_id, type)
        return JsonHttpResponse(result)
    except Exception as ex:
        logger.error('get_project_tree_async  error:<%s>' % str(ex))
        return JsonHttpResponse({'status': 1, 'msg': str(ex)})


def get_pipeline_info(pipe_id):
    principal_name_list = ''
    principal_id_list = ''

    pipeline_info = horae_interface.get_pipeline_info(int(pipe_id))

    pipeline_info = json.loads(pipeline_info)
    pipeline_info = pipeline_info['pipeline']
    pipeline_principal_list = pipeline_info['owner_list']

    # 获取有读写权限的用户list(负责人)
    for principal in pipeline_principal_list:
        principal_name_list += principal['username'] + ','
        principal_id_list += str(principal['id']) + ','
    principal_name_list = principal_name_list[:-1]
    principal_id_list = principal_id_list[:-1]

    pipeline_info['principal_name_list'] = principal_name_list
    pipeline_info['principal_id_list'] = principal_id_list

    return pipeline_info

@login_required(login_url='/login/')
def get_pipeline_detail(request):
    user = request.user

    try:
        pipe_id = request.POST.get("pipe_id")
        pipe_info = get_pipeline_info(pipe_id)
        pipe_info['is_super'] = is_admin(user.id)
        pipe_info['user_id'] = user.id
        return JsonHttpResponse(pipe_info)
    except Exception as ex:
        logger.error('get_project_tree_async  error:<%s> trace[%s]' % (
            str(ex), traceback.format_exc()))
        return JsonHttpResponse({'status': 1, 'msg': str(ex)})

@login_required(login_url='/login/')
def get_tasks(request):
    if request.method == 'POST':
        user = request.user

        pipeline_id = request.POST.get("pipeline_id")
        try:
            tasks = horae_interface.get_tasks_by_pipeline_id(int(pipeline_id))
            task_list = json.loads(tasks)['tasks']
        except Exception as ex:
            logger.error('get tasks fail: <%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': str(ex)})
        return JsonHttpResponse(
            {'status': 0, 'task_list': task_list})

@login_required(login_url='/login/')
def unlink_task(request):
    if request.method == 'POST':
        user = request.user

        link_from = request.POST.get('Link[from]')
        link_to = request.POST.get('Link[to]')
        try:
            result = horae_interface.delete_edge(user.id, int(link_from), int(link_to))
            status, msg = status_msg(result)
        except Exception as ex:
            logger.error('delete edge fail: <%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': str(ex)})
        return JsonHttpResponse(
            {'status': status, 'msg': msg})

@login_required(login_url='/login/')
def link_task(request):
    if request.method == 'POST':
        user = request.user
        try:
            link_from = request.POST.get('prev_task_id')
            link_to = request.POST.get('next_task_id')
            stream_type = 0
            file_name = ""
            rcm_context = ""
            rcm_topic = ""
            rcm_partition = 0
            dispatch_tag = request.POST.get('dispatch_tag')
            if rcm_partition == '' or rcm_partition is None:
                rcm_partition = 0

            if dispatch_tag == '' or dispatch_tag is None:
                dispatch_tag = -1

            task = Task.objects.get(id=link_to)
            t = time.time()
            edge = Edge(
                prev_task_id=int(link_from),
                next_task_id=int(link_to),
                stream_type=int(stream_type),
                file_name=file_name,
                rcm_context=rcm_context,
                rcm_topic=rcm_topic,
                rcm_partition=int(rcm_partition),
                dispatch_tag=int(dispatch_tag),
                pipeline_id=task.pl_id,
                last_update_time_us=int(round(t * 1000000))
            )

            result = horae_interface.add_edge(user.id, int(link_from), int(link_to), edge)
            status, msg = status_msg(result)
        except Exception as ex:
            logger.error('add edge fail: <%s>, trace[%s]' % (str(ex), traceback.format_exc()))
            return JsonHttpResponse(
                {'status': 1, 'msg': str(ex)})
        return JsonHttpResponse(
            {'status': status, 'msg': msg})

    # if request.method == 'POST':
    #     user = request.user

    #     link_from = request.POST.get('Link[from]')
    #     link_to = request.POST.get('Link[to]')
    #     try:
    #         result = horae_interface.add_edge(user.id, int(link_from), int(link_to))
    #         status, msg = status_msg(result)
    #     except Exception as ex:
    #         logger.error('add edge fail: <%s>' % str(ex))
    #         return JsonHttpResponse(
    #             {'status': 1, 'msg': str(ex)})
    #     return JsonHttpResponse(
    #         {'status': status, 'msg': msg})

def status_msg(result):
    result = json.loads(result)
    status = result['status']
    msg = result['info']
    return status, msg

def check_life_cycle(life_cycle):
    max_time = datetime.datetime.utcnow() + \
               datetime.timedelta(hours=8) + \
               datetime.timedelta(days=180)
    max_time_str = max_time.strftime("%Y%m%d")
    return max_time_str >= life_cycle

def update(request, pipe_id):
    pipeline = Pipeline.objects.get(id=pipe_id)
    user = request.user

    is_super = is_admin(user)
    if request.method == 'POST':
        form = PipelineForm(request.POST, instance=pipeline)
        if form.is_valid():
            try:
                name = form.cleaned_data['name']
                ct_time = form.cleaned_data['ct_time']
                principal = form.cleaned_data['principal']
                send_mail = form.cleaned_data['send_mail']
                send_sms = form.cleaned_data['send_sms']
                tag = form.cleaned_data['tag']
                project_group = form.cleaned_data['project_id']
                life_cycle = form.cleaned_data['life_cycle']
                description = form.cleaned_data['description']

                check_result = check_life_cycle(life_cycle)
                if not check_result:
                    raise Exception('生命周期设置不能超过6个月！')

                pipelines = Pipeline.objects.filter(name=name)
                if pipelines:
                    if not pipeline in pipelines:
                        raise Exception('指数名重复！')

                if not project_group:
                    project_group = 0

                #报警方式0邮件，1钉钉，2二者都报
                monitor_way = -1
                if send_mail:
                    if send_sms:
                        monitor_way = 2
                    else:
                        monitor_way = 0
                elif send_sms:
                    monitor_way = 1
                else:
                    monitor_way = -1

                result = horae_interface.update_pipeline(int(pipe_id), user.id,
                                                         life_cycle, name, ct_time, principal, monitor_way,
                                                         tag, description, 0, project_group)
                status, msg = status_msg(result)
            except Exception as ex:
                logger.error('update pipeline fail: <%s>' % str(ex))
                return JsonHttpResponse(
                    {'status': 1, 'msg': str(ex)})

            return JsonHttpResponse(
                {'status': status, 'msg': msg, 'send_mail': send_mail, 'send_sms': send_sms})
        else:
            return JsonHttpResponse({'status': 1,
                                     'msg': form.errors.items()})
    else:
        user = request.user

        form = PipelineForm(instance=pipeline)
        form.send_mail = False
        form.send_sms = False
        if pipeline.monitor_way == 2:
            form.send_mail = True
            form.send_sms = True
        elif pipeline.monitor_way == 1:
            form.send_sms = True
        elif pipeline.monitor_way == 0:
            form.send_mail = True

        print("DDDDDDDDDDDD: %d, %d, %d" % (pipeline.monitor_way, form.send_mail, form.send_sms))
        pipeline_info = get_pipeline_info(pipe_id)
    return render(request, 'update_pipeline.html',
                  {'form': form, 'pipeline': pipeline_info, 'is_super': is_super, 'page_title': '修改指数',
                   'pipeline_model': 1, 'page_index': 2})

@login_required(login_url='/login/')
def copy_pipeline(request):
    if request.method == 'POST':
        user = request.user

        pl_id = request.POST.get('pl_id')
        pl_name = request.POST.get('pl_name')
        project_id = request.POST.get('project_id')
        try:
            result = horae_interface.copy_pipeline(
                int(user.id),
                int(pl_id),
                pl_name,
                int(project_id))
            res = json.loads(result)
            if int(res["status"]) != 0:
                return JsonHttpResponse({'status': 1, 'msg': res["info"]})
            return JsonHttpResponse({'status': 0, 'msg': "OK", 'pl_id': res["pl_id"]})
        except Exception as ex:
            logger.error('get graph  error:<%s>' % str(ex))
            return JsonHttpResponse({'status': 1, 'msg': str(ex)})

@login_required(login_url='/login/')
@add_visit_record
def delete(request, pipe_id):
    if request.method == 'POST':
        user = request.user

        pipeline = Pipeline.objects.get(id=pipe_id)
        try:
            result = horae_interface.delete_pipeline(user.id, int(pipe_id))
            status, msg = status_msg(result)
        except Exception as ex:
            logger.error('delete pipeline fail: <%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': 'error:' + str(ex)})
        return JsonHttpResponse({'status': status, 'msg': msg})

def retry_count_num(retry_count):
    num = 0
    if retry_count == ONE_TIME:
        num = 1
    elif retry_count == FIVE_TIME:
        num = 5
    elif retry_count == ALWAYS:
        num = -1
    return num

def server_tag_db(server_tag):
    if server_tag == '':
        server_tag = 'ALL'
    return server_tag

@login_required(login_url='/login/')
def get_datas(request):
    if request.method == 'POST':
        data_type = request.POST.get('data_type')
        try:
            datas = []
            if int(data_type) == 1:
                #oss
                for i in range(10):
                    datas.append({"id": i, "name": "oss://geocloud/xl/xl_test" + str(i)})
            else:
                #odps
                for i in range(10):
                    datas.append({"id": i + 10, "name": "process.test_table_" + str(i)})

            data_list = []
            for data in datas:
                data_map = {}
                data_map["id"] = data["id"]
                data_map["name"] = data["name"]
                data_list.append(data_map)
            ret_map = {}
            ret_map["status"] = 0
            ret_map["info"] = "OK"
            ret_map["data_list"] = data_list
        except Exception as ex:
            logger.error("get datas failed! <%s>" % str(ex))
            return JsonHttpResponse(
                    {'status':1,'msg':str(ex)})
        return JsonHttpResponse(
                {'status':0,'data_list':data_list})
    
@login_required(login_url='/login/')
@add_visit_record
def update_task(request, task_id):
    user = request.user

    is_super = is_admin(user)
    task = Task.objects.get(id=task_id)
    if request.method == 'POST':
        form = TaskForm(request.POST, instance=task)
        if form.is_valid():
            try:
                name = form.cleaned_data['name']
                config = form.cleaned_data['config']
                retry_count = form.cleaned_data['retry_count']
                over_time = form.cleaned_data['over_time']
                priority = form.cleaned_data['priority']
                prev_task_ids = form.cleaned_data['prev_task_ids']
                server_tag = form.cleaned_data['server_tag']
                description = form.cleaned_data['description']
                proc_template = form.cleaned_data['template']
                version_id = 0
                try:
                    version_id = int(request.POST.get("version_id"))
                except Exception as ex:
                    version_id = 0

                if version_id <= 0:
                    version_id = 0

                if proc_template == 'null':
                    proc_template = None

                task.name = name
                task.config = config
                task.retry_count = retry_count_num(int(retry_count))
                task.over_time = int(over_time)
                task.prev_task_ids = prev_task_ids
                task.description = description
                task.priority = priority
                task.server_tag = server_tag_db(server_tag)
                task.version_id = version_id

                result = horae_interface.update_tasks(user.id, task, None, proc_template)
                res = json.loads(result)
                status = res['status']
                msg = res['info']
                task = {}
                if 'task' in res:
                    task = res['task']
            except Exception as ex:
                logger.error('update task fail: <%s>' % str(ex))
                return JsonHttpResponse(
                    {'status': 1, 'msg': str(ex)})

            return JsonHttpResponse(
                {'status': status, 'msg': msg, 'task': task})
        else:
            return JsonHttpResponse({'status': 1,
                                     'msg': form.errors.items()})
    else:
        print("get upload history succ 0: ")
        form = TaskForm(instance=task)
        task_info = get_task_info(task_id)
        pipe_id = task_info['pl_id']
        config = task_info['config']
        config_str = template_list(config)
        processor = get_processor_info(task_info['pid'])
        history_list = []
        tmp_map = {}
        tmp_map["id"] = task.version_id
        tmp_map["version"] = "0.0.0"
        tmp_map["name"] = "0.0.0"
        tmp_map["type"] = -1
        history_list.append(tmp_map)
        try:
            history_list = get_processor_upload_history(task_info['pid'])
            print("get upload history succ: ")
            print(history_list)
        except Exception as ex:
            print("get upload history catch ex: " + str(ex))
            pass

        rely_tasks = ''
        prev_task_ids = task_info['prev_task_ids']
        if prev_task_ids != 'null' and prev_task_ids is not None:
            rely_tasks = get_rely_tasks(prev_task_ids)
            # dict转json串
            rely_tasks = json.dumps(rely_tasks)
        else:
            rely_tasks = json.dumps(rely_tasks)
        server_tag = Task.objects.get(id=int(task_id)).server_tag

        # 获取公式上传包
        proc_name = processor['name']
        user = request.user

        username = user.username
        password = '<font color=red>${password}</font>'
        cmd = upload_cmd(request, username, password, proc_name)

    print("get upload history succ 1: ")
    proc_quote_json = horae_interface.get_processor_quote_num(processor['id'])
    result = json.loads(proc_quote_json)
    status = result['status']
    quote_num = 0
    if status == 0:
        quote_num = result['quote_num']

    pipeline = Pipeline.objects.get(id=pipe_id)
    return render(request, 'update_task.html',
                  {'form': form, 'task': task_info,
                   'config': config_str, 'pipe_id': pipe_id, 'pipe_name': pipeline.name,
                   'processor': processor, 'rely_tasks': rely_tasks,
                   'server_tag': server_tag, 'cmd': cmd, 'page_title': '修改任务',
                   'pipeline_model': 1, 'quote_num': quote_num, 'page_index': 2,
                   'is_super': is_super, 'version_list': history_list
                   })

@login_required(login_url='/login/')
@add_visit_record
def show_block(request, hash):
    user = request.user
    print("get hash " + hash)
    return render(request, 'show_block.html',
                  {'block_hash': hash, 'type': 0})

@login_required(login_url='/login/')
@add_visit_record
def update_block(request, hash):
    user = request.user
    prehash = ""
    timestamp = 0
    config = ""
    tmp_from = ""
    tmp_to = ""
    try:
        fileds = ck_client.execute("select prehash, hash, timestamp from default.zjc_ck_block_table where hash='" + hash + "';")
        for filed in fileds:
            prehash = filed[0]
            datetime_obj = datetime.datetime.fromtimestamp(filed[2] / 1000 + 8*3600)
            datetime_str = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
            timestamp = datetime_str

        fileds = ck_client.execute("select from, to, gas_used from default.zjc_ck_transaction_table where hash='" + hash + "' limit 20;")
        for filed in fileds:
            tmp_from = filed[0]
            tmp_to = filed[1]
    except Exception as ex:
        logger.error('select fail: %s' % str(ex))
        print('select fail: %s' % str(ex))
        return JsonHttpResponse([])

    is_super = is_admin(user)
    message = (prehash + hash).encode('utf-8')
    hash_object = hashlib.sha256(message)
    hex_dig = hash_object.hexdigest()
    return render(request, 'update_block.html',
                  {'block_timestamp': timestamp,
                   'cmd': '', 'page_title': '修改任务',
                   'pipeline_model': 1, 'quote_num': 0, 'page_index': 2,
                   'from': tmp_from, 'to': tmp_to, 'auth_hash': hex_dig,
                   'is_super': is_super, 'version_list': history_list,
                   'block_hash': hash, 'block_prehash': prehash,
                   })

def template_list(template):
    template_str = ''
    if template != '' and template is not None:
        templates = template.split('\n')
        for temp in templates:
            if temp:
                template_str += temp.strip() + '\n'
    template_str = template_str.strip()
    return template_str

# 获取任务详情
def get_task_info(task_id):
    task_info = horae_interface.get_task_info(int(task_id))
    task_info = json.loads(task_info)
    if task_info['status'] == 0:
        task_info = task_info['task']
        return task_info
    else:
        return ''

# 获取公式详情
def get_processor_info(proc_id):
    processor_info = horae_interface.get_processor_info(int(proc_id))
    processor_info = json.loads(processor_info)
    if 'processor' in processor_info:
        processor_info = processor_info['processor']
        return processor_info
    else:
        raise Exception('公式不存在！')

# 获取上传包历史
def get_processor_upload_history(proc_id):
    result = horae_interface.get_processor_package_history(proc_id)
    result = json.loads(result)
    history_list = result['package_historys']
    return history_list

# 获取依赖任务和相应指数list
def get_rely_tasks(prev_task_ids):
    rely_tasks = {}
    prev_task_ids = prev_task_ids.strip()
    prev_task_ids = prev_task_ids.strip(',')
    rely_task_list = prev_task_ids.split(',')
    if len(rely_task_list) > 0 and prev_task_ids != '':
        for task_id in rely_task_list:
            if get_task_info(task_id) != '':
                rely_tasks[task_id] = get_task_info(task_id)['pl_id']
    return rely_tasks

def upload_cmd(request, username, password, proc_name):
    cmd = ''
    cmd += ('curl "http://' + request.get_host() + '/processor/upload_processor/"' +
           ' -F "user=' + username + '" -F "pass=' + password + '" -F "proc_name=' +
           proc_name + '" -F "package=@<font color=red>${package_name}</font>"' +
           ' -F "version_name=<font color=red>${version_name}</font>"' +
           ' -F "description=<font color=red>${description}</font>"');
    return cmd

def get_pipelines(request):
    if request.method == 'POST':
        user = request.user

        task_id = request.POST.get("task_id")
        try:
            pipelines = horae_interface.get_all_authed_pipeline_info(user.id, task_id)
            pipeline_list = json.loads(pipelines)['pipelines']

        except Exception as ex:
            logger.error('get pipelines fail: <%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': str(ex)})
        return JsonHttpResponse(
            {'status': 0, 'pipeline_list': pipeline_list})

# 获取机器标签
def get_server_tags(request):
    if request.method == 'POST':
        server_tags = ''
        try:
            status = 0
            msg = 'OK'
        except Exception as ex:
            logger.error('get server_tags fail: <%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': 'error:' + str(ex)})
        return JsonHttpResponse(
            {'status': status, 'msg': msg, 'server_tags': server_tags})

def create_task_choose_proc(request):
    user = request.user

    is_super = is_admin(user)
    return render(request, 'create_task_choose_proc.html',
                  {'processor': [],
                   'type_str': '', 'private_str': '',
                   'is_owner': '', 'config_str': '', 'is_super': is_super,
                   'owner_name': '', 'page_title': '公式详情', 'pipeline_model': 1,
                   'page_index': 4
                   })

@login_required(login_url='/login/')
def get_processor_tree_async(request):
    user = request.user

    tree_id = 0
    if "id" in request.GET:
        tree_id = int(request.GET.get("id"))

    if tree_id == 0:
        return JsonHttpResponse([
                {"is_project": 1,
                 "text": tools_util.global_processor_top_type_map[tools_util.PROCESSOR_TOP_TYPE.USER_OWNER_PROC],
                 "id": tools_util.PROCESSOR_TOP_TYPE.USER_OWNER_PROC, "state": "closed"},
                {"is_project": 1,
                 "text": tools_util.global_processor_top_type_map[tools_util.PROCESSOR_TOP_TYPE.SHARED_PROC],
                 "id": tools_util.PROCESSOR_TOP_TYPE.SHARED_PROC, "state": "closed"},
                {"is_project": 2,
                 "text": tools_util.global_processor_top_type_map[tools_util.PROCESSOR_TOP_TYPE.PUBLIC_PROC],
                 "id": tools_util.PROCESSOR_TOP_TYPE.PUBLIC_PROC, "state": "closed", "is_super": is_admin(user.id)}])

    try:
        if tree_id == -2:
            result = horae_interface.get_shared_processor_tree_async(user.id, tree_id)
        elif tree_id == -3:
            result = horae_interface.get_standard_processor_tree_async(user.id, tree_id)
        else:
            result = horae_interface.get_processor_tree_async(user.id, tree_id)
        return JsonHttpResponse(result)
    except Exception as ex:
        logger.error('get_processor_tree_async  error:<%s>, trace[%s]' % (
            str(ex), traceback.format_exc()))
        return JsonHttpResponse({'status': 1, 'msg': str(ex)})

@login_required(login_url='/login/')
def get_shared_processor_tree_async(request):
    user = request.user

    tree_id = 0
    if "id" in request.GET:
        tree_id = int(request.GET.get("id"))

    if tree_id == 0:
        return JsonHttpResponse([{"is_project": 1, "text": "我创建的公式", "id": -1, "state": "closed"},
                {"is_project": 1, "text": "共享给我的公式", "id": -2, "state": "closed"},
                {"is_project": 1, "text": "标准库公式", "id": -3, "state": "closed"}])

    try:
        result = horae_interface.get_shared_processor_tree_async(user.id, tree_id)
        return JsonHttpResponse(result)
    except Exception as ex:
        logger.error('get_shared_processor_tree_async  error:<%s>' % str(ex))
        return JsonHttpResponse({'status': 1, 'msg': str(ex)})

@login_required(login_url='/login/')
def get_processor(request):
    try:
        user = request.user

        processor_info = get_processor_info(request.POST.get("id"))
        type = processor_info['type']
        private = processor_info['private']
        is_owner = 0
        is_super = is_admin(user)
        owner_name = User.objects.get(id=processor_info['owner_id']).username

        type_str = processor_type(type)
        private_desc = private_str(private)
        if user.id == processor_info['owner_id'] or is_super == 1:
            is_owner = 1

        config = processor_info['config']
        config_str = template_list(config)
        result = {
            "status": 0, "msg": "ok", 'processor': processor_info,
            'type_str': type_str, 'private_str': private_desc,
            'is_owner': is_owner, 'config_str': config_str,
            'input_config': template_list(processor_info['input_config']),
            'output_config': template_list(processor_info['output_config']),
            'is_super': is_super,
            'owner_name': owner_name, 'page_title': '公式详情', 'pipeline_model': 1,
            'page_index': 4
        }
        print(result)
        return HttpResponse(json.dumps(result), content_type='application/json')
    except Exception as ex:
        logger.error('get_processor: <%s>' % str(ex))
        return JsonHttpResponse({'status': 1, 'msg': str(ex)})

def processor_type(type):
    type_str = ''
    if type == TaskForm.TYPE_STREAM_DATA:
        type_str = 'stream'
    if type == TaskForm.TYPE_SCRIPT:
        type_str = 'python'
    if type == TaskForm.TYPE_SPARK:
        type_str = 'spark'
    if type == TaskForm.TYPE_OOZIE:
        type_str = 'oozie'
    if type == TaskForm.TYPE_ODPS:
        type_str = 'odps'
    if type == TaskForm.TYPE_SHELL:
        type_str = 'shell'
    if type == TaskForm.TYPE_DOCKER:
        type_str = 'docker'
    if type == TaskForm.TYPE_CLICKHOUSE:
        type_str = 'clickhouse'
    return type_str

def private_str(private):
    private_str = ''
    if private == 0:
        private_str = '开放'
    elif private == 1:
        private_str = '私有'
    elif private == 2:
        private_str = '公共'
    return private_str

# 浏览历史，画图表
@login_required(login_url='/login/')
@add_visit_record
def view_processor_history(request, proc_id):
    if request.method == 'POST':
        try:
            history_list = get_processor_upload_history(int(proc_id))
        except Exception as ex:
            logger.error('view history fail: <%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': 'error:' + str(ex)})
        return JsonHttpResponse(
            {'status': 0, 'msg': '获取成功', 'history_list': history_list})

@login_required(login_url='/login/')
@add_visit_record
def create_task(request, pipe_id):
    user = request.user

    is_super = is_admin(user)
    if request.method == 'POST':
        form = TaskForm(request.POST)
        processor = None
        if form.is_valid():
            try:
                type = form.cleaned_data['type']
                name = form.cleaned_data['name']
                use_processor = form.cleaned_data['use_processor']
                processor_id = form.cleaned_data['processor_id']
                config = form.cleaned_data['config']
                template = form.cleaned_data['template']
                retry_count = form.cleaned_data['retry_count']
                over_time = form.cleaned_data['over_time']
                priority = form.cleaned_data['priority']
                prev_task_ids = form.cleaned_data['prev_task_ids']
                description = form.cleaned_data['description']
                server_tag = form.cleaned_data['server_tag']
                version_id = request.POST.get('version_id')

                now_time = StaticFunction.get_now_format_time(
                    "%Y-%m-%d %H:%M:%S")
                retry_count = retry_count_num(int(retry_count))

                if is_corrent_config(config) == 1:
                    return JsonHttpResponse(
                        {'status': 1, 'msg': '参数配置key不能为空！'})

                if prev_task_ids == 'null':
                    prev_task_ids = ''

                if prev_task_ids == 'null':
                    prev_task_ids = ''

                task = Task(
                    pl_id=int(pipe_id),
                    pid=int(processor_id),
                    next_task_ids='',
                    prev_task_ids=prev_task_ids,
                    over_time=int(over_time),
                    name=name,
                    config=config,
                    retry_count=retry_count,
                    last_run_time='',
                    priority=int(priority),
                    except_ret=0,
                    server_tag=server_tag_db(server_tag),
                    description=description,
                    version_id=version_id
                )

                if task.pid == -1:
                    processor = Processor(
                        name='proc_for_' + name,
                        type=int(type),
                        template=template,
                        update_time=now_time,
                        config=config,
                        owner_id=user.id,
                        private=1,
                        ap=0,
                    )

                tasks = Task.objects.filter(pl_id=pipe_id, name=name)
                if tasks:
                    raise Exception('任务名重复！')

                result = horae_interface.add_new_task_to_pipeline(user.id, task, processor)
                logger.info(result)
                res = json.loads(result)
                status = res['status']
                if status != 0:
                    return JsonHttpResponse(res)

                msg = res['info']
                task = res['task']
            except Exception as ex:
                logger.error('create task fail: <%s>， traceback[%s]' % (
                    str(ex), traceback.format_exc()))
                return JsonHttpResponse(
                    {'status': 1, 'msg': str(ex)})

            return JsonHttpResponse(
                {'status': status, 'msg': msg, 'task': task})
        else:
            return JsonHttpResponse({'status': 1, 'msg': form.errors.items()})
    else:
        form = TaskForm()
    pipeline = Pipeline.objects.get(id=pipe_id)
    return render(request, 'create_task.html',
                  {'form': form, 'pipe_id': pipe_id, 'is_super': is_super, 'page_title': '创建任务',
                   'pipe_name': pipeline.name, 'pipeline_model': 1, 'page_index': 2})

# 任务配置参数检查
def is_corrent_config(config):
    res = 0
    config_i = ''
    config = config.strip()
    if config:
        config_arr = config.split('\n')
        if len(config_arr) > 1:
            for config_i in config_arr:
                config_i = config_i.strip()
                if config_i.find('=') >= 0:
                    res = 0
                else:
                    res = 1
                    break
        else:
            if config.find('=') >= 0:
                res = 0
            else:
                res = 1
    return res

@login_required(login_url='/login/')
@add_visit_record
def create(request):
    error = []
    user = request.user

    is_super = is_admin(user)
    if request.method == 'POST':
        form = PipelineForm(request.POST)
        if form.is_valid():
            try:
                name = form.cleaned_data['name']
                ct_time = form.cleaned_data['ct_time']
                principal = form.cleaned_data['principal']
                send_mail = form.cleaned_data['send_mail']
                send_sms = form.cleaned_data['send_sms']
                tag = form.cleaned_data['tag']
                project_group = form.cleaned_data['project_id']
                life_cycle = form.cleaned_data['life_cycle']
                description = form.cleaned_data['description']

                check_result = check_life_cycle(life_cycle)
                if not check_result:
                    raise Exception('生命周期设置不能超过6个月！')

                pipelines = Pipeline.objects.filter(name=name)
                if pipelines:
                    raise Exception('指数名重复！')

                if not project_group:
                    project_group = 0

                monitor_way = -1
                if send_mail:
                    if send_sms:
                        monitor_way = 2
                    else:
                        monitor_way = 0
                elif send_sms:
                    monitor_way = 1
                else:
                    monitor_way = -1

                result = horae_interface.create_new_pipeline(name, ct_time,
                                                             user.id, principal, monitor_way, tag, description,
                                                             life_cycle, 0, project_group)
                status, msg = status_msg(result)
                if status != 0:
                    raise Exception(msg)
            except Exception as ex:
                logger.error('create pipeline fail: <%s>, trace:[%s]' % (str(ex), traceback.format_exc()))
                return JsonHttpResponse(
                    {'status': 1, 'msg': str(ex), 'send_mail': send_mail, 'send_sms': send_sms})

            pipeline_id = Pipeline.objects.get(name=name).id
            return JsonHttpResponse(
                {'status': status, 'msg': msg, 'pipeline_id': pipeline_id, 'send_mail': send_mail, 'send_sms': send_sms})
        else:
            return JsonHttpResponse({'status': 1, 'msg': form.errors.items()})
    else:
        form = PipelineForm()
    return render(request, 'create_pipeline.html',
                  {'form': form, 'page_title': '创建指数', 'pipeline_model': 1, 'page_index': 2, 'is_super': is_super})

@login_required(login_url='/login/')
def get_user_list(request):
    if request.method == 'POST':
        user = request.user

        try:
            all_users = horae_interface.get_all_user_info()
            all_users = json.loads(all_users)
            all_users = all_users['users']

            user_list = []
            for user in all_users:
                user_list.append({'id': user['id'], 'name': user['username']})
        except Exception as ex:
            logger.error('get user_list fail: <%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': 'error:' + str(ex)})
        return JsonHttpResponse(
            {'status': 0, 'user_list': user_list, 'msg': '获取成功！'})

@login_required(login_url='/login/')
def get_pipeline_with_project_tree(request):
    user = request.user

    try:
        id = int(request.POST.get("id"))
        result = horae_interface.get_pipeline_with_project_tree(user.id, id)
        return JsonHttpResponse(result)
    except Exception as ex:
        logger.error('get_pipeline_with_project_tree  error:<%s>, trace:%s' % (
            str(ex), traceback.format_exc()))
        return JsonHttpResponse({'status': 1, 'msg': str(ex)})

@login_required(login_url='/login/')
def processor_detail_with_id(request, proc_id):
    return render(request, 'processor_detail.html', {'proc_id': proc_id})

@login_required(login_url='/login/')
def processor_detail(request):
    user = request.user

    is_super = is_admin(user)
    return render(request, 'processor_detail.html',
                  {'processor': {},
                   'type_str': '', 'private_str': '',
                   'is_owner': '', 'config_str': '', 'is_super': is_super,
                   'owner_name': '', 'page_title': '公式详情', 'pipeline_model': 1,
                   'page_index': 4})

@login_required(login_url='/login/')
def search_processor(request):
    user = request.user

    try:
        word = request.POST.get("word")
        with_project = 0
        limit = 100
        if "with_project" in request.POST:
            with_project = int(request.POST.get("with_project"))

        if "limit" in request.POST:
            limit = int(request.POST.get("limit"))

        result = horae_interface.search_processor(user.id, word, with_project, limit)
        return JsonHttpResponse(result)
    except Exception as ex:
        logger.error('search_processor  error:<%s>, trace:%s' % (str(ex), traceback.format_exc()))
        return JsonHttpResponse({'status': 1, 'msg': str(ex)})

# 引用信息
@login_required(login_url='/login/')
def view_quote(request, proc_id):
    if request.method == 'POST':
        try:
            quote_list = get_processor_quote_list(int(proc_id))
        except Exception as ex:
            logger.error('view quote fail: <%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': 'error:' + str(ex)})
        return JsonHttpResponse(
            {'status': 0, 'msg': '获取成功', 'quote_list': quote_list})

# 获取引用
def get_processor_quote_list(proc_id):
    result = horae_interface.get_processor_quote_list(proc_id)
    result = json.loads(result)
    quote_list = result['quote_list']
    return quote_list

# 获取上传包命令
def get_upload_cmd(request):
    if request.method == 'POST':
        proc_name = request.POST.get("proc_name")
        user = request.user

        username = user.username
        password = '<font color=red>${password}</font>';
        cmd = upload_cmd(request, username, password, proc_name)
        return JsonHttpResponse({'cmd': cmd})

@login_required(login_url='/login/')
@add_visit_record
def update_processor(request, processor_id):
    processor = Processor.objects.get(id=processor_id)
    user = request.user

    is_super = is_admin(user)
    if request.method == 'POST':
        form = ProcessorForm(request.POST, instance=processor)
        if form.is_valid():
            try:
                type = form.cleaned_data['type']
                name = form.cleaned_data['name']
                template = form.cleaned_data['template']
                principal = form.cleaned_data['principal']
                input_config = request.POST.get('input_config')
                output_config = request.POST.get('output_config')
                config = form.cleaned_data['config']
                tag = form.cleaned_data['tag']
                description = form.cleaned_data['description']
                now_time = StaticFunction.get_now_format_time(
                    "%Y-%m-%d %H:%M:%S")
                project_id = int(request.POST.get("project_id"))

                processors = Processor.objects.filter(name=name)
                if processors:
                    if not processor in processors:
                        raise Exception('公式名重复！')

                processor.name = name
                processor.type = int(type)
                processor.template = template
                processor.update_time = now_time
                processor.description = description
                processor.config = config
                processor.input_config = input_config
                processor.output_config = output_config
                processor.project_id = project_id
                processor.tag = tag

                result = horae_interface.update_processor(int(user.id), processor, principal)
                status, msg = status_msg(result)
            except Exception as ex:
                logger.error('update processor fail: <%s>' % str(ex))
                return JsonHttpResponse(
                    {'status': 1, 'msg': str(ex)})

            return JsonHttpResponse(
                {'status': status, 'msg': msg})
        else:
            return JsonHttpResponse({'status': 1,
                                     'msg': form.errors.items()})
    else:
        form = ProcessorForm(instance=processor)
        processor_info = get_processor_info(processor_id)
        config = processor_info['config']
        config_str = template_list(config)
        input_config = template_list(processor_info['input_config'])
        output_config = template_list(processor_info['output_config'])

    return render(request, 'update_processor.html',
                  {'form': form, 'processor': processor_info,
                   'config_str': config_str,
                   'input_config': input_config,
                   'output_config': output_config,
                   'page_title': '修改公式', 'pipeline_model': 1,
                   'page_index': 4, 'is_super': is_super
                   })

# 执行状态
@login_required(login_url='/login/')
def history(request):
    user = request.user

    is_super = is_admin(user)
    status = request.GET.get('status')
    runtime = request.GET.get('runtime')

    pl_name = request.GET.get('pl_name')
    task_name = request.GET.get('task_name')
    if status == "success":
        return render(request, 'pipeline_history.html',
                      {'is_super': is_super, 'page_title': '执行状态', 'user': user, 'pipename': '', 'status': '2',
                       'runtime': runtime, 'pipeline_model': 1, 'page_index': 3})
    elif status == "fail":
        return render(request, 'pipeline_history.html',
                      {'is_super': is_super, 'page_title': '执行状态', 'user': user, 'pipename': '', 'status': '3',
                       'runtime': runtime, 'pipeline_model': 1, 'page_index': 3})
    elif pl_name:
        return render(request, 'pipeline_history.html',
                      {'is_super': is_super, 'page_title': '执行状态', 'user': user, 'pipename': pl_name,
                       'taskname': task_name, 'status': '', 'runtime': runtime, 'pipeline_model': 1, 'page_index': 3})
    else:
        return render(request, 'pipeline_history.html',
                      {'is_super': is_super, 'page_title': '执行状态', 'user': user, 'pipename': '', 'status': '',
                       'runtime': '', 'pipeline_model': 1, 'page_index': 3})

# DDE数据
@login_required(login_url='/login/')
def futures_index(request, pipe_id):
    user = request.user
    is_super = is_admin(user)
    return render(request, 'futures_index.html', {'is_super': is_super, 'pipe_id': pipe_id, 'user': user})
    
# DDE数据
@login_required(login_url='/login/')
def futures_datas(request):
    if request.method == 'POST':
        user = request.user
        user = request.user
        is_super = is_admin(user)
        pipe_id = request.POST.get("pipe_id")
        limit = request.POST.get("limit")
        last_timestamp = request.POST.get("last_timestamp")

        cmd = 'SELECT timestamp, index FROM IndexRealTime '
        if last_timestamp is not None:
            cmd += " where timestamp > " + str(last_timestamp)

        cmd += " order by timestamp desc "
        if limit != "":
            cmd += " limit " + limit
        else:
            cmd += " limit 100 "

        try:
            result = ck_client.execute(cmd)
            contracts_dict = dict()
            res_arr = []
            min_val = 999999999;
            max_val = 0
            for item in result:
                exval = float(item[1]) * 100
                if exval > max_val:
                    max_val = exval

                if exval < min_val:
                    min_val = exval

                tm = int(item[0]) + 8 * 3600;
                res_arr.append({ 'y': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(tm)), 'item1': exval })

            return JsonHttpResponse({'status': 0, 'cmd': cmd, 'value': res_arr, 'min': float(min_val), 'max': float(max_val) })
        except Exception as ex:
            logger.error('select fail: <%s, %s>' % (cmd, str(ex)))
            return JsonHttpResponse({'status': 1, 'msg': str(ex)})

    return JsonHttpResponse({'status': 1, 'msg': "not post"})

def search_equal(str):
    if str.startswith('='):
        str = "='" + str[1:] + "'"
    return str

def list_history_condition(request):
    condition = {}

    draw = request.POST.get('draw')
    # 分页
    start = request.POST.get('start')
    length = request.POST.get('length')
    page_min = int(start)
    page_max = int(start) + int(length)

    # 搜索条件
    '''
   start_time = request.POST.get('columns[0][search][value]');             #开始时间
    end_time = request.POST.get('etime')                #结束时间
    pipeline_name = request.POST.get('pipeline_name')   #指数名
    task_name = request.POST.get('task_name')           #任务名
    status = request.POST.get('status')                 #状态
    '''

    run_time = request.POST.get('columns[1][search][value]')
    pl_name = request.POST.get('columns[2][search][value]')
    task_name = request.POST.get('columns[3][search][value]')
    status = request.POST.get('columns[4][search][value]')
    start_time = request.POST.get('columns[5][search][value]')
    if run_time != '':
        run_time.encode()
        condition['run_time'] = search_equal(run_time)
    if pl_name != '':
        pl_name.encode()
        condition['pl_name'] = search_equal(pl_name)
    if task_name != '':
        task_name.encode()
        condition['task_name'] = search_equal(task_name)
    if status != '':
        condition['status'] = status
    if start_time != '':
        start_time.encode()
        condition['start_time'] = search_equal(start_time)
    print(condition)
    return condition, int(draw), page_min, page_max


@login_required(login_url='/login/')
@add_visit_record
def history_list(request, order_info):
    user = request.user

    condition, draw, page_min, page_max = list_history_condition(request)
    order_name = 'start_time'
    order_tag_list = order_info.split('_')
    order_tag = int(order_tag_list[0])
    if order_tag == 0:
        order_name = 'run_time'
    elif order_tag == 1:
        order_name = 'pl_name'
    elif order_tag == 2:
        order_name = 'task_name'
    elif order_tag == 3:
        order_name = 'status'
    else:
        order_name = 'start_time'

    desc = int(order_tag_list[1])
    desc_name = "desc"
    if desc == 0:
        desc_name = "asc"

    pipelines = horae_interface.show_run_history(
        0, user.id, page_min, page_max, order_name, desc_name, condition)

    pipelines = json.loads(pipelines)
    pipe_list = pipelines["runhistory_list"]
    pipe_count = pipelines["count"]

    pipeline_total_count = pipe_count
    pipeline_filter_count = pipeline_total_count

    result = {"draw": draw, "recordsTotal": pipeline_total_count,
              "recordsFiltered": pipeline_filter_count}

    result['data'] = pipe_list

    return HttpResponse(json.dumps(result), content_type='application/json')

@login_required(login_url='/login/')
@add_visit_record
def get_task_information(request, id):
    user = request.user

    is_super = is_admin(user)
    run_log = horae_interface.get_run_history_info(id)
    run_log = json.loads(run_log)
    result = run_log['runhistory']
    return render(request, 'pipeline_log.html',
                  {'is_super': is_super, 'page_title': '查看日志', 'user': user, 'log': result, 'pipeline_model': 1,
                   'page_index': 3})

@login_required(login_url='/login/')
@add_visit_record
def run_tasks(request):
    if request.method == 'POST':
        user = request.user

        task_id_list = request.POST.get("task_id_list")
        run_time = request.POST.get("run_time")
        ordered_num = 0
        if "ordered_num" in request.POST:
            ordered_num = int(request.POST.get("ordered_num"))
        task_id_list = task_id_list.split(',')
        try:
            result = horae_interface.run_tasks(user.id, task_id_list, run_time, ordered_num)
            status, msg = status_msg(result)
        except Exception as ex:
            logger.error('run task error:<%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': str(ex)})
        return JsonHttpResponse(
            {'status': status, 'msg': msg})

@login_required(login_url='/login/')
@add_visit_record
def historypipe(request, pipe_id):
    user = request.user

    is_super = is_admin(user)
    pipeline = Pipeline.objects.get(id=pipe_id)
    pipe_name = '=%s' % pipeline.name
    return render(request, 'pipeline_history.html',
                  {'is_super': is_super, 'page_title': '执行状态',
                   'user': user, 'pipename': pipe_name, 'status': '',
                   'runtime': '', 'pipeline_model': 1, 'page_index': 3})

@login_required(login_url='/login/')
@add_visit_record
def in_historypipe(request, pipe_id):
    user = request.user

    is_super = is_admin(user)
    pipeline = Pipeline.objects.get(id=pipe_id)
    pipe_name = '=%s' % pipeline.name
    return render(request, 'in_pipeline_history.html',
                  {'is_super': is_super, 'page_title': '执行状态',
                   'user': user, 'pipename': pipe_name, 'status': '',
                   'runtime': '', 'pipeline_model': 1, 'page_index': 3})

@login_required(login_url='/login/')
@add_visit_record
def create_processor(request):
    user = request.user

    is_super = is_admin(user)
    error = []
    if request.method == 'POST':
        form = ProcessorForm(request.POST)
        if form.is_valid():
            try:
                type = form.cleaned_data['type']
                name = form.cleaned_data['name']
                principal = form.cleaned_data['principal']
                template = form.cleaned_data['template']
                input_config = request.POST.get('input_config')
                output_config = request.POST.get('output_config')
                config = form.cleaned_data['config']
                tag = form.cleaned_data['tag']
                description = form.cleaned_data['description']
                project_id = int(request.POST.get("project_id"))

                now_time = StaticFunction.get_now_format_time(
                    "%Y-%m-%d %H:%M:%S")

                processor = Processor(
                    name=name,
                    type=int(type),
                    template=template,
                    update_time=now_time,
                    description=description,
                    config=config,
                    owner_id=user.id,
                    private=1,
                    ap=0,
                    tag=tag,
                    tpl_files='',
                    project_id=project_id,
                    input_config=input_config,
                    output_config=output_config
                )

                processors = Processor.objects.filter(name=name)
                if processors:
                    raise Exception('公式名重复！')

                result = horae_interface.create_processor(processor, principal)
                status, msg = status_msg(result)
                if status != 0:
                    raise Exception(msg)

                return JsonHttpResponse({'status': status, 'msg': msg, "pid": processor.id})
            except Exception as ex:
                logger.error('create processor fail: <%s>' % str(ex))
                return JsonHttpResponse(
                    {'status': 1, 'msg': str(ex)})

            return JsonHttpResponse(
                {'status': status, 'msg': msg})
        else:
            return JsonHttpResponse({'status': 1, 'msg': form.errors.items()})
    else:
        form = ProcessorForm()
    return render(request, 'create_processor.html', {'form': form, 'page_title': '创建公式',
                                                     'pipeline_model': 1, 'page_index': 4, 'is_super': is_super})

@login_required(login_url='/login/')
def get_proc_project_tree(request):
    user = request.user

    try:
        type = -1
        if "type" in request.GET:
            type = int(request.GET.get('type'))

        result = horae_interface.get_proc_project_tree(user.id, type)
        return JsonHttpResponse(result)
    except Exception as ex:
        logger.error('get_proc_project_tree  error:<%s>, trace[%s]' % (
            str(ex), traceback.format_exc()))
        return JsonHttpResponse({'status': 1, 'msg': str(ex)})

@login_required(login_url='/login/')
def add_new_project(request):
    if request.method == 'POST':
        user = request.user

        project_name = request.POST.get('project_name')
        description = request.POST.get('description')
        parent_id = 0
        if "parent_id" in request.POST:
            parent_id = int(request.POST.get('parent_id'))

        type = 0
        if "type" in request.POST:
            type = int(request.POST.get('type'))

        try:
            result = horae_interface.add_new_project(user.id, project_name, '', description, parent_id, type)
            res = json.loads(result)
            if int(res["status"]) != 0:
                return JsonHttpResponse({'status': 1, 'msg': res['info']})
            return JsonHttpResponse(res)
        except Exception as ex:
            logger.error('create project failed:<%s>' % str(ex))
            return JsonHttpResponse({'status': 1, 'msg': "创建项目失败"})


@login_required(login_url='/login/')
@add_visit_record
def public_processor(request):
    if request.method == 'POST':
        user = request.user

        proc_id = request.POST.get("id")
        project_id = None
        if "project_id" in request.POST:
            project_id = request.POST.get("project_id")

        try:
            result = horae_interface.public_processor(int(proc_id), project_id)
            status, msg = status_msg(result)

            # 发布公式时，给管理员发旺旺通知
            '''
            stdout, stderr, retcode = \
                    ali_wangwang.WangWang.send_message(
                    'ark_admin',
                    user.username, 
                    '公式发布上线',
                    CURRENT_URL+'processor/%s' % proc_id,
                    '查看')
           '''

        except Exception as ex:
            logger.error('public processor fail: <%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': 'error:' + str(ex)})
        return JsonHttpResponse({'status': status, 'msg': msg})


@login_required(login_url='/login/')
def upload_package_with_local(request):
    if request.method == 'POST':
        user = request.user

        try:
            proc_id = request.POST.get('proc_id')
            type = int(request.POST.get('type'))
            version_name = request.POST.get('version_name')
            desc = request.POST.get('desc')
            url = None
            file_name = None
            if type == 1:  # git
                url = request.POST.get('git_url')
            elif type == 0:  # local file
                file_obj = request.FILES.get('file')
                if not file_obj.name.strip().endswith(".zip"):
                    raise Exception("本地文件上传，必须是zip文件。")

                file_path = os.path.join(local_package_path, str(proc_id))
                if os.path.exists(file_path):
                    shutil.rmtree(file_path)

                os.makedirs(file_path)

                file_name = os.path.join(file_path, file_obj.name)
                f = open(file_name, 'wb')
                for chunk in file_obj.chunks():
                    f.write(chunk)
                f.close()

                no_block_sys_cmd.NoBlockSysCommand().run_once("cd %s && unzip %s" % (file_path, file_obj.name))
                no_block_sys_cmd.NoBlockSysCommand().run_once("cd %s && rm -rf %s" % (file_path, file_obj.name))
                file_path_name = file_obj.name.strip('.zip')
                #file_path = os.path.join(file_path, file_path_name)
                no_block_sys_cmd.NoBlockSysCommand().run_once(
                    "cd %s && tar -zcvf ./%s.tar.gz ./*" % (file_path, proc_id))

                file_name = os.path.join(file_path, "%s.tar.gz" % proc_id)
            elif type == 2:
                # docker
                url = request.POST.get('docker_url')
            else:
                raise Exception("不支持的上传公式方式[%s]" % type)

            res = horae_interface.upload_package_with_local(
                user.id, proc_id, version_name, desc, git_url=url, file_name=file_name, type=type)
            return JsonHttpResponse(res)
        except Exception as ex:
            logger.error('upload_package  error:<%s>, trace:%s' % (str(ex), traceback.format_exc()))
            return JsonHttpResponse({'status': 1, 'msg': str(ex)})

@login_required(login_url='/login/')
def get_params(request):
    if request.method == 'POST':
        user = request.user

        processor_id = request.POST.get("processor_id")
        config_str = ''
        template = ''
        try:
            processor = horae_interface.get_processor_info(int(processor_id))
            processor = json.loads(processor)['processor']
            template = processor['template']
            config = ""
            if processor["input_config"] is not None and processor["input_config"].strip() != "":
                config += processor["input_config"].strip() + "\n"

            if processor["output_config"] is not None and processor["output_config"].strip() != "":
                    config += processor["output_config"].strip() + "\n"

            config += processor['config']
            config_str = template_list(config)
        except Exception as ex:
            logger.error('get param fail: <%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': str(ex)})
        return JsonHttpResponse(
            {'status': 0, 'config': config_str, 'template': template})

@login_required(login_url='/login/')
@add_visit_record
def delete_task(request, pipe_id):
    if request.method == 'POST':
        user = request.user

        task_id = request.POST.get('Task[id]')
        logger.error(task_id)
        try:
            result = horae_interface.delete_task_info(user.id, int(task_id))
            status, msg = status_msg(result)
        except Exception as ex:
            logger.error('delete task fail: <%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': str(ex)})
        return JsonHttpResponse(
            {'status': status, 'msg': msg})

def get_proc_with_project_tree(request):
    user = request.user

    try:
        id = int(request.POST.get("id"))
        result = horae_interface.get_proc_with_project_tree(user.id, id)
        return JsonHttpResponse(result)
    except Exception as ex:
        logger.error('get_proc_with_project_tree  error:<%s>, trace:%s' % (
            str(ex), traceback.format_exc()))
        return JsonHttpResponse({'status': 1, 'msg': str(ex)})

@login_required(login_url='/login/')
def delete_processor(request, proc_id):
    if request.method == 'POST':
        user = request.user

        try:
            result = horae_interface.delete_processor(user.id, int(proc_id))
            status, msg = status_msg(result)
        except Exception as ex:
            logger.error('delete processor fail: <%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': 'error:' + str(ex)})
        return JsonHttpResponse({'status': status, 'msg': msg})

@login_required(login_url='/login/')
def delete_proc_version(request):
    user = request.user

    try:
        proc_id = int(request.POST.get("proc_id"))
        id = int(request.POST.get("id"))
        result = horae_interface.delete_proc_version(user.id, proc_id, id)
        return JsonHttpResponse(result)
    except Exception as ex:
        logger.error('delete_proc_version  error:<%s>, trace:%s' % (str(ex), traceback.format_exc()))
        return JsonHttpResponse({'status': 1, 'msg': str(ex)})

@login_required(login_url='/login/')
def delete_project(request):
    if request.method == 'POST':
        user = request.user

        project_id = request.POST.get('project_id')
        try:
            result = horae_interface.delete_project(user.id, int(project_id))
            res = json.loads(result)
            if int(res["status"]) != 0:
                return JsonHttpResponse({'status': 1, 'msg': res['info']})
            return JsonHttpResponse({'status': 0, 'msg': "OK"})
        except Exception as ex:
            logger.error('create project failed:<%s>' % str(ex))
            return JsonHttpResponse({'status': 1, 'msg': "删除项目失败"})

@login_required(login_url='/login/')
def get_message(request):
    if request.method == 'POST':
        user = request.user

        taskid = request.POST.get('taskid')
        run_time = request.POST.get('run_time')
        try:
            message = horae_interface.get_task_run_status_info(taskid, run_time)
            message = json.loads(message)
            previnfo = message['prev_info']
            info = message['info']
            boolcheck = 0
            if previnfo:
                boolcheck = 1
        except Exception as ex:
            logger.error('get prev_info fail: <%s>' % str(ex))
            return JsonHttpResponse(
                {'prev': "error"})
        return JsonHttpResponse({'prev': previnfo, 'infomessage': info, 'check': boolcheck})

@login_required(login_url='/login/')
def get_status_graph(request):
    user = request.user
    is_super = is_admin(user)
    pl_id = request.GET.get('pl_id')
    run_time = request.GET.get('runtime')
    task_name = request.GET.get('taskname')
    return render(request, 'status_graph.html',
                  {'is_super': is_super, 'page_title': '任务执行状态', 'user': user, 'pipelineid': pl_id,
                   'runtime': run_time, 'task_name': task_name, 'pipeline_model': 1, 'page_index': 3
                   })

@login_required(login_url='/login/')
def get_graph(request):
    if request.method == 'POST':
        user = request.user

        pipe_id = request.POST.get('pipe_id')
        run_time = request.POST.get('run_time')
        try:
            result = horae_interface.run_history_show_graph(pipe_id, run_time)
            history = json.loads(result)
            list = history['runhistory_list']
            for li in list:
                li['id'] = li['task_id']
        except Exception as ex:
            logger.error('get graph  error:<%s>' % str(ex))
            return JsonHttpResponse({'status': 1, 'msg': str(ex)})
        return JsonHttpResponse({'res': list})

@login_required(login_url='/login/')
def get_all_log_list(request):
    if request.method == "POST":
        user = request.user
        schedule_id = request.POST.get('schedule_id')
        subpath = request.POST.get('subpath')
        rerun_id = 0
        if "rerun_id" in request.POST:
            rerun_id = int(request.POST.get('rerun_id'))

        try:
            list = horae_interface.get_task_run_logs(int(schedule_id), subpath, rerun_id)
            list = json.loads(list)
            list_log = list['log_file_list']
            list_log_arr = list_log.split('\n')
            stat = list['status']
            info = list['info']
        except Exception as ex:
            logger.error('get log list error:<%s>, trace: %s' % (
                str(ex), traceback.format_exc()))
            return JsonHttpResponse({'list': "error"})
        return JsonHttpResponse({'list': list_log_arr, 'status': stat, 'info': info})

@login_required(login_url='/login/')
def get_log_content(request):
    if request.method == 'POST':
        user = request.user

        schedule_id = request.POST.get("schedule_id")
        file_name = request.POST.get('file_name')
        rerun_id = 0
        if "rerun_id" in request.POST:
            rerun_id = int(request.POST.get('rerun_id'))

        try:
            log_content = horae_interface.get_task_log_content(
                schedule_id, file_name, 0, 10240, rerun_id)
            return JsonHttpResponse({
                'file_content': log_content, 'status': 0, 'len': len(log_content)})
        except Exception as ex:
            logger.error('get log content fail: <%s> trace<%s>' % (
                str(ex), traceback.format_exc()))
            return JsonHttpResponse(
                {'filecontent': "系统出错：" + str(ex)})

@login_required(login_url='/login/')
def get_tail(request):
    if request.method == 'POST':
        user = request.user

        schedule_id = request.POST.get('schedule_id')
        filename = request.POST.get('file_name')
        rerun_id = 0
        if "rerun_id" in request.POST:
            rerun_id = int(request.POST.get('rerun_id'))

        try:
            result = horae_interface.get_log_content_tail(schedule_id, filename, rerun_id)
            history = json.loads(result)
            list = history['file_content']
        except Exception as ex:
            logger.error('get graph  error:<%s>' % str(ex))
            return JsonHttpResponse({'status': 1, 'msg': str(ex)})
        return JsonHttpResponse({'res': list})

def file_iterator(schedule_id, file_name, chunk_size=512, rerun_id=0):
    try:
        index = 0
        downed_len = 0
        while True:
            log_content = horae_interface.get_task_log_content(
                schedule_id, file_name, downed_len, 1024 * 1024, rerun_id)
            try:
                res = json.loads(log_content)
                if res["status"] != 0:
                    break;
            except:
                pass

            index += 1
            content_len = len(log_content)
            downed_len += content_len

            if content_len <= 0:
                break
            yield log_content

            if index > 1024:
                break
    except Exception as ex:
        yield str(ex)

@login_required(login_url='/login/')
def big_file_download(request, args):
    arg_list = args.split('&')
    schedule_id = int(arg_list[0])
    file_name = arg_list[1]
    rerun_id = int(arg_list[2])
    print(args, ":", schedule_id, ":", file_name, ":", rerun_id)
    down_load_name = file_name.split('/')[-1]

    response = StreamingHttpResponse(file_iterator(schedule_id, file_name, 512, rerun_id))
    response['Content-Type'] = 'application/octet-stream'
    response['Content-Disposition'] = 'attachment;filename="{0}"'.format(down_load_name)
    return response

@login_required(login_url='/login')
def run_one_task(request):
    if request.method == 'POST':
        user = request.user

        task_id_list = request.POST.get("task_id_list")
        run_time = request.POST.get("run_time")
        task_id_list = task_id_list.split(',')
        try:
            result = horae_interface.run_tasks(user.id, task_id_list, run_time)
            result = json.loads(result)
            status = result['status']
            info = result['info']
        except Exception as ex:
            logger.error('run tasks error:<%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': str(ex)})
        return JsonHttpResponse(
            {'status': status, 'msg': info})

@login_required(login_url='/login/')
def run_all_pipeline(request):
    if request.method == 'POST':
        user = request.user

        pl_id_list = request.POST.get("pl_id_list")
        run_time = request.POST.get("run_time")
        try:
            result = horae_interface.run_pipeline(user.id, pl_id_list, run_time)
            result = json.loads(result)
            status = result['status']
            info = result['info']
        except Exception as ex:
            logger.error('run pipeline error:<%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': str(ex)})
        return JsonHttpResponse(
            {'status': status, 'msg': info})

@login_required(login_url='/login/')
def stop_task(request):
    if request.method == 'POST':
        user = request.user

        task_id = request.POST.get("task_id")
        run_time = request.POST.get('run_time')
        try:
            result = horae_interface.stop_task(user.id, task_id, run_time)
            result = json.loads(result)
            status = result['status']
        except Exception as ex:
            logger.error('run some tasks error:<%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': str(ex)})
        return JsonHttpResponse(
            {'status': status, 'msg': result['info']})

@login_required(login_url='/login/')
def run_task_with_all_successors(request):
    if request.method == 'POST':
        user = request.user

        task_id_list = request.POST.get("task_id_list")
        run_time = request.POST.get("run_time")
        try:
            result = horae_interface.run_task_with_all_successors(user.id, task_id_list, run_time)
            result = json.loads(result)
            status = result['status']
            info = result['info']
        except Exception as ex:
            logger.error('run task error:<%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': str(ex)})
        return JsonHttpResponse(
            {'status': status, 'msg': info})

@login_required(login_url='/login/')
def set_task_success(request):
    try:
        task_id = request.POST.get('task_id')
        run_time = request.POST.get('run_time')
        user = request.user

        res = horae_interface.set_task_success(user.id, task_id, run_time)
        return JsonHttpResponse(res)
    except Exception as ex:
        logger.error('set task success fail, %s' % traceback.format_exc())
        return JsonHttpResponse({'status': 1, 'msg': str(ex)})

@login_required(login_url='/login/')
def run_some_task(request):
    if request.method == 'POST':
        user = request.user

        task_id_list = request.POST.get("tasklist")
        run_time = request.POST.get('runtimelist')
        task_id_list = task_id_list.split(',')
        run_time = run_time.split(',')
        task_pair_list = []
        for index in range(len(task_id_list)):
            tub = (task_id_list[index], run_time[index])
            task_pair_list.append(tub)
        try:
            result = horae_interface.run_one_by_one_task(user.id, task_pair_list)
            result = json.loads(result)
            status = result['status']
            info = result['info']
        except Exception as ex:
            logger.error('run some tasks error:<%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': str(ex)})
        return JsonHttpResponse(
            {'status': status, 'msg': info})

@login_required(login_url='/login/')
def get_retry_history_list(request):
    if request.method == 'POST':
        try:
            user = request.user
            schedule_id = request.POST.get("schedule_id")
            res = horae_interface.get_retry_history_list(user.id, schedule_id)
            return JsonHttpResponse(res)
        except Exception as ex:
            logger.error('run some tasks error:<%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': str(ex)})

# 指数上线
@login_required(login_url='/login/')
def on_line(request):
    if request.method == 'POST':
        user = request.user

        pipe_id = request.POST.get("pipe_id")
        on_line = request.POST.get("on_line")
        reason = request.POST.get("reason")
        try:
            result = horae_interface.pipeline_off_or_on_line(
                user.id, int(pipe_id), int(on_line), reason)
            status, msg = status_msg(result)
        except Exception as ex:
            logger.error('change status fail: <%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': 'error:' + str(ex)})
        return JsonHttpResponse({'status': status, 'msg': msg})

# 下次上传前删除upload_files中的文件
def delete_upload_files(proc_id):
    cmd = "rm -rf %s" % (WORK_PATH + proc_id)
    stdout, stderr, retcode = __no_block_cmd.run_once(cmd)
    if retcode != 0:
        return False

# 上传文件
def handle_uploaded_file(file, proc_id, is_package):
    file_name = ""
    try:
        path = UPLOAD_FILES_PATH + str(proc_id) + '/'
        if not os.path.exists(path):
            os.makedirs(path)
        file_name = path + file.name
        destination = open(file_name, 'wb+')
        for chunk in file.chunks():
            destination.write(chunk)
        destination.close()

        # mv package 成 proc_id.tar.gz
        if proc_id and is_package == 1:
            cmd = "mv %s %s.tar.gz" % (WORK_PATH + str(proc_id) + '/' + file.name, \
                                       WORK_PATH + str(proc_id) + '/' + str(proc_id))
            print(cmd)
            stdout, stderr, retcode = __no_block_cmd.run_once(cmd)
            if retcode != 0:
                return False

    except Exception as e:
        print(e)

    return file_name

def upload_processor(request):
    if request.method == 'POST':
        user_name = request.POST.get('user')
        password = request.POST.get('pass')
        proc_name = request.POST.get('proc_name')
        description = request.POST.get('description')
        version_name = request.POST.get('version_name')
        if version_name is None or version_name.strip() == "":
            return JsonHttpResponse({'status': 1, 'msg': '没有填写版本名.'})

        overwrite = True
        # 判断proc_name是否存在
        if proc_name is None or proc_name.strip() == "":
            return JsonHttpResponse({'status': 1, 'msg': '没有填写公式名.'})
        else:
            processor = Processor.objects.filter(name=proc_name)
            if len(processor) == 0:
                return JsonHttpResponse({'status': 1, 'msg': '公式名不存在.'})

        password = hashlib.md5(password.encode(encoding='UTF-8')).hexdigest()
        password = hashlib.md5(password.encode(encoding='UTF-8')).hexdigest()
        password = hashlib.md5(password.encode(encoding='UTF-8')).hexdigest()

        print(password)
        # 判断用户密码是否正确
        user = authenticate(username=user_name, password=password)
        if user is None:
            return JsonHttpResponse(
                {'status': 1, 'msg': 'The username and password were incorrect.'})

        try:
            if 'package' in request.FILES:
                package = request.FILES['package']
                handle_uploaded_file(package, processor[0].id, 1)
            else:
                return JsonHttpResponse(
                    {'status': 1, 'msg': '没有填写package参数!'})

            result = horae_interface.upload_pacakge(
                    processor[0].id,
                    user.id,
                    WORK_PATH + str(processor[0].id) + '/',
                    version_name,
                    description,
                    overwrite)
            status, msg = status_msg(result)
            # 调完接口，删除临时文件
            if status == 0:
                delete_upload_files(str(processor[0].id))
        except Exception as ex:
            logger.error('upload processor package fail:<%s>' % str(ex))
            return JsonHttpResponse(
                {'status': 1, 'msg': 'error:' + str(ex)})
        return JsonHttpResponse({"status": status, "msg": msg})
    else:
        return JsonHttpResponse({"status": 1, "msg": "error"})

@login_required(login_url='/login/')
@add_visit_record
def create_edge(request):
    user = request.user
    is_super = is_admin(user)
    if request.method == 'POST':
        return JsonHttpResponse(
            {'status': 1, 'msg': "invalid post"})
    else:
        prev_task_id = int(request.GET.get('prev_task_id'))
        next_task_id = int(request.GET.get('next_task_id'))
        prev_task = Task.objects.get(id=prev_task_id)
        next_task = Task.objects.get(id=next_task_id)
        try:
            edge = Edge.objects.get(prev_task_id=prev_task_id, next_task_id=next_task_id)
            if edge is not None:
                return render(request, 'update_edge.html',
                    {'prev_task_id': prev_task_id, 'next_task_id': next_task_id, 'page_title': '创建任务',
                    'prev_task_name': prev_task.name, 'next_task_name': next_task.name})
        except Exception as ex:
            logger.info('create new edge.')

        return render(request, 'create_edge.html',
                      {
                       'prev_task_id': prev_task_id,
                       'next_task_id': next_task_id,
                       'page_title': '创建任务',
                       'prev_task_name': prev_task.name,
                       'next_task_name': next_task.name,
                       'def_rcm_context': 'dags_inner_tx_2',
                       'def_rcm_topic': 'dags_inner_topic_2',
                       })

@login_required(login_url='/login/')
@add_visit_record
def update_edge(request):
    print("request method: " + request.method)
    if request.method == 'POST':
        try:
            print(request.POST.get('prev_task_id'))
            print(request.POST.get('next_task_id'))
            print(request.POST.get('dispatch_tag'))
            prev_task_id = int(request.POST.get('prev_task_id'))
            next_task_id = int(request.POST.get('next_task_id'))
            dispatch_tag = int(request.POST.get('dispatch_tag'))
            edge = Edge.objects.get(prev_task_id=prev_task_id, next_task_id=next_task_id)
            edge.dispatch_tag = dispatch_tag
            edge.save()
            return JsonHttpResponse({'status': 0, 'msg': "success"})
        except Exception as ex:
            logger.info('create new edge.')
            return JsonHttpResponse(
                {'status': 1, 'msg': "invalid post" + str(ex)})
    else:
        prev_task_id = int(request.GET.get('prev_task_id'))
        next_task_id = int(request.GET.get('next_task_id'))
        prev_task = Task.objects.get(id=prev_task_id)
        next_task = Task.objects.get(id=next_task_id)
        try:
            edge = Edge.objects.get(prev_task_id=prev_task_id, next_task_id=next_task_id)
            if edge is not None:
                return render(request, 'update_edge.html',
                    {'prev_task_id': prev_task_id, 'next_task_id': next_task_id, 'page_title': '创建任务',
                    'prev_task_name': prev_task.name, 'next_task_name': next_task.name, 'dispatch_tag': edge.dispatch_tag})
        except Exception as ex:
            logger.info('update new edge.')

        return JsonHttpResponse(
            {'status': 1, 'msg': "invalid edge"})
    
# @login_required(login_url='/login/')
# @add_visit_record
# def update_edge(request):
#     user = request.user
#     is_super = is_admin(user)
#     if request.method == 'POST':
#         try:
#             link_from = request.POST.get('prev_task_id')
#             link_to = request.POST.get('next_task_id')
#             edge = Edge.objects.get(prev_task_id=link_from, next_task_id=link_to)
#             changed = False
#             if edge.stream_type != int(request.POST.get('stream_type')):
#                 edge.stream_type = int(request.POST.get('stream_type'))
#                 print(0)
#                 changed = True
            
#             if edge.file_name.strip() != request.POST.get('file_name').strip():
#                 edge.file_name = request.POST.get('file_name').strip()
#                 print(1)
#                 changed = True

#             if edge.rcm_context.strip() != request.POST.get('rcm_context').strip():
#                 edge.rcm_context = request.POST.get('rcm_context').strip()
#                 print(2)
#                 changed = True

#             if edge.rcm_topic.strip() != request.POST.get('rcm_topic').strip():
#                 edge.rcm_topic = request.POST.get('rcm_topic').strip()
#                 print(3)
#                 changed = True

#             rcm_partition = request.POST.get('rcm_partition')
#             dispatch_tag = request.POST.get('dispatch_tag')
#             if rcm_partition == '' or rcm_partition is None:
#                 rcm_partition = 1

#             if dispatch_tag == '' or dispatch_tag is None:
#                 dispatch_tag = -1

#             if edge.rcm_partition != int(rcm_partition):
#                 edge.rcm_partition = int(rcm_partition)
#                 print(4)
#                 changed = True

#             if edge.dispatch_tag != int(dispatch_tag):
#                 edge.dispatch_tag = int(dispatch_tag)
#                 print(5)
#                 changed = True

#             if changed:
#                 t = time.time()
#                 edge.last_update_time_us = int(round(t * 1000000))

#             edge.save()
#             return JsonHttpResponse(
#                 {'status': 0, 'msg': "success"})
#         except Exception as ex:
#             logger.error('add edge fail: <%s>' % str(ex))
#             return JsonHttpResponse(
#                 {'status': 1, 'msg': str(ex)})
#     else:
#         prev_task_id = int(request.GET.get('prev_task_id'))
#         next_task_id = int(request.GET.get('next_task_id'))
#         prev_task = Task.objects.get(id=prev_task_id)
#         next_task = Task.objects.get(id=next_task_id)
#         try:
#             edge = Edge.objects.get(prev_task_id=prev_task_id, next_task_id=next_task_id)
#             return render(request, 'update_edge.html',
#                 {
#                     'prev_task_id': prev_task_id,
#                     'next_task_id': next_task_id,
#                     'page_title': '修改任务流',
#                     'prev_task_name': prev_task.name,
#                     'next_task_name': next_task.name,
#                     'stream_type': edge.stream_type,
#                     'file_name': edge.file_name,
#                     'rcm_context': edge.rcm_context,
#                     'rcm_topic': edge.rcm_topic,
#                     'rcm_partition': edge.rcm_partition,
#                     'dispatch_tag': edge.dispatch_tag,
#                 })
#         except Exception as ex:
#             return JsonHttpResponse({'status': 1, 'msg': '数据流不存在'})

@login_required(login_url='/login/')
def copy_pipeline(request):
    if request.method == 'POST':
        user = request.user

        pl_id = request.POST.get('pl_id')
        pl_name = request.POST.get('pl_name')
        project_id = request.POST.get('project_id')
        try:
            result = horae_interface.copy_pipeline(
                int(user.id),
                int(pl_id),
                pl_name,
                int(project_id))
            res = json.loads(result)
            if int(res["status"]) != 0:
                return JsonHttpResponse({'status': 1, 'msg': res["info"]})
            return JsonHttpResponse({'status': 0, 'msg': "OK", 'pl_id': res["pl_id"]})
        except Exception as ex:
            logger.error('get graph  error:<%s>' % str(ex))
            return JsonHttpResponse({'status': 1, 'msg': str(ex)})

@login_required(login_url='/login/')
def copy_task(request):
    if request.method == 'POST':
        user = request.user

        task_id = request.POST.get('task_id')
        dest_pl_id = request.POST.get('dest_pl_id')
        try:
            result = horae_interface.copy_task(user.id, task_id, dest_pl_id)
            res = json.loads(result)
            if int(res["status"]) != 0:
                return JsonHttpResponse({'status': 1, 'msg': res["info"]})
            return JsonHttpResponse({'status': 0, 'msg': "OK", 'data': res["task"]})
        except Exception as ex:
            logger.error('get graph  error:<%s>' % str(ex))
            return JsonHttpResponse({'status': 1, 'msg': str(ex)})
