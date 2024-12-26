from django.db import models
from django.contrib.auth.models import User
from horae.perm_history_manager import PermHistoryManager

class Pipeline(models.Model):
    name = models.CharField(max_length=128, unique=True)
    owner_id = models.IntegerField(default=0, db_index=True)
    ct_time = models.CharField(max_length=250)
    update_time = models.DateTimeField('date published')
    enable = models.IntegerField(default=0, db_index=True)
    type = models.IntegerField(default=0, db_index=True)
    email_to = models.CharField(max_length=1024, default="", null=True)
    description = models.CharField(max_length=1024, null=True)
    sms_to = models.CharField(max_length=1024, null=True)
    tag = models.CharField(max_length=1024, default="", null=True)
    life_cycle = models.CharField(max_length=50)
    monitor_way = models.IntegerField(default=0)
    private = models.IntegerField(default=0)
    project_id = models.IntegerField(default=0, db_index=True, null=True)

class Processor(models.Model):
    name = models.CharField(max_length=250, unique=True)
    type = models.IntegerField(default=0, db_index=True)
    template = models.TextField(null=True)
    update_time = models.DateTimeField('update_time')
    description = models.CharField(max_length=1024)
    config = models.TextField(null=True)
    owner_id = models.IntegerField(default=0, db_index=True)
    private = models.IntegerField(default=0, db_index=True, null=True)
    ap = models.IntegerField(default=0, db_index=True, null=True)
    tag = models.CharField(max_length=1024, null=True)
    tpl_files = models.CharField(max_length=1024, null=True)
    project_id=models.IntegerField(default=0, db_index=True, null=True)
    input_config = models.TextField(null=True)
    output_config = models.TextField(null=True)

class Task(models.Model):
    pl_id = models.IntegerField(default=0, db_index=True)
    pid = models.IntegerField(default=0, db_index=True)
    next_task_ids = models.CharField(max_length=10240, default=",", null=True)
    prev_task_ids = models.CharField(max_length=1024, default=",", null=True)
    over_time = models.IntegerField(default=0)
    name = models.CharField(max_length=250)
    config = models.TextField(null=True)
    retry_count = models.IntegerField(default=0, null=True)
    last_run_time = models.CharField(max_length=50, null=True)
    priority = models.IntegerField(default=6, null=False)
    except_ret = models.IntegerField(default=0, null=False)
    description = models.CharField(max_length=1024, null=True)
    server_tag = models.CharField(max_length=50, default='ALL')
    version_id = models.IntegerField(default=0, null=False)

class Schedule(models.Model):
    task_id = models.IntegerField(default=0, db_index=True)
    run_time = models.CharField(max_length=20, db_index=True)
    status = models.IntegerField(default=0)
    pl_id = models.IntegerField(default=0, db_index=True)
    end_time = models.DateTimeField(
            'task terminate time',
            null=True,
            default='2000-01-01 00:00:00')
    start_time = models.DateTimeField(
            'task start time',
            default='2000-01-01 00:00:00')
    init_time = models.DateTimeField(
            'task called time',
            null=True,
            default='2000-01-01 00:00:00')
    ret_code = models.IntegerField(default=0, null=True)

    class Meta:
        unique_together = ("task_id", "run_time")

class RunHistory(models.Model):
    task_id = models.IntegerField(default=0, db_index=True)
    run_time = models.CharField(max_length=20, db_index=True)
    pl_id = models.IntegerField(default=0, db_index=True)
    start_time = models.DateTimeField(
            'task start time',
            null=True,
            default='2000-01-01 00:00:00')
    end_time = models.DateTimeField(
            'task terminate time',
            null=True,
            default='2000-01-01 00:00:00')
    status = models.IntegerField(default=0, db_index=True)
    schedule_id = models.IntegerField(default=0, db_index=True)
    tag = models.IntegerField(default=0, null=True)
    type = models.IntegerField(default=0)
    task_handler = models.CharField(max_length=4096, null=True)
    run_server = models.CharField(max_length=20, null=True)
    server_tag = models.CharField(max_length=50, default='ALL')
    pl_name = models.CharField(max_length=1024, null=False)
    task_name = models.CharField(max_length=1024, null=False)
    cpu = models.IntegerField(default=0, null=True)
    mem = models.IntegerField(default=0, null=True)
    ret_code = models.IntegerField(default=0, null=True)

    class Meta:
        unique_together = ("task_id", "run_time")

class RerunHistory(models.Model):
    task_id = models.IntegerField(default=0, db_index=True)
    run_time = models.CharField(max_length=20, db_index=True)
    pl_id = models.IntegerField(default=0, db_index=True)
    start_time = models.DateTimeField(
        'task start time',
        null=True,
        default='2000-01-01 00:00:00')
    end_time = models.DateTimeField(
        'task terminate time',
        null=True,
        default='2000-01-01 00:00:00')
    status = models.IntegerField(default=0, db_index=True)
    schedule_id = models.IntegerField(default=0, db_index=True)
    tag = models.IntegerField(default=0, null=True)
    type = models.IntegerField(default=0)
    task_handler = models.CharField(max_length=4096, null=True)
    run_server = models.CharField(max_length=20, null=True)
    server_tag = models.CharField(max_length=50, default='ALL')
    pl_name = models.CharField(max_length=1024, null=False)
    task_name = models.CharField(max_length=1024, null=False)

class ReadyTask(models.Model):
    pl_id = models.IntegerField(default=0, db_index=True)
    schedule_id = models.IntegerField(default=0, db_index=True)
    status = models.IntegerField(default=0)
    update_time = models.DateTimeField('date published', null=True)
    type = models.IntegerField(default=0)
    init_time = models.DateTimeField('task called time', null=True)
    retry_count = models.IntegerField(default=0, null=True)
    retried_count = models.IntegerField(default=0, null=True)
    run_time = models.CharField(max_length=20, db_index=True)
    server_tag = models.CharField(max_length=50, null=True)
    task_id = models.IntegerField(default=0, db_index=True)
    pid = models.IntegerField(default=0, db_index=True)
    owner_id = models.IntegerField(default=0, db_index=True)
    run_server = models.CharField(max_length=20, default='', null=True)
    task_handler = models.CharField(max_length=4096, default='', null=True)
    is_trigger = models.IntegerField(default=0, null=False)
    next_task_ids = models.CharField(max_length=200, default=",", null=True)
    prev_task_ids = models.CharField(max_length=200, default=",", null=True)
    work_dir = models.CharField(max_length=1024, default='', null=True)
    ret_code = models.IntegerField(default=0, null=True)

    class Meta:
        unique_together = ("task_id", "run_time", "schedule_id", "is_trigger")

class UploadHistory(models.Model):
    processor_id = models.IntegerField(default=0, db_index=True)
    # 算子使用状态： 0没用 1使用中
    status = models.IntegerField(default=0)
    update_time = models.DateTimeField('date published', null=True)
    upload_time = models.DateTimeField('date published', null=True)
    upload_user_id = models.IntegerField(default=0, db_index=True)
    version = models.CharField(
            max_length=250,
            null=False,
            unique=False)
    description = models.CharField(max_length=1024, default='', null=True)
    name = models.CharField(max_length=250, null=False)
    git_url = models.CharField(max_length=1024, null=True)
    type = models.IntegerField(default=-1)

    class Meta:
        unique_together = ("processor_id", "name")


class Project(models.Model):
    name = models.CharField(max_length=20)
    owner_id = models.IntegerField(default=0, db_index=True)
    is_default = models.IntegerField(default=0, null=False)  # 默认个人项目
    description = models.CharField(max_length=10240, default='', null=True)
    parent_id = models.IntegerField(default=0, db_index=True)
    type = models.IntegerField(default=0, db_index=True)  # 0 pipeline的项目管理，1 processor的项目管理
    class Meta:
        unique_together = ("name", "type")

class OrderdSchedule(models.Model):
    task_id = models.IntegerField(default=0, db_index=True)
    run_time = models.CharField(max_length=20, db_index=True)
    status = models.IntegerField(default=0)
    pl_id = models.IntegerField(default=0, db_index=True)
    end_time = models.DateTimeField(
            'task terminate time',
            null=True,
            default='2000-01-01 00:00:00')
    start_time = models.DateTimeField(
            'task start time',
            default='2000-01-01 00:00:00')
    init_time = models.DateTimeField(
            'task called time',
            null=True,
            default='2000-01-01 00:00:00')
    ordered_id = models.CharField(max_length=255, db_index=True)
    run_tag = models.IntegerField(default=0, db_index=True)

    class Meta:
        unique_together = ("task_id", "run_time")

class PermHistory(models.Model):
    resource_type = models.CharField(max_length=16)
    resource_id = models.BigIntegerField()
    permission = models.CharField(
        max_length=200,
        verbose_name='seprated by |, like: read|update')
    applicant = models.ForeignKey(User, related_name='proposer_user', on_delete=models.CASCADE)
    grantor = models.ForeignKey(User, related_name='grantor_user', on_delete=models.CASCADE)

    (PENDING_STATUS, ACCEPTED_STATUS, DENIED_STATUS,
     GRANTED_STATUS, REVOKED_STATUS) = range(5)
    status_choices = (
        (PENDING_STATUS, 'pending'),
        (ACCEPTED_STATUS, 'accepted'),
        (DENIED_STATUS, 'denied'),
        (GRANTED_STATUS, 'unsolicited grant'),
        (REVOKED_STATUS, 'unsolicited revoke'),
    )
    status = models.PositiveSmallIntegerField(
        choices = status_choices,
        default = PENDING_STATUS)
    deadline = models.DateTimeField(null=True, blank = True, default='2000-01-01 00:00:00')
    update_time = models.DateTimeField(auto_now=True)
    create_time = models.DateTimeField(auto_now_add=True)
    reason = models.CharField(max_length=512)

    # 同项目
    PERM_TYPE_INSIDE_PROJECT = 0
    # 跨项目
    PERM_TYPE_CROSS_PROJECT = 1
    odps_perm_type = models.IntegerField(default=-1, null=True)
    objects = PermHistoryManager(
        PENDING_STATUS, ACCEPTED_STATUS, DENIED_STATUS,
        GRANTED_STATUS, REVOKED_STATUS)

class VisitRecord(models.Model):
    app = models.CharField(max_length = 128)
    uri = models.CharField(max_length = 128)
    param = models.CharField(max_length = 128, default = '')
    user = models.ForeignKey(
        User, null = True, related_name = 'user_view_history', on_delete=models.CASCADE)
    description = models.CharField(max_length = 1024, default = '')
    ip = models.CharField(max_length = 64, default = '')
    visit_time = models.DateTimeField(auto_now_add = True)

    @staticmethod
    def add(app, uri, param = '', user = None, description = '', ip = ''):
        VisitRecord.objects.create(
            app = app, uri = uri, param = param,
            user = user, description = description,
            ip = ip)

    @staticmethod
    def request_add(request, param = '', description = ''):
        items = request.path.split('/')
        app = items[1] if len(items) >= 2 \
            else items[0] if len(items) >= 1 else 'none'

        if not request.user or request.user.is_anonymous:
            user = None
        else:
            user = request.user

        VisitRecord.add(app, request.path, param = param,
            user = user, description = description,
            ip = request.META['REMOTE_ADDR'])

    @staticmethod
    def count(conditions = None):
        # conditions: {'user': user, 'uri': 'uri', 'app': 'app'}
        # key可能为user uri app中的0个或多个
        if not conditions:
            conditions = {}
        return VisitRecord.objects.filter(**conditions).count()

class Edge(models.Model):
    prev_task_id = models.IntegerField(default=0, db_index=False)
    next_task_id = models.IntegerField(default=0, db_index=False)
    stream_type = models.IntegerField(default=0, db_index=False)
    file_name = models.CharField(max_length=1024, default='', null=True)
    rcm_context = models.CharField(max_length=256, default='', null=True)
    rcm_topic = models.CharField(max_length=256, default='', null=True)
    rcm_partition = models.IntegerField(default=0, db_index=False)
    dispatch_tag = models.IntegerField(default=-1, db_index=False)
    pipeline_id = models.IntegerField(default=0, db_index=False)
    last_update_time_us = models.BigIntegerField(default=0, null=True)
    class Meta:
        unique_together = ("prev_task_id", "next_task_id")

class UserInfo(models.Model):
    userid = models.IntegerField(default=0, db_index=False)
    email = models.CharField(max_length=1024, default='', null=True)
    dingding = models.CharField(max_length=256, default='', null=True)