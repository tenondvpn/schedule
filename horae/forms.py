#coding=utf-8
import re
from django import forms
from django.forms import ModelForm
from horae.models import Pipeline,Processor,Task

class PipelineForm(ModelForm):
    error_messages = {
        'duplicate_path': '此路径已存在！',
        'invalid': '流程名称为字母、数字、下划线！',
    }

    name = forms.CharField(
            label='流程名称',
            widget = forms.TextInput(attrs={'class':'form-control',
                'placeholder':'输入任务名称，格式依照“行业类目_项目_功能”'
                }),
            error_messages={
                'required':'流程名称不能为空！',
                'unique':'流程名称重复！',
                'invalid': '流程名称为字母、数字、下划线！',}
            )

    ct_time = forms.CharField(
            label='调度时间',
            widget= forms.TextInput(attrs={'class':'form-control',
                'placeholder':'crontab格式，如:10 * * * * 表示每小时10分运行一次'
                }),
            required = False)
    
    principal = forms.CharField(
            label='负责人',
            widget = forms.TextInput(attrs={'class':'form-control',
                'type':'hidden'}),
            required = False)

    tag = forms.CharField(
            label='应用分组',
            widget = forms.TextInput(attrs={'class':'form-control',
                'type':'hidden'}),
            required = False)

    project_id = forms.IntegerField(
            label='所属项目',
            widget = forms.TextInput(attrs={'class':'form-control',
                'type':'hidden'}),
            required = False)

    life_cycle = forms.CharField(
            label='有效期至',
            widget = forms.TextInput(attrs={'class':'form-control',
                'placeholder':'设置生命周期，会自动删除流程，最大生命周期为6个月'
                }),
            error_messages={
                'required':'生命周期不能为空！'}
            )

    description = forms.CharField(
            label='描述',
            widget = forms.Textarea(attrs={'class':'form-control',
                'cols': 80, 'rows': 5,
                'placeholder':
                '请详细填写流程的功能描述信息'
                }),
            required=False) 

    send_mail = forms.BooleanField(label='邮件',required=False,
            initial=True)
    send_sms = forms.BooleanField(label='钉钉',required=False) 

    class Meta: 
        model = Pipeline
        fields = ('name' , 'ct_time','principal','tag','description',
                'send_mail','send_sms','life_cycle')


class ProcessorForm(ModelForm):
    error_messages = {
        'invalid': '流程名称为字母、数字、下划线！',
    }

    TYPE_STREAM_DATA = -1
    TYPE_SCRIPT = 1
    TYPE_SPARK = 2
    TYPE_OOZIE = 3
    TYPE_ODPS = 4
    TYPE_SHELL = 5
    TYPE_DOCKER = 6
    TYPE_CLICKHOUSE = 7
    TYPE_V100 = 8
    type_choices = (
        (TYPE_STREAM_DATA, 'stream_data'),
        (TYPE_SCRIPT, 'python'),
        (TYPE_SPARK, 'spark'),
        (TYPE_OOZIE, 'oozie'),
        (TYPE_ODPS, 'odps'),
        (TYPE_SHELL, 'shell'),
        (TYPE_DOCKER, 'docker'),
        (TYPE_CLICKHOUSE, 'clickhouse'),
        (TYPE_V100, 'v100')
    )
    
    type = forms.ChoiceField(label='算子类型',
            choices = type_choices,
            widget= forms.Select(attrs={'class':'form-control',
                'style':'width:100px',
                }),
            initial=TYPE_SCRIPT
            )

    name = forms.CharField(
            label='算子名称',
            widget = forms.TextInput(attrs={'class':'form-control',
                'placeholder':'请使用字母、数字、下划线',
                'style':'width:303px'
                }),
            error_messages={
                'required':'算子名称不能为空！',
                'unique':'算子名称重复！',
                'invalid': '算子名称为字母、数字、下划线！',}
            )
    principal = forms.CharField(
            label='负责人',
            widget = forms.TextInput(attrs={'class':'form-control',
                'type':'hidden'}),
            required = False)

    config = forms.CharField(
            label='参数配置',
            widget = forms.TextInput(attrs={'class':'form-control',
                'type':'hidden'}),
            error_messages={
                'required':'参数模板不能为空！',}
            )

    template = forms.CharField(
            label='执行脚本',
            widget = forms.Textarea(attrs={'class':'form-control',
                'cols': 80, 'rows': 5,
                'placeholder':
                '执行脚本模板',
                }),
            required=False) 

    description = forms.CharField(
            label='描述',
            widget = forms.Textarea(attrs={'class':'form-control',
                'cols': 80, 'rows': 5,
                'style':'margin-left:14px',
                'placeholder':
                '请详细填写算子的功能描述信息'
                }),
            required=False) 
    
    tag = forms.CharField(
            label='标签',
            widget = forms.TextInput(attrs={'class':'form-control',
                'type':'hidden'}),
            required = False)


    class Meta: 
        model = Processor
        fields = ('name','type','template','config','description','tag')

class TaskForm(ModelForm):
    error_messages = {
        'invalid': '任务名称为字母、数字、下划线！',
    }

    TYPE_STREAM_DATA = -1
    TYPE_SCRIPT = 1
    TYPE_SPARK = 2
    TYPE_OOZIE = 3
    TYPE_ODPS = 4
    TYPE_SHELL = 5
    TYPE_DOCKER = 6
    TYPE_CLICKHOUSE = 7
    TYPE_V100 = 8
    type_choices = (
        (TYPE_STREAM_DATA, 'stream_data'),
        (TYPE_SCRIPT, 'python'),
        (TYPE_SPARK, 'spark'),
        (TYPE_OOZIE, 'oozie'),
        (TYPE_ODPS, 'odps'),
        (TYPE_SHELL, 'shell'),
        (TYPE_DOCKER, 'docker'),
        (TYPE_CLICKHOUSE, 'clickhouse'),
        (TYPE_V100, 'v100')
    )

    LEVEL_ONE = 5
    LEVEL_TWO = 6
    LEVEL_THREE = 7
    LEVEL_FOUR = 8
    LEVEL_FIVE = 9
    LEVEL_SIX = 10

    priority_choice = (
        (LEVEL_TWO, 'P4'),
        (LEVEL_THREE, 'P3'),
        (LEVEL_FOUR, 'P2'),
        (LEVEL_FIVE,'P1'),
        (LEVEL_SIX,'P0')
    )

    NOT_RETRY = 1
    ONE_TIME = 2
    FIVE_TIME = 3
    ALWAYS = 4
    retry_choice = (
        (NOT_RETRY, '不重试'),
        (ONE_TIME, '1次'),
        (FIVE_TIME, '5次'),
        (ALWAYS, '一直'),
    )
    
    type = forms.ChoiceField(label='任务类型',
            choices = type_choices,
            widget= forms.Select(attrs={'class':'form-control',
                'style':'width:129px',
                }),
            initial=TYPE_SCRIPT,required=False)

    use_processor = forms.BooleanField(label='选择算子',required=False,
            initial=False)

    name = forms.CharField(
            label='任务名称',
            widget = forms.TextInput(attrs={'class':'form-control',
                'placeholder':'请输入任务名称',
                'style':'width:474px',
                }),
            error_messages={
                'required':'任务名称不能为空！',
                'unique':'任务名称重复！',
                'invalid': '任务名称为字母、数字、下划线！',}
            )
    

    config = forms.CharField(
            label='任务参数',
            widget = forms.TextInput(attrs={'class':'form-control',
                'type':'hidden'}),
            required=False)

    template = forms.CharField(
            label='执行脚本',
            widget = forms.Textarea(attrs={'class':'form-control',
                'cols': 80, 'rows': 5,
                'placeholder':
                '执行脚本模板',
                'style':'width:720px',
                }),
            required=False) 

    processor_id = forms.CharField(
            label='算子id',
            widget = forms.TextInput(attrs={'class':'form-control',
                'type':'hidden'}),
            required=False)

    prev_task_ids = forms.CharField(
            label='依赖ids',
            widget = forms.TextInput(attrs={'class':'form-control',
                'type':'hidden'}),
            required=False)

    retry_count = forms.ChoiceField(label='失败重复次数',
            choices = retry_choice,
            widget= forms.Select(attrs={'class':'form-control',
                'style':'width:150px',
                }),
            initial=NOT_RETRY
            ) 
    
    over_time = forms.IntegerField(
            label='超时报警时间',
            widget = forms.TextInput(attrs={'class':'form-control',
                'style':'width:155px',
                'placeholder':'单位：秒'
                }),
            required=False)


    description = forms.CharField(
            label='描述',
            widget = forms.Textarea(attrs={'class':'form-control',
                'cols': 80, 'rows': 5,
                'placeholder':'请说明任务的基本功能及用途',
                'style':'width:720px; margin-left:15px;',
                }),
            required=False) 

    priority = forms.ChoiceField(label='优先级',
            choices = priority_choice,
            widget= forms.Select(attrs={'class':'form-control',
                'style':'width:150px',
                }),
            initial=LEVEL_ONE
            ) 

    server_tag = forms.CharField(
            label='机器标签',
            widget = forms.TextInput(attrs={'class':'form-control',
                'type':'hidden'}),
            required=False)

    class Meta: 
        model = Task
        fields = ('name','type','config','template','processor_id',\
                'prev_task_ids','description','retry_count','over_time',\
                'priority','server_tag')

#upload file
class DocumentForm(forms.Form):
    docfile = forms.FileField(
            label = 'Select a file'
            )










