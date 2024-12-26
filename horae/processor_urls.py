from django.contrib import admin
from django.urls import path, re_path, include
from horae import views

urlpatterns = (
    re_path(r'^$', views.processor_detail),
    re_path(r'^(?P<proc_id>\d+)/$', views.processor_detail_with_id),
    re_path(r'^index/$', views.processor_detail),
    re_path(r'^create_task_choose_proc/$', views.create_task_choose_proc),
    re_path(r'^get_processor_tree_async/$', views.get_processor_tree_async),
    re_path(r'^get_processor/$', views.get_processor),
    re_path(r'^view_history/(?P<proc_id>\d+)/$', views.view_processor_history),
    re_path(r'^search_processor/$', views.search_processor),
    re_path(r'^view_quote/(?P<proc_id>\d+)/$', views.view_quote),
    re_path(r'^get_upload_cmd/$', views.get_upload_cmd),
    re_path(r'^create/$', views.create_processor),
    re_path(r'^update/(?P<processor_id>\d+)/$', views.update_processor),
    re_path(r'^delete/(?P<proc_id>\d+)/$', views.delete_processor),
    re_path(r'^get_proc_project_tree/$', views.get_proc_project_tree),
    re_path(r'^add_new_project/$', views.add_new_project),
    re_path(r'^delete_project/$', views.delete_project),
    re_path(r'^public_processor/$', views.public_processor),
    re_path(r'^upload_package_with_local/$', views.upload_package_with_local),
    re_path(r'^get_proc_with_project_tree/$', views.get_proc_with_project_tree),
    re_path(r'^delete_proc_version/$', views.delete_proc_version),
    re_path(r'^upload_processor/$', views.upload_processor),

)


