###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import os
import shutil
import sys

import no_block_sys_cmd
import task_util

class LinuxFileCommand(object):
    """
        linux文件相关操作命令
    """
    def exist_dir(self, dir_path, run_once=True):
        return os.path.exists(dir_path)

    def exist_file(self, file_path, run_once=True):
        return os.path.exists(file_path)

    def read_file(self, file_path, run_once=False):
        ret, content = task_util.StaticFunction.get_all_content_from_file(
                file_path)
        if ret != task_util.FileCommandRet.FILE_COMMAND_SUCC:
            return None
        return content

    def mk_dir(self, file_dir, run_once=False):
        try:
            os.mkdir(file_dir)
            return True
        except:
            return False

    def mv_file(self, src_file, dest_file):
        try:
            shutil.move(src_file, dest_file)
            return True
        except:
            return False

    def mv_dir(self, src_dir, dest_dir):
        return self.mv_file(src_dir, dest_dir)

    def cp_file(self, src_file, dest_file, run_once=True):
        try:
            shutil.copy(src_file, dest_file)
            return True
        except:
            return False

    def cp_dir(self, src_dir, dest_dir):
        return self.cp_file(src_dir, dest_dir)

    def rm_file(self, file_name, run_once=False):
        try:
            shutil.rmtree(file_name)
            return True
        except:
            return False

    def rm_dir(self, dir_name, run_once=False):
        return self.rm_file(dir_name)

    def ls_dir(self, dir_name):
        if not self.exist_dir(dir_name):
            return None
        file_list = []
        path_list = os.listdir(dir_name)
        for path in path_list:
            tmp_dir = os.path.join(dir_name, path)
            if os.path.isdir(tmp_dir) and not path.endswith("/"):
                path = path + "/"
            file_list.append(path)
        return '\n'.join(file_list)

    # 获取脚本文件的当前路径
    def cur_file_dir(self):
        path = sys.path[0]
        if os.path.isdir(path):
            return path
        elif os.path.isfile(path):
            return os.path.dirname(path)
        return None

    def file_tail(self, file_path):
        cmd = "tail %s" % file_path
        stdout, stderr, retcode = no_block_sys_cmd.NoBlockSysCommand(
                ).run_once(cmd)
        if retcode != 0:
            return None
        return stdout

if __name__ == '__main__':
    linux_cmd = LinuxFileCommand()
    print(linux_cmd.file_tail('./linux_file_cmd.py'))

    