###############################################################################
# coding: utf-8
#
###############################################################################
"""

Authors: xielei
"""

import subprocess

import pexpect

import task_util

class RemoteSysCommand(object):
    def remove_remote_dir(self, ip, dir):
        cmd = ' '.join(['ssh', ip, '"' + 'if [ -d', dir + ' ]',
                ';then rm -rf', dir, ';fi' + '"'])
        ret = self.execute_cmd(cmd)
        if ret != '' and ret != []:
            return False
        return True

    def execute_cmd_with_password(self, cmd, os_passwd):
        foo = pexpect.spawn(cmd)
        while not task_util.CONSTANTS.GLOBAL_STOP:
            foo.before = None
            index = foo.expect([
                    '.*password.*', 
                    '.*Password.*', 
                    pexpect.EOF, 
                    pexpect.TIMEOUT])
            if index in [0, 1]:
                foo.sendline(os_passwd)
            elif index == 2:
                result = foo.before
                foo.close()
                exit_status = foo.exitstatus
                return result, exit_status
            elif index == 3:
                continue
        return None, -1

    def execute_cmd(self, cmd):
        sub_proc_out = subprocess.Popen(
                cmd, 
                shell=True, 
                stdout=subprocess.PIPE).stdout
        return sub_proc_out.readlines()
