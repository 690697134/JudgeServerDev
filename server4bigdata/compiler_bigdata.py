#coding=utf-8
import subprocess

#编译输入程序，返回生成的可执行文件路径exe_path
class Compiler(object):
    def compilebigdata(self,compile_config,compile_log):
        cmd = compile_config.get("compile_command")
        cmd = cmd.format(compile_log=compile_log)
        # print('cmd = ',cmd)
        p = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True)
        out,err = p.communicate()
        return out,err


