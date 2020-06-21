import hashlib
import json
import os
import requests
import _judger
import subprocess

from client.Python.languages import  c_lang_config, cpp_lang_config, java_lang_config, c_lang_spj_config, \
    c_lang_spj_compile, py2_lang_config, py3_lang_config,hadoop_config

UNLIMITED = -1
VERSION = 0x020101

#各种执行结果返回码
RESULT_SUCCESS = 0
RESULT_WRONG_ANSWER = -1
RESULT_CPU_TIME_LIMIT_EXCEEDED = 1
RESULT_REAL_TIME_LIMIT_EXCEEDED = 2
RESULT_MEMORY_LIMIT_EXCEEDED = 3
RESULT_RUNTIME_ERROR = 4
RESULT_SYSTEM_ERROR = 5

#各种错误状态码
ERROR_INVALID_CONFIG = -1
ERROR_FORK_FAILED = -2
ERROR_PTHREAD_FAILED = -3
ERROR_WAIT_FAILED = -4
ERROR_ROOT_REQUIRED = -5
ERROR_LOAD_SECCOMP_FAILED = -6
ERROR_SETRLIMIT_FAILED = -7
ERROR_DUP2_FAILED = -8
ERROR_SETUID_FAILED = -9
ERROR_EXECVE_FAILED = -10
ERROR_SPJ_ERROR = -11

def run(workdir,jar_path,main_class,input_path,output_path,):
    os.chdir(workdir)
    cmd = ['hadoop', 'jar', jar_path, main_class, input_path, output_path]
    p = subprocess.Popen(cmd,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out,err = p.communicate()
    return out


if __name__ == "__main__":
    token = "mrhanice"

    language_config = hadoop_config
    compile_config = hadoop_config["compile"]
    run_config = hadoop_config['run']
    _command = run_config['command']
    workdir = '/home/mrhanice/OJdev/hadooptest/wordcount'
    jar_path = 'word3file.jar'
    main_class = 'com.wordcount.WcDriver'
    input_path = '/in'
    output_path = '/out6'
    result = run(workdir,jar_path,main_class,input_path,output_path)
    print(result)