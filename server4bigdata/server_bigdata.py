#coding=utf-8
import hashlib
import json
import os
import sys
import shutil
import uuid
import time
import subprocess
from multiprocessing import Process,Manager,Queue
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.append(BASE_DIR)

from flask import Flask, request, Response

from compiler_bigdata import Compiler
from config_bigdata import (PROJECT_BASE, JUDGER_WORKSPACE_BASE, TEST_CASE_DIR, LOG_BASE, COMPILER_LOG_PATH, JUDGER_RUN_LOG_PATH,
                                   SERVER_LOG_PATH)
from exception_bigdata import TokenVerificationFailed, CompileError, JudgeRuntimeError, JudgeClientError,JudgeServerException,TimeLimitExceeded

from utils_bigdata import server_info, logger, token
from judge_bigdatabk import JudgeBigData,AppInfo

app = Flask(__name__)
# DEBUG = os.environ.get("judger_debug") == "1"
DEBUG = True
app.debug = DEBUG

#初始化环境和创建一些工作目录并修改权限
class InitSubmissionEnv(object):
    #初始化work_dir和test_case_dir
    def __init__(self, judger_workspace, submission_id, init_test_case_dir=False):
        self.work_dir = os.path.join(judger_workspace, submission_id)
        self.init_test_case_dir = init_test_case_dir
        if init_test_case_dir:
            self.test_case_dir = os.path.join(self.work_dir, "submission_" + submission_id)
        else:
            self.test_case_dir = None

    def __enter__(self):
        try:
            os.mkdir(self.work_dir)  #创建工作目录
            if self.init_test_case_dir:
                os.mkdir(self.test_case_dir)  #创建测试样例目录
            # os.chown(self.work_dir, COMPILER_USER_UID, RUN_GROUP_GID)  #修改文件的属主和属组
            os.chmod(self.work_dir, 0o711)   #dir权限711
        except Exception as e:
            logger.exception(e)
            raise JudgeClientError("failed to create runtime dir")
        return self.work_dir, self.test_case_dir

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not DEBUG:
            try:
                shutil.rmtree(self.work_dir) #删除工作目录下的所有文件
            except Exception as e:
                logger.exception(e)
                raise JudgeClientError("failed to clean runtime dir")

class JudgeServer(object):
    cnt :int = -1
    spark_UI_port = 0
    q :Queue = None

    def __init__(self,q):
        JudgeServer.q = q

    def kill_timeout_app(self,appDict):
        cmd = 'yarn application -kill {appId}'
        killed_appIds = []
        for appId,appInfo in appDict.items():
            t_now = time.time()
            t_running = t_now - appInfo.create_time
            if t_running >= appInfo.timeout + 5:
                # print("t_now = %d t_create = %d t_running = %d" % (t_now,appInfo.create_time,t_running))
                try:
                    kill_cmd = cmd.format(appId=appId)
                    p = subprocess.Popen(kill_cmd,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True)
                    out,err = p.communicate(timeout=30)
                    killed_appIds.append(appId)
                except (subprocess.TimeoutExpired) as e:
                    p.kill()
                except Exception as e:
                    logger.exception(e)

        for killed_appId in killed_appIds:
            appDict.pop(killed_appId)

    def deal_appId(self,q):
        appDict = {}
        cnt = 0
        while True:
            # print(appDict)
            self.kill_timeout_app(appDict)
            while q.empty() != True:
                appinfo = q.get(True)
                appDict[appinfo.appId] = appinfo
                cnt += 1
            time.sleep(5)
            # print("cnt = ",cnt)

    def create_dealappID_process(self):
        p = Process(target=self.deal_appId,args=(JudgeServer.q,))
        p.start()

    @classmethod
    def judgebigdata(cls,language_config,max_cpu_time,src,test_case_id=None):
        compile_config = language_config.get("compile")
        run_config = language_config.get("run")
        submission_id = uuid.uuid4().hex

        with InitSubmissionEnv(JUDGER_WORKSPACE_BASE, submission_id=str(submission_id),init_test_case_dir=False) as dirs:
            submission_dir,_ = dirs
            os.chdir(submission_dir)
            problem_dir = os.path.join(PROJECT_BASE, str(test_case_id))
            os.system('cp -r {} ./'.format(problem_dir))

            switch = {
                "hadoop": 'src/main/java/Main.java',
                "spark-Scala": "src/main/java/Main.scala",
                "spark-Java": "src/main/java/Main.java",
                "flink-Scala": "src/main/java/Main.scala",
                "flink-Java": "src/main/java/Main.java",
            }
            src_path = os.path.join(submission_dir, str(test_case_id), switch.get(language_config["name"]))
            old_java = os.path.join(submission_dir, str(test_case_id),"src/main/java/Main.java")
            old_scala = os.path.join(submission_dir, str(test_case_id),"src/main/java/Main.scala")
            if os.path.exists(old_java):
                os.remove(old_java)
            if os.path.exists(old_scala):
                os.remove(old_scala)
            with open(src_path, "w", encoding="utf-8") as f:  # 重写Main.java文件，并把源代码src写入该文件
                f.write(src)

            project_dir = os.path.join(submission_dir,str(test_case_id))
            os.chdir(project_dir)

            compile_log = os.path.join(submission_dir,'compile.log')
            compile_info,err = Compiler().compilebigdata(compile_config,compile_log)
            jar_dir = os.path.join(project_dir,'target','problem.jar')
            if os.path.exists(jar_dir):
                test_case_dir = os.path.join(TEST_CASE_DIR,str(test_case_id))
                judgebigdata = JudgeBigData(run_config=run_config,
                                            problem_id=test_case_id,
                                            submission_dir=submission_dir,
                                            test_case_dir=test_case_dir,
                                            max_cpu_time=max_cpu_time)
                cls.cnt = cls.cnt + 1
                with open(os.path.join(test_case_dir, "info")) as f:
                    test_case_info = json.load(f)
                    test_case_group_num = len(test_case_info['test_cases'])

                cls.spark_UI_port += test_case_group_num
                result = judgebigdata.run(language_config,cls.cnt % 8,cls.q,cls.spark_UI_port)
                return result
            else:
                raise CompileError(compile_info)
        raise JudgeServerException('JudgeServer error')


@app.route('/', defaults={'path': ''})
@app.route('/<path:path>', methods=["POST"])
def server(path):
    if path in ('judge','ping','compile_spj','judgebigdata'):
        _token = request.headers.get('X-Judge-Server-Token')
        # print("client_token=", _token, "server_token=", token)
        try:
            if _token != token:
                raise TokenVerificationFailed("invalid token")
            try:
                data = request.json
            except Exception:
                data = {}
            # print('data =',data)
            ret = {'err':None,'data':getattr(JudgeServer,path)(**data)}
        except (CompileError, TokenVerificationFailed, JudgeRuntimeError, JudgeClientError,TimeLimitExceeded) as e:
            logger.exception(e)
            ret = {'err':e.__class__.__name__,'data':e.message}
        except Exception as e:
            logger.exception(e)
            ret = {'err': 'JudgeClientError','data':e.__class__.__name__ + " :" + str(e)}
    else:
        ret = {'err': 'InvalidRequest','data':'404'}
    print(ret)
    return Response(json.dumps(ret),mimetype='application/json')

# gunicorn -w 4 -b 0.0.0.0:8080 --worker-class eventlet --time 120 server_bigdata:app
if __name__ == '__main__':
    manager = Manager()
    q = manager.Queue()
    js = JudgeServer(q)
    js.create_dealappID_process()
    app.run(debug=False, port=8090,use_reloader=False)
