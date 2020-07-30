#coding=utf-8
import hashlib
import json
import os
import sys
import shutil
import uuid

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.append(BASE_DIR)

from flask import Flask, request, Response

from server4bigdata.compiler_bigdata import Compiler
from server4bigdata.config_bigdata import (PROJECT_BASE, JUDGER_WORKSPACE_BASE, TEST_CASE_DIR, LOG_BASE, COMPILER_LOG_PATH, JUDGER_RUN_LOG_PATH,
                                   SERVER_LOG_PATH)
from server4bigdata.exception_bigdata import TokenVerificationFailed, CompileError, JudgeRuntimeError, JudgeClientError,JudgeServerException,TimeLimitExceeded

from server4bigdata.utils_bigdata import server_info, logger, token
from server4bigdata.judge_bigdata import JudgeBigData

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

class JudgeServer:

    @classmethod
    def judgebigdata(cls,language_config,max_cpu_time,src,test_case_id=None):
        compile_config = language_config.get("compile")
        run_config = language_config.get("run")
        submission_id = uuid.uuid4().hex

        # src_path = os.path.join(PROJECT_BASE, str(problem_id), 'src/main/java/com/hadoop/Main.java')


        # os.chdir(problem_dir)

        with InitSubmissionEnv(JUDGER_WORKSPACE_BASE, submission_id=str(submission_id),init_test_case_dir=False) as dirs:
            submission_dir,_ = dirs
            os.chdir(submission_dir)
            problem_dir = os.path.join(PROJECT_BASE, str(test_case_id))
            os.system('cp -r {} ./'.format(problem_dir))
            src_path = os.path.join(submission_dir, str(test_case_id), 'src/main/java/com/hadoop/Main.java')
            with open(src_path, "w", encoding="utf-8") as f:  # 重写Main.java文件，并把源代码src写入该文件
                f.write(src)

            project_dir = os.path.join(submission_dir,str(test_case_id))
            os.chdir(project_dir)
            # print('project_dir =',project_dir)
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
                result = judgebigdata.run()
                return result
            else:
                raise CompileError(compile_info)
        raise JudgeServerException('JudgeServer error')


@app.route('/', defaults={'path': ''})
@app.route('/<path:path>', methods=["POST"])
def server(path):
    if path in ('judge','ping','compile_spj','judgebigdata'):
        _token = request.headers.get('X-Judge-Server-Token')
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


if __name__ == '__main__':
    app.run(debug=DEBUG,port=10010)
