import hashlib
import json
import os
import re
import shutil
import subprocess
import time
import signal

from multiprocessing import Pool,Process,Queue,Manager
from threading import Thread
import psutil

from config_bigdata import PROJECT_BASE
from exception_bigdata import JudgeBigDataError,JudgeRuntimeError,TimeLimitExceeded
# from utils_bigdata import logger

WA = 1
AC = 0
TIME_LIMITED = -2
RUNTIME_ERROR = -1


def _run(instance,language_config, test_case_file_id,index,q,cnt):
    # switch = {
    #     "hadoop": instance._judge_one_hadoop(test_case_file_id),
    #     "spark": instance._judge_one_spark(test_case_file_id)
    # }
    if language_config['name'] == 'hadoop':
        return instance._judge_one_hadoop(test_case_file_id,index,q)
    elif language_config['name'] == 'spark-Java' or language_config['name'] ==  "spark-Scala":
        return instance._judge_one_spark(test_case_file_id,index,q,cnt)
    elif language_config['name'] == 'flink-Java' or language_config['name'] == "flink-Scala":
        return instance._judge_one_flink(test_case_file_id,index,q)
    # return switch.get(language_config['name'])

class AppInfo(object):
    def __init__(self,appId,type,create_time,timeout):
        self.appId = appId
        self.type = type
        self.create_time = create_time
        self.timeout = timeout

class JudgeBigData(object):
    def __init__(self,run_config,problem_id,submission_dir,test_case_dir,max_cpu_time):
        self._run_config = run_config
        self._problem_id = problem_id
        self._submission_dir = submission_dir
        self._pool = Pool(processes=psutil.cpu_count())
        self._test_case_dir = test_case_dir
        self._test_case_info = self._load_test_case_info()
        self._max_cpu_time = max_cpu_time
        self._appDict = {}

    def _load_test_case_info(self):
        try:
            with open(os.path.join(self._test_case_dir, "info")) as f:
                return json.load(f)
        except IOError:
            raise JudgeBigDataError("Test case not found")
        except ValueError:
            raise JudgeBigDataError("Bad test case config")

    def _compare(self,test_case_file_id, user_output_dir):
        stripped_output_md5_list = self._test_case_info['test_cases'][test_case_file_id]['stripped_output_md5']

        num_reduce_task = self._test_case_info['partitions']
        for part_id in range(num_reduce_task):
            out_path = os.path.join(user_output_dir, str(part_id))

            if os.path.exists(out_path):
                with open(out_path) as f:
                    content = f.read()
                    item_info = hashlib.md5(content.encode('utf-8').rstrip()).hexdigest()
                    # print("item_info = ",item_info,"md5 = ",stripped_output_md5_list[part_id],"part_id = ",part_id)
                    if item_info != stripped_output_md5_list[part_id]:
                        return WA
            else:
                return RUNTIME_ERROR
        return AC

    def _kill_child_processes(self,parent_pid, sig=signal.SIGKILL):
        # print("timeout kill child_process ",parent_pid)
        if psutil.pid_exists(parent_pid):
            # print("exists pid ", parent_pid)
            p = psutil.Process(int(parent_pid))
        else:
            return
        child_pid = p.children(recursive=True)
        # print("child_pid = ",child_pid)
        for pid in child_pid:
            os.kill(pid.pid, sig)

    def _add_appID(self,out_log, type, create_time, timeout, q):
        # print("ready to add_appID")
        cnt = 0
        t_begin = time.time()
        while (time.time() - t_begin) < timeout:
            time.sleep(5)
            appID: str = "-1"
            with open(out_log, "r", encoding='utf-8') as f:
                for line in f.readlines():
                    word_list: list = line.split(" ")
                    # print(word_list)
                    for id, word in enumerate(word_list):
                        if word == 'application' and id + 1 < len(word_list) and word_list[id+1].startswith('application'):
                            appID = word_list[id + 1]
                            # print("find app id")
                            q.put(AppInfo(appID, type, create_time, timeout))
                            # print("appID = ", appID)
                            return
            cnt += 1
        print("180s passed can't find appId %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")

    def _judge_one_hadoop(self,test_case_file_id,index,q):
        input_path = os.path.join(self._test_case_dir,str(test_case_file_id) + '.in')
        input_path = 'file://' + input_path

        out_dir = os.path.join(self._submission_dir,str(test_case_file_id) + '.out')
        out_dir = 'file://' + out_dir

        main_class = 'Main'
        jar_name = 'problem.jar'

        jar_path = os.path.join(self._submission_dir,str(self._problem_id),'target')

        out_log = os.path.join(self._submission_dir,'out' + str(test_case_file_id) + '.log')

        os.chdir(jar_path)
        cmd = self._run_config['command'].format(jar_path=jar_name,
                                                 main_class=main_class,
                                                 queue_name=str(index),
                                                 input_path=input_path,
                                                 out_path=out_dir,
                                                 out_log=out_log)
        # pro_list = re.split("\\s+",cmd)
        # print(pro_list)
        try:
            t_beginning = time.time()
            p = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True)

            self._add_appID(out_log, 'Hadoop', t_beginning, self._max_cpu_time,q)
            had_run = time.time() - t_beginning
            com_time = self._max_cpu_time - had_run
            if com_time <= 0:
                com_time = 1

            out, err = p.communicate(timeout=com_time)
            code = p.poll()
            # print('code = ',code)
        except (subprocess.TimeoutExpired) as e:
            # print("time limited")
            p.kill()
            return (TIME_LIMITED,e,self._max_cpu_time)
        except Exception as e:
            return (RUNTIME_ERROR,err,0)

        totol_time = time.time() - t_beginning

        num_reduce_task = self._test_case_info['partitions']
        for part_id in range(num_reduce_task):
            part_out_file = os.path.join(self._submission_dir, str(test_case_file_id) + '.out',
                                         "part-r-" + '{:05d}'.format(part_id))
            des = os.path.join(self._submission_dir, str(test_case_file_id) + '.out', str(part_id))
            os.system("mv {src} {des}".format(src=part_out_file, des=des))

        code = self._compare(test_case_file_id,os.path.join(self._submission_dir,str(test_case_file_id) + '.out'))

        if code == RUNTIME_ERROR:
            return (RUNTIME_ERROR,err,0)
        else:
            return (code,None,totol_time)

    def _judge_one_spark(self,test_case_file_id,index,q,ui_cnt):
        input_path = os.path.join(self._test_case_dir,str(test_case_file_id) + '.in')
        input_path = 'file://' + input_path

        out_dir = os.path.join(self._submission_dir,str(test_case_file_id) + '.out')
        out_dir = 'file://' + out_dir

        main_class = 'Main'
        jar_name = 'problem.jar'

        jar_path = os.path.join(self._submission_dir,str(self._problem_id),'target')

        out_log = os.path.join(self._submission_dir,'out' + str(test_case_file_id) + '.log')

        os.chdir(jar_path)
        ui_port = 61000 + (ui_cnt % 500)
        cmd = self._run_config['command'].format(main_class=main_class,
                                                 master='yarn',
                                                 jar_path=jar_name,
                                                 queue_name=str(index),
                                                 ui_port=ui_port,
                                                 input_path=input_path,
                                                 out_path=out_dir,
                                                 out_log=out_log)
        # pro_list = re.split("\\s+",cmd)
        # print("cmd = ",cmd)
        # print(pro_list)
        try:
            t_beginning = time.time()
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 universal_newlines=True)

            self._add_appID(out_log, 'Spark', t_beginning, self._max_cpu_time, q)
            had_run = time.time() - t_beginning
            com_time = self._max_cpu_time - had_run
            if com_time <= 0:
                com_time = 1

            out, err = p.communicate(timeout=com_time)
            code = p.poll()
            # print('code = ',code)
            # print("cost time = ",time.time() - t_beginning)
        except (subprocess.TimeoutExpired) as e:
            # print("time limited")
            p.kill()
            return (TIME_LIMITED, e, self._max_cpu_time)
        except Exception as e:
            # print("other err ",e)
            return (RUNTIME_ERROR, err, 0)

        totol_time = time.time() - t_beginning
        # print("total_time = ",totol_time)

        num_reduce_task = self._test_case_info['partitions']
        for part_id in range(num_reduce_task):
            part_out_file = os.path.join(self._submission_dir, str(test_case_file_id) + '.out', "part-" + '{:05d}'.format(part_id))
            des = os.path.join(self._submission_dir, str(test_case_file_id) + '.out', str(part_id))
            os.system("mv {src} {des}".format(src=part_out_file,des = des))

        code = self._compare(test_case_file_id,os.path.join(self._submission_dir,str(test_case_file_id) + '.out'))

        if code == RUNTIME_ERROR:
            return (RUNTIME_ERROR,err,0)
        else:
            return (code,None,totol_time)

    def _judge_one_flink(self, test_case_file_id,index,q):
        input_path = os.path.join(self._test_case_dir, str(test_case_file_id) + '.in')
        input_path = 'file://' + input_path

        out_dir = os.path.join(self._submission_dir, str(test_case_file_id) + '.out')
        out_dir = 'file://' + out_dir

        main_class = 'Main'
        jar_name = 'problem.jar'

        jar_path = os.path.join(self._submission_dir, str(self._problem_id), 'target')

        out_log = os.path.join(self._submission_dir, 'out' + str(test_case_file_id) + '.log')

        os.chdir(jar_path)
        cmd = self._run_config['command'].format(cluster='yarn-cluster',
                                                 main_class=main_class,
                                                 queue_name=str(index),
                                                 jar_path=jar_name,
                                                 input_path=input_path,
                                                 out_path=out_dir,
                                                 out_log=out_log)
        # pro_list = re.split("\\s+",cmd)
        # print(pro_list)
        # print("jar_path ", jar_path)
        # print("cmd = ", cmd)
        try:
            t_beginning = time.time()
            p = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            # p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

            self._add_appID(out_log, 'Flink', t_beginning, self._max_cpu_time, q)
            had_run = time.time() - t_beginning
            com_time = self._max_cpu_time - had_run
            if com_time <= 0:
                com_time = 1

            out, err = p.communicate(timeout=com_time)
            code = p.poll()

        except (subprocess.TimeoutExpired) as e:
            p.kill()
            return (TIME_LIMITED, e, self._max_cpu_time)
        except Exception as e:
            return (RUNTIME_ERROR, err, 0)

        totol_time = time.time() - t_beginning

        num_reduce_task = self._test_case_info['partitions']

        if num_reduce_task == 1:
            os.system("mv {} {}".format(os.path.join(self._submission_dir, str(test_case_file_id) + '.out'),os.path.join(self._submission_dir,"0")))

            os.system("mkdir {}".format(os.path.join(self._submission_dir, str(test_case_file_id) + '.out')))

            os.system("mv {} {}".format(os.path.join(self._submission_dir,'0'),
                                        os.path.join(self._submission_dir, str(test_case_file_id) + '.out/')))
        else:
            for part_id in range(num_reduce_task):
                part_out_file = os.path.join(self._submission_dir, str(test_case_file_id) + '.out', str(part_id + 1))
                des = os.path.join(self._submission_dir, str(test_case_file_id) + '.out', str(part_id))
                os.system("mv {src} {des}".format(src=part_out_file, des=des))

        code = self._compare(test_case_file_id, os.path.join(self._submission_dir, str(test_case_file_id) + '.out'))


        if code == RUNTIME_ERROR:
            return (RUNTIME_ERROR, err, 0)
        else:
            return (code, None, totol_time)

    def run(self,language_config,index,q,cnt):
        tmp_result = []
        result = []
        for test_case_file_id, _ in self._test_case_info["test_cases"].items():
            tmp_result.append(self._pool.apply_async(_run, (self,language_config,test_case_file_id,index,q,cnt + int(test_case_file_id))))
        self._pool.close()
        self._pool.join()
        for item in tmp_result:
            result.append(item.get())

        max_time = 0
        for tup in result:
            if tup[0] == RUNTIME_ERROR:
                raise JudgeRuntimeError(str(tup[1]))
            elif tup[0] == TIME_LIMITED:
                raise TimeLimitExceeded("Time out in " + str(self._max_cpu_time) + 's')
            elif tup[0] == WA:
                return {'judge_status':'WA','cpu_cost_time':tup[2]}
            else:
                max_time = max(max_time,tup[2])

        return {'judge_status':'AC','cpu_cost_time':max_time}

    def __getstate__(self):
        # http://stackoverflow.com/questions/25382455/python-notimplementederror-pool-objects-cannot-be-passed-between-processes
        self_dict = self.__dict__.copy()
        del self_dict["_pool"]
        return self_dict

