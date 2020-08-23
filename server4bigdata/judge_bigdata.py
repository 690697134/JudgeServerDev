import hashlib
import json
import os
import shutil
import subprocess
import time

from multiprocessing import Pool
import psutil

from server4bigdata.config_bigdata import PROJECT_BASE
from server4bigdata.exception_bigdata import JudgeBigDataError,JudgeRuntimeError,TimeLimitExceeded

WA = 1
AC = 0
TIME_LIMITED = -2
RUNTIME_ERROR = -1


def _run(instance,language_config, test_case_file_id):
    # switch = {
    #     "hadoop": instance._judge_one_hadoop(test_case_file_id),
    #     "spark": instance._judge_one_spark(test_case_file_id)
    # }
    if language_config['name'] == 'hadoop':
        return instance._judge_one_hadoop(test_case_file_id)
    elif language_config['name'] == 'spark':
        return instance._judge_one_spark(test_case_file_id)
    # return switch.get(language_config['name'])

class JudgeBigData(object):
    def __init__(self,run_config,problem_id,submission_dir,test_case_dir,max_cpu_time):
        self._run_config = run_config
        self._problem_id = problem_id
        self._submission_dir = submission_dir
        self._pool = Pool(processes=psutil.cpu_count())
        self._test_case_dir = test_case_dir
        self._test_case_info = self._load_test_case_info()
        self._max_cpu_time = max_cpu_time

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

        num_reduce_task = self._test_case_info['numReduceTask']
        for part_id in range(num_reduce_task):
            out_path = os.path.join(user_output_dir, str(part_id))

            if os.path.exists(out_path):
                with open(out_path) as f:
                    content = f.read()
                    item_info = hashlib.md5(content.encode('utf-8').rstrip()).hexdigest()
                    print("item_info = ",item_info,"md5 = ",stripped_output_md5_list[part_id],"part_id = ",part_id)
                    if item_info != stripped_output_md5_list[part_id]:
                        return WA
            else:
                return RUNTIME_ERROR
        return AC

    def _judge_one_hadoop(self,test_case_file_id):
        input_path = os.path.join(self._test_case_dir,str(test_case_file_id) + '.in')
        input_path = 'file://' + input_path

        out_dir = os.path.join(self._submission_dir,str(test_case_file_id) + '.out')
        out_dir = 'file://' + out_dir

        main_class = 'com.hadoop.Main'
        jar_name = 'problem.jar'

        jar_path = os.path.join(self._submission_dir,str(self._problem_id),'target')

        out_log = os.path.join(self._submission_dir,'out' + str(test_case_file_id) + '.log')
        # print('out_log =',out_log)
        os.chdir(jar_path)
        cmd = self._run_config['command'].format(jar_path=jar_name,
                                                 main_class=main_class,
                                                 input_path=input_path,
                                                 out_path=out_dir,
                                                 out_log=out_log)

        try:
            t_beginning = time.time()
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True)
            out, err = p.communicate(timeout=self._max_cpu_time)

        except (subprocess.TimeoutExpired) as e:
            p.terminate()
            return (TIME_LIMITED,e,self._max_cpu_time)
        except Exception as e:
            # print("exception runtime_error")
            return (RUNTIME_ERROR,err,0)


        code = self._compare(test_case_file_id,os.path.join(self._submission_dir,str(test_case_file_id) + '.out'))
        totol_time = time.time() - t_beginning

        if code == RUNTIME_ERROR:
            # print('compare runtime_error')
            return (RUNTIME_ERROR,err,0)
        else:
            return (code,None,totol_time)

    def _judge_one_spark(self,test_case_file_id):
        input_path = os.path.join(self._test_case_dir,str(test_case_file_id) + '.in')
        input_path = 'file://' + input_path

        out_dir = os.path.join(self._submission_dir,str(test_case_file_id) + '.out')
        out_dir = 'file://' + out_dir

        main_class = 'Main'
        jar_name = 'problem.jar'

        jar_path = os.path.join(self._submission_dir,str(self._problem_id),'target',jar_name)

        out_log = os.path.join(self._submission_dir,'out' + str(test_case_file_id) + '.log')

        os.chdir("/home/hadoop/spark-2.4.6-bin-hadoop2.7")
        cmd = self._run_config['command'].format(main_class=main_class,
                                                 master='yarn',
                                                 jar_path=jar_path,
                                                 input_path=input_path,
                                                 out_path=out_dir,
                                                 out_log=out_log)
        try:
            t_beginning = time.time()
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True)
            out, err = p.communicate(timeout=self._max_cpu_time)

        except (subprocess.TimeoutExpired) as e:
            p.terminate()
            return (TIME_LIMITED,e,self._max_cpu_time)
        except Exception as e:
            print("exception runtime_error")
            return (RUNTIME_ERROR,err,0)

        num_reduce_task = self._test_case_info['numReduceTask']
        for part_id in range(num_reduce_task):
            part_out_file = os.path.join(self._submission_dir, str(test_case_file_id) + '.out', "part-" + '{:05d}'.format(part_id))
            des = os.path.join(self._submission_dir, str(test_case_file_id) + '.out', str(part_id))
            os.system("mv {src} {des}".format(src=part_out_file,des = des))

        code = self._compare(test_case_file_id,os.path.join(self._submission_dir,str(test_case_file_id) + '.out'))
        totol_time = time.time() - t_beginning

        if code == RUNTIME_ERROR:
            # print('compare runtime_error')
            return (RUNTIME_ERROR,err,0)
        else:
            return (code,None,totol_time)

    def run(self,language_config):
        tmp_result = []
        result = []
        for test_case_file_id, _ in self._test_case_info["test_cases"].items():
            tmp_result.append(self._pool.apply_async(_run, (self,language_config,test_case_file_id)))
        self._pool.close()
        self._pool.join()
        for item in tmp_result:
            result.append(item.get())

        for tup in result:
            if tup[0] == RUNTIME_ERROR:
                raise JudgeRuntimeError(str(tup[1]))
            elif tup[0] == TIME_LIMITED:
                raise TimeLimitExceeded("Time out in " + str(self._max_cpu_time) + 's')
            elif tup[0] == WA:
                return {'judge_status':'WA','cpu_cost_time':tup[2]}
        return {'judge_status':'AC','cpu_cost_time':tup[2]}

    def __getstate__(self):
        # http://stackoverflow.com/questions/25382455/python-notimplementederror-pool-objects-cannot-be-passed-between-processes
        self_dict = self.__dict__.copy()
        del self_dict["_pool"]
        return self_dict

