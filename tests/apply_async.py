import hashlib
import json
import os
import shutil
import subprocess

from multiprocessing import Pool
import psutil




def judge_one(test_case_file_id):
    input_path = os.path.join('/OJ/test_case/0', str(test_case_file_id) + '.in')
    input_path = 'file://' + input_path
    out_dir = os.path.join('/OJ/run/001', str(test_case_file_id) + '.out')
    out_dir = 'file://' + out_dir
    main_class = 'com.hadoop.Main'
    jar_name = 'problem' + str(0) + '.jar'
    jar_path = os.path.join('/OJ/problems', str(0), 'target')
    out_log = os.path.join('/OJ/run/001', 'out' + str(test_case_file_id) + '.log')
    os.chdir(jar_path)
    run_cmd = 'hadoop jar {jar_path} {main_class} {input_path} {out_path} | tee {out_log}'
    cmd = run_cmd.format(jar_path=jar_name,
                                             main_class=main_class,
                                             input_path=input_path,
                                             out_path=out_dir,
                                             out_log=out_log)
    print("cmd = ", cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    return err

def hello(str):
    return str

if __name__ == '__main__':
    pool = Pool(processes=psutil.cpu_count())
    tmp_result = []
    result = []
    for test_case_file_id in [1,2]:
        print('test_case_file_id = ', test_case_file_id)
        tmp_result.append(pool.apply_async(judge_one, (test_case_file_id,)))
    pool.close()
    pool.join()
    for item in tmp_result:
        result.append(item.get())
    print(result)
    # temp = []
    # for i in [1,2]:
    #     temp.append(pool.apply_async(hello,(str(i) + 'hello',)))
    # for item in temp:
    #     print(item.get())