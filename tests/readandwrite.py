import os
import re
import time
import subprocess

def add_appID(out_log,type,create_time,timeout):
    # begin_time = time.time()
    time.sleep(2)
    # print(time.time() - begin_time)
    appID :str = "-1"
    with open(out_log,"r",encoding='utf-8') as f:
        for line in f.readlines():
            word_list :list = line.split(" ")
            for id,word in enumerate(word_list):
                if word == 'application':
                    appID = word_list[id + 1]
                    break
            if appID != '-1':
                break
    print("appId = ",appID)

if __name__ == '__main__':
    cmd = "hadoop jar {jar_path} {main_class} -Dmapred.job.queue.name={queue_name} {input_path} {out_path} >> {out_log} 2>&1"
    test_case_file_id = 1
    test_case_dir = '/OJ/test_case/1001'
    input_path = os.path.join(test_case_dir, str(test_case_file_id) + '.in')
    input_path = 'file://' + input_path

    submission_dir = '/OJ/dev/1001'
    out_dir = os.path.join(submission_dir, str(test_case_file_id) + '.out')
    out_dir = 'file://' + out_dir

    main_class = 'Main'
    jar_name = 'problem.jar'

    # jar_path = os.path.join(self._submission_dir, str(self._problem_id), 'target')

    out_log = os.path.join(submission_dir, 'out' + str(test_case_file_id) + '.log')

    os.chdir(submission_dir)
    cmd = cmd.format(jar_path=jar_name,
                                             main_class=main_class,
                                             queue_name=str(0),
                                             input_path=input_path,
                                             out_path=out_dir,
                                             out_log=out_log)
    pro_list = re.split("\\s+", cmd)
    print(pro_list)
    try:
        t_beginning = time.time()
        p = subprocess.Popen(cmd,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        print("pid = ", p.pid)
        add_appID(out_log,type='hadoop',create_time = t_beginning,timeout=300)
        out, err = p.communicate(timeout=300)
    except (subprocess.TimeoutExpired) as e:
        pass

    os.system("rm -rf /OJ/dev/1001/1.out /OJ/dev/1001/out1.log")
