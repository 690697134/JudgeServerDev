import os
import hashlib
import json

class OutInfo(object):
    def __init__(self,problem_dir,num_reduce_task = 1):
        self._problem_dir = problem_dir
        self._num_reduce_task = num_reduce_task
    def generate_info(self):
        info = {'numReduceTask':self._num_reduce_task}
        info['test_cases'] = {}
        case_id = 1
        while True:
            input_path = os.path.join(self._problem_dir,str(case_id) + '.out')
            if os.path.exists(input_path):
                stripped_output_md5_list = []
                for part_id in range(self._num_reduce_task):
                    file_path = os.path.join(input_path,'part-r-' + '{:05d}'.format(part_id))
                    with open(file_path) as f:
                        content = f.read()
                        item_info = hashlib.md5(content.encode('utf-8').rstrip()).hexdigest()
                        stripped_output_md5_list.append(item_info)
                item = {'stripped_output_md5':stripped_output_md5_list}
                info['test_cases'][case_id] = item
            else:
                break
            case_id += 1
        info_path = os.path.join(self._problem_dir,'info')
        with open(info_path,"w", encoding="utf-8") as info_f:
            info_f.write(json.dumps(info))
        return info

if __name__ == '__main__':
    outinfo = OutInfo('/OJ/test_case/0',3)
    info = outinfo.generate_info()
    for test_case_file_id, _ in info["test_cases"].items():
        print(test_case_file_id)