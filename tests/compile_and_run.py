import os
import subprocess

if __name__ == '__main__':
    input_path = 'file://' + '/OJ/test_case/0/1.in'
    out_dir = 'file://' + '/OJ/out1'
    main_class = 'com.hadoop.Main'
    jar_name = 'problem' + '0' + '.jar'
    os.chdir('/OJ/problems/0/target')
    run_cmd = 'hadoop jar {jar_path} {main_class} {input_path} {out_path}'
    cmd = run_cmd.format(jar_path=jar_name,
                         main_class=main_class,
                         input_path=input_path,
                         out_path=out_dir)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    if len(out) is not 0:
        print('out=',str(out))
    if len(err) is not 0:
        print('err=',str(err))