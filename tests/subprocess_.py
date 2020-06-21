import subprocess
import os

if __name__ == '__main__':
    workdir = '/home/mrhanice/OJdev/hadooptest/wordcount'
    os.chdir(workdir)
    jar_path = 'word3file.jar'
    main_class = 'com.wordcount.WcDriver'
    input_path = '/in'
    output_path = '/out6'
    cmd = ['hadoop','jar',jar_path,main_class,input_path,output_path]
    p = subprocess.Popen(cmd)

