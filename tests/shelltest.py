import os

if __name__ == '__main__':
    os.chdir('/OJ/problems/')
    # code = os.system('cp -r 1004 ../tmp')
    # print(type(code))
    # print(code)
    problem_dir = '/aaa'
    str = 'cp -r {} ./'.format(problem_dir)
    print(str)
