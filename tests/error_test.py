
from server4bigdata.exception_bigdata import JudgeRuntimeError,CompileError

if __name__ == '__main__':
    a = JudgeRuntimeError('aaa')
    b = CompileError('bbb')
    print('a =',a.message)
    print('b =',b.message)