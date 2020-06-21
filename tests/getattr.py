class A:
    name = "this is name"
    def get_name(self,str):
        print(self.name,str)

if __name__ == '__main__':
    a = A()
    getattr(a,'name')
    getattr(a,'get_name')('aaa')