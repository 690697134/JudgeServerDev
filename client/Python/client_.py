import hashlib
import json
import os
import requests
from client.Python import languages


#JudgeServerClientError
class JudgeServerClientError(Exception):
    pass

#模拟的JudgeServer的客户端
class JudgeServerClient(object):
    def __init__(self, token, server_base_url):
        self.token = hashlib.sha256(token.encode("utf-8")).hexdigest()
        self.server_base_url = server_base_url.rstrip("/")

    #发送post请求
    def _request(self, url, data=None):
        kwargs = {"headers": {"X-Judge-Server-Token": self.token,
                              "Content-Type": "application/json"}}
        if data:
            kwargs["data"] = json.dumps(data)
        try:
            return requests.post(url, **kwargs).json()
        except Exception as e:
            raise JudgeServerClientError(str(e))

    def ping(self):
        return self._request(self.server_base_url + "/ping")

    # print(client.judge(src=cpp_src, language_config=cpp_lang_config,
    #                    max_cpu_time=1000, max_memory=1024 * 1024 * 128,
    #                    test_case_id="normal"), "\n\n")

    #模拟客户端提交Judge
    def judge(self, src, language_config, max_cpu_time, max_memory, test_case_id=None, test_case=None, spj_version=None, spj_config=None,
              spj_compile_config=None, spj_src=None, output=False):
        if not (test_case or test_case_id) or (test_case and test_case_id):
            raise ValueError("invalid parameter")

        data = {"language_config": language_config,
                "src": src,
                "max_cpu_time": max_cpu_time,
                "max_memory": max_memory,
                "test_case_id": test_case_id,
                "test_case": test_case,
                "spj_version": spj_version,
                "spj_config": spj_config,
                "spj_compile_config": spj_compile_config,
                "spj_src": spj_src,
                "output": output}
        return self._request(self.server_base_url + "/judge", data=data)

    #模拟客户端提交spj_Judge
    def compile_spj(self, src, spj_version, spj_compile_config):
        data = {"src": src, "spj_version": spj_version,
                "spj_compile_config": spj_compile_config}
        return self._request(self.server_base_url + "/compile_spj", data=data)


if __name__ == "__main__":
    token = "mrhanice"


    java_src = r"""
    import java.util.Scanner;
    public class Main{
        public static void main(String[] args){
            Scanner in=new Scanner(System.in);
            int a=in.nextInt();
            int b=in.nextInt();
            System.out.println(a + b);
        }
    }
    """


    client = JudgeServerClient(token=token, server_base_url="http://127.0.0.1:5000")
    print("ping")
    print(client.ping(), "\n\n")


    test_case_path = os.fspath("/home/mrhanice/OJdev/JudgeServer/tests/test_case/normal")
    print("java_judge")
    print(client.judge(src=java_src, language_config=languages.java_lang_config,
                       max_cpu_time=1000, max_memory=256 * 1024 * 1024,
                       test_case_id='normal')
          ,"\n\n")

