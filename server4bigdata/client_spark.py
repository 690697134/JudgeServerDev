# coding=utf-8
import hashlib
import json
import os,sys

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.append(BASE_DIR)

import requests
from client.Python import languages


# JudgeServerClientError
class JudgeServerClientError(Exception):
    pass


class JudgeServerClient(object):
    def __init__(self, token, server_base_url):
        self.token = hashlib.sha256(token.encode("utf-8")).hexdigest()
        self.server_base_url = server_base_url.rstrip("/")

    def _request(self, url, data=None):
        kwargs = {'headers': {'X-Judge-Server-Token': self.token,
                              'Content-Type': 'application/json'}}
        if data:
            kwargs['data'] = json.dumps(data)
        try:
            return requests.post(url, **kwargs).json()
        except Exception as e:
            raise JudgeServerClientError(str(e))

    def ping(self):
        return self._request(self.server_base_url + '/ping')

    def judge(self, src, language_config, max_cpu_time, test_case_id=0):
        data = {
            'language_config': language_config,
            'src': src,
            'max_cpu_time': max_cpu_time,
            'test_case_id': test_case_id,
        }
        return self._request(self.server_base_url + '/judgebigdata', data=data)


if __name__ == '__main__':
    token = 'judge_server_token'
    client = JudgeServerClient(token=token, server_base_url="http://127.0.0.1:10010")
    src = r'''import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf :SparkConf = new SparkConf().setAppName("Main")
    conf.setMaster("local[*]")
    val sc :SparkContext = new SparkContext(conf)
    val rdd1 = sc.textFile(args(0),4)
    val rdd2 = rdd1.flatMap(
      line => {
        val str = line.replaceAll("[^a-zA-Z]"," ")
        val splits = str.split("\\s+")
        splits
      }
    ).map(x => (x,1)).reduceByKey(_ + _).sortBy(x => (x._2,x._1),false)
    rdd2.saveAsTextFile(args(1))
    sc.stop()
  }
}
'''

    spark_config = {
        "name": "spark",
        "compile": {
            "compile_command": "mvn clean package -e | tee {compile_log}"
        },
        "run": {
            "command": "bin/spark-submit --class {main_class} --master {master} {jar_path} {input_path} {out_path} | tee {out_log}",
        }
    }
    client.judge(src=src,
                 language_config=spark_config,
                 max_cpu_time=80,
                 test_case_id=2001)
    # client.judge(src,languages.hadoop_config,0)

