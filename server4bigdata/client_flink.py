# coding=utf-8
import hashlib
import json
import os,sys

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.append(BASE_DIR)

import requests

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
    token = 'OJ4BigData'
    client = JudgeServerClient(token=token, server_base_url="http://127.0.0.1:8090")
#     src = r'''import org.apache.flink.api.common.operators.Order
# import org.apache.flink.api.scala._
# import org.apache.flink.core.fs.FileSystem.WriteMode
#
# object Main {
#   def main(args: Array[String]): Unit = {
#     val env = ExecutionEnvironment.getExecutionEnvironment
#     //    val inputPath = "./input"
#     env.setParallelism(1)
#     val inputDS: DataSet[String] = env.readTextFile(args(0))
#     val wordCountDS: AggregateDataSet[(String, Int)] = inputDS.flatMap(line => {
#       val str = line.replaceAll("[^a-zA-Z]"," ")
#       val splits = str.split("\\s+")
#       splits
#     })
#       .map((_, 1))
#       .groupBy(0)
#       .sum(1)
#     wordCountDS.sortPartition(x => (x._2,x._1),Order.DESCENDING).writeAsText(args(1),WriteMode.OVERWRITE)
#     env.execute()
#   }
# }
# '''
#
#     _flink_config_Scala = {
#         "name": "flink-Scala",
#         "compile": {
#             "compile_command": "mvn clean package -e | tee {compile_log}"
#         },
#         "run": {
#             "command": "/usr/local/flink-1.10.2/bin/flink run -c {main_class}  {jar_path} {input_path} {out_path} | tee {out_log}",
#         }
#     }
#     client.judge(src=src,
#                  language_config=_flink_config_Scala,
#                  max_cpu_time=80,
#                  test_case_id=3001)
    # client.judge(src,languages.hadoop_config,0)

    src = r'''import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

class ComKey implements Comparable<ComKey> {
    public Integer cnt;
    public String word;
    public ComKey() {}
    public ComKey(Integer cnt,String word) {
        this.cnt = cnt;
        this.word = word;
    }

    @Override
    public int compareTo(ComKey comKey) {
        return this.cnt == comKey.cnt ? comKey.word.compareTo(this.word) : comKey.cnt - this.cnt;
    }
}

public class Main {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataSet<String> input = env.readTextFile(args[0]);
        DataSet<Tuple2<String,Integer>> ds = input.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String str = value.replaceAll("[^a-zA-Z]"," ");
                String[] splits = str.split("\\s+");
                for (String word : splits) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }).groupBy(0).sum(1);
        DataSet<Tuple2<ComKey,Integer>> temp = ds.mapPartition(new MapPartitionFunction<Tuple2<String, Integer>, Tuple2<ComKey,Integer>>() {
            @Override
            public void mapPartition(Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<ComKey, Integer>> out) throws Exception {
                for(Tuple2<String,Integer> tp: values) {
                    out.collect(new Tuple2<ComKey, Integer>(new ComKey(tp.f1,tp.f0),1));
                }
            }
        }).sortPartition(0,Order.ASCENDING);

        DataSet<Tuple2<String,Integer> > ans = temp.mapPartition(new MapPartitionFunction<Tuple2<ComKey, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void mapPartition(Iterable<Tuple2<ComKey, Integer>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
                for(Tuple2<ComKey,Integer> tp : values) {
                    out.collect(new Tuple2<String,Integer>(tp.f0.word,tp.f0.cnt));
                }
            }
        });
        ans.writeAsText(args[1], FileSystem.WriteMode.OVERWRITE);
        env.execute("wordcount");
    }
}
    '''

    _flink_config_Java = {
        "name": "flink-Java",
        "compile": {
            "compile_command": "mvn clean package -e | tee {compile_log}"
        },
        "run": {
            "command": "/usr/local/flink-1.10.2/bin/flink run -c {main_class}  {jar_path} {input_path} {out_path} | tee {out_log}",
        }
    }
    client.judge(src=src,
                 language_config=_flink_config_Java,
                 max_cpu_time=80,
                 test_case_id=3001)

