#coding=utf-8
import hashlib
import json
import os
import requests

#JudgeServerClientError
class JudgeServerClientError(Exception):
    pass

class JudgeServerClient(object):
    def __init__(self,token, server_base_url):
        self.token = hashlib.sha256(token.encode("utf-8")).hexdigest()
        self.server_base_url = server_base_url.rstrip("/")

    def _request(self,url,data=None):
        kwargs = {'headers':{'X-Judge-Server-Token':self.token,
                             'Content-Type':'application/json'}}
        if data:
            kwargs['data'] = json.dumps(data)
        try:
            return requests.post(url,**kwargs).json()
        except Exception as e:
            raise JudgeServerClientError(str(e))

    def ping(self):
        return self._request(self.server_base_url + '/ping')

    def judge(self,src,language_config,max_cpu_time,test_case_id=0):
        data = {
            'language_config':language_config,
            'src':src,
            'max_cpu_time':max_cpu_time,
            'test_case_id':test_case_id,
        }
        return self._request(self.server_base_url + '/judgebigdata',data=data)

if __name__ == '__main__':
    token = 'OJ4BigData'
    client = JudgeServerClient(token=token, server_base_url="http://127.0.0.1:8090")
    src = '''
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

class WcMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    private Text word = new Text();
    private IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringBuffer stringBuffer = new StringBuffer(value.toString());
        for(int i = 0; i < stringBuffer.length(); i++ ) {
            if(!Character.isLetter(stringBuffer.charAt(i))) {
                stringBuffer.setCharAt(i,' ');
            }
        }
        String[] words = stringBuffer.toString().split(" ");

        for(String word: words) {
            if(word.trim().isEmpty()) {
                continue;
            }
            this.word.set(word);
            context.write(this.word,this.one);
        }
    }
}

class WcReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable total = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable v : values) {
            sum += v.get();
        }
        this.total.set(sum);
        context.write(key,total);
    }
}
public class Main extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        //int a = 1;
        //int c = a / 0;
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf,"wordcount");

        job.setJarByClass(Main.class);
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(WcReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,new Path(strings[0]));
        FileOutputFormat.setOutputPath(job,new Path(strings[1]));

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),new Main(),args);
        System.exit(res);
    }
}'''

    hadoop_config = {
        "name": "hadoop",
        "compile": {
            "compile_command": "mvn clean package -e | tee {compile_log}"
        },
        "run": {
            # "command": "hadoop jar {jar_path} -D mapreduce.job.queuename={queue_name} {main_class} {input_path} {out_path} | tee {out_log}",
            "command": "hadoop jar {jar_path} {main_class} -Dmapred.job.queue.name={queue_name} {input_path} {out_path} >> {out_log} 2>&1",
        }
    }
    client.judge(src=src,
                 language_config=hadoop_config,
                 max_cpu_time=300,
                 test_case_id=1001)
    # client.judge(src,languages.hadoop_config,0)

