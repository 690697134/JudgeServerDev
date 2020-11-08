FROM registry.cn-hangzhou.aliyuncs.com/mrhanice/repository:ubuntu18.04v1.0

#RUN (/usr/sbin/sshd -D &) && cd /usr/local/hadoop-2.7.7 && sbin/start-dfs.sh && sbin/start-yarn.sh && cd /usr/local/flink-1.10.2 && bin/start-cluster.sh

HEALTHCHECK --interval=5s --retries=3 CMD python3 /code/service_bigdata.py
ADD server4bigdata /code
WORKDIR /code
RUN gcc -shared -fPIC -o unbuffer.so unbuffer.c
EXPOSE 8080
ENTRYPOINT /code/entrypoint.sh
