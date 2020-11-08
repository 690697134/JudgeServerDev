#!/bin/bash

/usr/sbin/sshd -D &

rm -rf /judger/*
mkdir -p /judger/run /judger/spj
mkdir -p /OJ/run

chown compiler:code /judger/run /OJ/run
chmod 711 /judger/run /OJ/run

chown compiler:spj /judger/spj
chmod 710 /judger/spj

cd /usr/local/hadoop-2.7.7
sbin/start-dfs.sh
sbin/start-yarn.sh
cd /usr/local/flink-1.10.2
bin/start-cluster.sh

cd /code

core=$(grep --count ^processor /proc/cpuinfo)
n=$(($core*2))
#exec gunicorn --workers $n --threads $n  --error-logfile /OJ/log/gunicorn.log --time 600 --bind 0.0.0.0:8080 server_bigdata:app --log-level=debug --reload
gunicorn --workers $n --threads $n --error-logfile /OJ/log/gunicorn.log --log-level debug --time 300 --bind 0.0.0.0:8080 server_bigdata:app
#python3 server_bigdata.py
