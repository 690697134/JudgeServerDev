#coding=utf-8
import hashlib
import logging
import os,sys
import socket

import psutil

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.append(BASE_DIR)

from config_bigdata import SERVER_LOG_PATH
from exception_bigdata import JudgeClientError

#工具类文件
logger = logging.getLogger(__name__)
handler = logging.FileHandler("/OJ/log/judge_server.log")
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.WARNING)


def server_info():
    ver = 0x020101
    return {"hostname": socket.gethostname(),
            "cpu": psutil.cpu_percent(),
            "cpu_core": psutil.cpu_count(),
            "memory": psutil.virtual_memory().percent,
            "judger_version": ".".join([str((ver >> 16) & 0xff), str((ver >> 8) & 0xff), str(ver & 0xff)])}


def get_token():
    token = os.environ.get("TOKEN")
    if token:
        return token
    else:
        raise JudgeClientError("env 'TOKEN' not found")

#IO模式
class ProblemIOMode:
    standard = "Standard IO"
    file = "File IO"

token = hashlib.sha256(get_token().encode("utf-8")).hexdigest()
