import _judger
import hashlib
import logging
import os
import socket

import psutil

from server.config import SERVER_LOG_PATH
from server.exception import JudgeClientError

#工具类文件
logger = logging.getLogger(__name__)
handler = logging.FileHandler(SERVER_LOG_PATH)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.WARNING)


def server_info():
    ver = _judger.VERSION
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
