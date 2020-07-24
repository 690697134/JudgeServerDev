import json
import os

import requests

from server.exception import JudgeServiceError
from server.utils import server_info, logger, token

#SERVICE_URL=http://judge-server:8080
#BACKEND_URL=http://oj-backend:8000/api/judge_server_heartbeat/
#给后端发送心跳
class JudgeService(object):
    def __init__(self):
        self.service_url = os.environ["SERVICE_URL"]
        self.backend_url = os.environ["BACKEND_URL"]

    def _request(self, data):
        try:
            resp = requests.post(self.backend_url, json=data,
                                 headers={"X-JUDGE-SERVER-TOKEN": token,
                                          "Content-Type": "application/json"}, timeout=5).text
        except Exception as e:
            logger.exception(e)
            raise JudgeServiceError("Heartbeat request failed")
        try:
            r = json.loads(resp)
            if r["error"]:
                raise JudgeServiceError(r["data"])
        except Exception as e:
            logger.exception("Heartbeat failed, response is {}".format(resp))
            raise JudgeServiceError("Invalid heartbeat response")

    def heartbeat(self):
        data = server_info()
        data["action"] = "heartbeat"
        data["service_url"] = self.service_url
        self._request(data)


if __name__ == "__main__":
    try:
        if not os.environ.get("DISABLE_HEARTBEAT"):
            service = JudgeService()
            service.heartbeat()
        exit(0)
    except Exception as e:
        logger.exception(e)
        exit(1)
