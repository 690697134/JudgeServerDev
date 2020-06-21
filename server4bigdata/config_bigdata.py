#coding=utf-8
import os
import pwd

import grp

#一些文件的配置路径
PROJECT_BASE = '/OJ/problems'
JUDGER_WORKSPACE_BASE = "/OJ/run"
TEST_CASE_DIR = "/OJ/test_case"
LOG_BASE = "/OJ/log"

COMPILER_LOG_PATH = os.path.join(LOG_BASE, "compile.log")
JUDGER_RUN_LOG_PATH = os.path.join(LOG_BASE, "judger.log")
SERVER_LOG_PATH = os.path.join(LOG_BASE, "judge_server.log")

#获取用户id和用户组id
RUN_USER_UID = pwd.getpwnam("code").pw_uid
RUN_GROUP_GID = grp.getgrnam("code").gr_gid

#获取用户id和用户组id
COMPILER_USER_UID = pwd.getpwnam("compiler").pw_uid
COMPILER_GROUP_GID = grp.getgrnam("compiler").gr_gid


