# -*- coding: utf-8 -*-

import os
import sys
import signal

def daemon_init():
    pid = os.fork()
    if pid > 0:
        sys.exit(0)
    os.setsid()
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    pid = os.fork()
    if pid > 0:
        sys.exit(0)
    os.chdir("/")
    os.umask(0)
