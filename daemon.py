# -*- coding: utf-8 -*-

import os
import sys
import signal

def daemon_init(log_out, log_err):
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
    r=open("/dev/null", "r")
    w=open(log_out, "a+")
    e=open(log_err, "a+", 0)
    os.dup2(r.fileno(), sys.stdin.fileno())
    os.dup2(w.fileno(), sys.stdout.fileno())
    os.dup2(e.fileno(), sys.stderr.fileno())
