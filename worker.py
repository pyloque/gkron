# -*- coding: utf-8 -*-

import os
import sys
import time
import errno
import random
import socket
import select
import signal
from datetime import datetime, timedelta

from core import Buffer, HandlerMap

class Worker(object):

    def __init__(self, parent, store):
        self.parent = parent
        self.store = store
        self.buffer = Buffer()
        self.handlers = HandlerMap()
        self.stoped = False

    def start(self):
        self.register_handlers()
        self.register_signals()
        while not self.stoped:
            self.interact()

    def register_handlers(self):
        self.handlers.add_handler('bye', self.bye)
        self.handlers.add_handler('task', self.task)

    def command(self, cmd):
        self.buffer.append(self.parent, cmd)
        while True:
            fd, cmd_type, cmd_param = self.buffer.next_cmd()
            if not cmd_type:
                break
            self.handlers.process_cmd(fd, cmd_type, cmd_param)

    def bye(self,fd,param):
        self.prepare_exit()

    def task(self, fd, param):
        task_id = int(param)
        task_info = self.store.get_task(task_id)
        if task_info:
            task_info.run()
        self.send_parent('task_finish:%s' % task_id)

    def send_parent(self, cmd):
        self.parent.send(cmd + ';')

    def interact(self):
        r,w,e=[],[],[]
        try:
            r,w,e=select.select([self.parent], [self.parent], [], 2)
        except select.error, ex:
            if ex[0] == errno.EINTR:
                self.prepare_exit()
                return
            if ex[0] == errno.EAGAIN:
                pass
            else:
                raise
        if not r:
            return
        father = r[0]
        cmd = father.recv(1024)
        self.command(cmd)

    def prepare_exit(self):
        self.stoped = True

    def register_signals(self):
        signal.signal(signal.SIGINT, self.int_handler)

    def int_handler(self, signum, frame):
        print 'signal catched %s %s' % (signum, frame)
        self.prepare_exit()

