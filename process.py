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
from timer import TaskManager
from store import RedisTaskStore
from daemon import daemon_init
from worker import Worker

class Process(object):

    def __init__(self, child_num):
        self.child_num = child_num
        self.child_fds = set()
        self.child_pids = set()
        self.child_in_busy = set()
        self.handlers = HandlerMap()
        self.buffer = Buffer()
        self.pending_task_ids = []
        self.make_daemon()
        self.store = RedisTaskStore('localhost', 6379, '9')
        self.delayed_tasks = TaskManager(self.store)
        self.stoped = False

    def make_daemon(self):
        daemon_init("/var/log/gkron/gkron.out", "/var/log/gkron/gkron.err")

    def start(self):
        for i in range(self.child_num):
            self.fork_child()
        self.register_handlers()
        self.register_signals()
        while not self.stoped:
            self.interact()
        self.wait_children_to_die()

    def add_task(self, task):
        self.delayed_tasks.add_task(task)

    def clear_all_tasks(self):
        self.delayed_tasks.clear_all()

    def fork_child(self):
        parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM, 0)
        parent.setblocking(0)
        child.setblocking(0)
        pid = os.fork()
        if pid:
            self.father(pid, parent, child)
        else:
            self.child(parent, child)

    def child(self, parent, child):
        child.close()
        Worker(parent, self.store).start()
        sys.exit(0)

    def father(self, pid, parent, child):
        parent.close()
        self.child_fds.add(child)
        self.child_pids.add(pid)

    def wait_children_to_die(self):
        for pid in self.child_pids:
            try:
                os.waitpid(pid, os.P_WAIT)
            except OSError, ex:
                if ex[0] == errno.EINTR:
                    pass
                else:
                    raise
        print 'father is over'

    def interact(self):
        self.process_timer()
        r,w=[],[]
        try:
            r,w,e=select.select(self.child_fds, self.child_fds - self.child_in_busy, [], 2)
        except select.error, ex:
            if ex[0] == errno.EINTR:
                self.prepare_exit()
                return
            if ex[0] == errno.EAGAIN:
                pass
            else:
                raise
        random.shuffle(w)
        for child in w:
            if len(self.pending_task_ids) == 0:
                break
            task_id = self.pending_task_ids.pop()
            self.send_child(child, 'task:%s' % task_id)
        for child in r:
            msg = child.recv(1024)
            self.buffer.append(child, msg)
            while True:
                fd, cmd_type, cmd_param = self.buffer.next_cmd()
                if not fd:
                    break
                self.handlers.process_cmd(fd, cmd_type, cmd_param)

    def process_timer(self):
        if self.delayed_tasks.is_empty():
            return
        while not self.delayed_tasks.is_empty():
            _timer, task_id = self.delayed_tasks.peek_task()
            border = datetime.now()
            if _timer > border:
                break
            _timer, task_id = self.delayed_tasks.pop_task()
            self.pending_task_ids.append(task_id)

    def send_child(self, child, cmd):
        self.child_in_busy.add(child)
        child.send(cmd +';')

    def prepare_exit(self):
        print 'ctrl+c is catched'
        for child in self.child_fds:
            try:
                child.send('bye:')
            except IOError, ex:
                if ex[0] == errno.EPIPE:
                    pass
                else:
                    raise
        self.stoped = True

    def register_signals(self):
        signal.signal(signal.SIGINT, self.int_handler)
        signal.signal(signal.SIGCHLD, self.child_exit_handler)

    def int_handler(self,signum,frame):
        print 'int signal catched %s %s' %(signum, frame)
        self.prepare_exit()

    def child_exit_handler(self, signum, frame):
        print 'child exit signal catched %s %s' %(signum, frame)

    def register_handlers(self):
        self.handlers.add_handler('task_finish', self.task_finish)

    def task_finish(self, fd, param):
        self.child_in_busy.remove(fd)
        task_id = int(param)
        self.delayed_tasks.on_finish(task_id)

