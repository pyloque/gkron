# -*- coding: utf-8 -*-

import os
import sys
import time
import errno
import socket
import select
import signal
from datetime import datetime, timedelta

from core import Buffer, HandlerMap
from timer import TaskManager
from store import RedisTaskStore

class Process(object):

    def __init__(self, child_num):
        self.child_num = child_num
        self.child_fds = set()
        self.child_pids = set()
        self.handlers = HandlerMap()
        self.buffer = Buffer()
        self.pending_task_ids = []
        self.store = RedisTaskStore('localhost', 6379, '9')
        self.delayed_tasks = TaskManager(self.store)
        self.stoped = False

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
            r,w,e=select.select(self.child_fds, self.child_fds, [], 2)
        except select.error, ex:
            if ex[0] == errno.EINTR:
                self.prepare_exit()
                return
            if ex[0] == errno.EAGAIN:
                pass
            else:
                raise
        for child in r:
            msg = child.recv(1024)
            self.buffer.append(msg)
            while True:
                cmd_type, cmd_param = self.buffer.next_cmd()
                if not cmd_type:
                    break
                self.handlers.process_cmd(cmd_type, cmd_param)
        for child in w:
            if len(self.pending_task_ids) == 0:
                break
            task_id = self.pending_task_ids.pop()
            self.send_child(child, 'task:%s' % task_id)

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

    def task_finish(self, param):
        task_id = int(param)
        self.delayed_tasks.on_finish(task_id)

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
        self.buffer.append(cmd)
        while True:
            cmd_type, cmd_param = self.buffer.next_cmd()
            if not cmd_type:
                break
            self.handlers.process_cmd(cmd_type, cmd_param)

    def bye(self,param):
        self.prepare_exit()

    def task(self, param):
        task_id = int(param)
        task_info = self.store.get_task(task_id)
        task_info.run()
        self.send_parent('task_finish:%s' % param)

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

