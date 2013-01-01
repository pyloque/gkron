# -*- coding: utf-8 -*-

class Buffer(object):

    def __init__(self):
        self.streams = {}

    def append(self, fd, msg):
        stream = self.ensure_fd(fd)
        stream.extend(msg)

    def ensure_fd(self, fd):
        stream = self.streams.get(fd, None)
        if not stream:
            stream = bytearray()
            self.streams[fd] = stream
        return stream

    def next_cmd_fd(self ,fd):
        stream = self.ensure_fd(fd)
        i = stream.find(';')
        if i < 0:
            return None
        cmd = str(stream[:i])
        for k in range(i+1):
            stream.pop(0)
        x = cmd.split(':')
        if len(x) == 1:
            cmd_type,cmd_param=x,None
        cmd_type,cmd_param=x
        if not cmd_type:
            print 'illegal cmd protocal %s' % cmd
            return self.next_cmd()
        return fd, cmd_type, cmd_param

    def next_cmd(self):
        fds = self.streams.keys()
        for fd in fds:
            r = self.next_cmd_fd(fd)
            if r:
                return r
        return None,None,None

class HandlerMap(object):

    def __init__(self):
        self.handlers = {}

    def add_handler(self, cmd_type, handler):
        self.handlers[cmd_type] = handler

    def process_cmd(self, fd, cmd_type, cmd_param):
        handler = self.handlers.get(cmd_type, None)
        if handler:
            handler(fd,cmd_param)
        else:
            self.default(fd, cmd_type, cmd_param)

    def default(self, fd, cmd_type, cmd_param):
        print 'Unknown command %s %s from %s' % (cmd_type,cmd_param, fd)

