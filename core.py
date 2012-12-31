# -*- coding: utf-8 -*-

class Buffer(object):

    def __init__(self):
        self.stream = bytearray()

    def append(self, msg):
        self.stream.extend(msg)

    def next_cmd(self):
        i = self.stream.find(';')
        if i < 0:
            return None,None
        cmd = str(self.stream[:i])
        for k in range(i+1):
            self.stream.pop(0)
        x = cmd.split(':')
        if len(x) == 1:
            cmd_type,cmd_param=x,None
        cmd_type,cmd_param=x
        if not cmd_type:
            print 'illegal cmd protocal %s' % cmd
            return self.next_cmd()
        return cmd_type, cmd_param

class HandlerMap(object):

    def __init__(self):
        self.handlers = {}

    def add_handler(self, cmd_type, handler):
        self.handlers[cmd_type] = handler

    def process_cmd(self, cmd_type, cmd_param):
        print '*' *10, cmd_type, cmd_param
        handler = self.handlers.get(cmd_type, None)
        if handler:
            handler(cmd_param)
        else:
            self.default(cmd_type, cmd_param)

    def default(self, cmd_type, cmd_param):
        print 'Unknown command %s %s' % (cmd_type,cmd_param)

