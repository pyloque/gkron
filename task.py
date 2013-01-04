# -*- coding: utf-8 -*-

import os
import time
from datetime import datetime

import urlfetch
from timer import standard_time_format

class TaskInfo(object):

    def __init__(self, period_type, period_spec, task_id=None):
        self.period_type = period_type
        self.period_spec = period_spec
        self.id = task_id

    def json(self):
        r = {
             'id': self.id,
             'period_type': self.period_type,
             'period_spec': self.period_spec,
             'task_type': self.task_type
             }
        r.update(self.__json__())
        return r

class HttpTask(TaskInfo):

    def __init__(self, period_type, period_spec, task_id=None, **kwargs):
        super(HttpTask, self).__init__(period_type, period_spec, task_id)
        self.url = kwargs.get('url')
        self.method = kwargs.get('method', 'GET')
        self.data = kwargs.get('data', None)

    @property
    def task_type(self):
        return 'http'

    def run(self):
        try:
            self._run()
        except urlfetch.RequestError, ex:
            print repr(ex)

    def _run(self):
        if self.method == 'GET':
            urlfetch.get(self.url)
        elif self.method == 'POST':
            urlfetch.post(self.url, self.data)
        print '%s %s task is running url=%s in process %d' % (datetime.now().strftime(standard_time_format), self.period_type, self.url, os.getpid())

    def __json__(self):
        return {
                'url': self.url,
                'method': self.method,
                'data': self.data,
                }

class MemoryTask(TaskInfo):

    def __init__(self, period_type, period_spec, task_id=None, **kwargs):
        super(MemoryTask, self).__init__(period_type, period_spec, task_id)
        self.value = kwargs.get('value')

    @property
    def task_type(self):
        return 'memory'

    def run(self):
        print '%s %s task is running value=%d in process %d' % (datetime.now().strftime(standard_time_format), self.period_type, self.value, os.getpid())

    def __json__(self):
        return {
                'value': self.value
                }

class TaskBuilder(object):

    @staticmethod
    def from_json(r):
        task_type = r.pop('task_type')
        task_cls = None
        if task_type == 'memory':
            task_cls = MemoryTask
        elif task_type == 'http':
            task_cls = HttpTask
        else:
            task_cls = None
        if not task_cls:
            print 'illegal task format %s' % repr(r)
            return None
        task_id = r.pop('id')
        period_type = r.pop('period_type')
        period_spec = r.pop('period_spec')
        return task_cls(period_type, period_spec, task_id, **r)
