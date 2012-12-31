# -*- coding: utf-8 -*-

class TaskInfo(object):

    def __init__(self, period_type, period_spec, task_id=None):
        self.id = task_id
        self.period_type = period_type
        self.period_spec = period_spec

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

    def __init__(self, url, method, data, period_type, period_spec, task_id=None):
        super(HttpTask, self).__init__(period_type, period_spec, task_id)
        self.url = url
        self.method = method
        self.data = data

    @property
    def task_type(self):
        return 'http'

    def run(self):
        pass

    def __json__(self):
        return {
                'url': self.url,
                'method': self.method,
                'data': self.data,
                }

class MemoryTask(TaskInfo):

    def __init__(self, value, period_type, period_spec, task_id=None):
        super(MemoryTask, self).__init__(period_type, period_spec, task_id)
        self.value = value

    @property
    def task_type(self):
        return 'memory'

    def run(self):
        print '%s task is running value=%d' % (self.period_type, self.value)

    def __json__(self):
        return {
                'value': self.value
                }

class TaskBuilder(object):

    @staticmethod
    def from_json(r):
        task_type = r['task_type']
        if task_type == 'memory':
            return MemoryTask(r['value'], r['period_type'], r['period_spec'], r['id'])
        elif task_type == 'http':
            return HttpTask(r['url'], r['method'], r['data'], r['period_type'], r['period_spec'], r['id'])
        else:
            return None
