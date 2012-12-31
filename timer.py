# -*- coding: utf-8 -*-

from heapq import heapify, heappush, heappop
from datetime import datetime, timedelta

def strptime(spec, _format):
    return datetime.strptime(spec, _format)

class BaseTimer(object):

    def next_run(self, last_run):
        pass

    def finished(self, last_run):
        pass

standard_time_format = '%Y-%m-%d %H:%M:%S'

class Once(BaseTimer):

    def __init__(self, when_to_run):
        self.when_to_run = when_to_run

    def next_run(self, last_run=None):
        return self.when_to_run

    def finished(self, last_run=None):
        if not last_run or last_run <= self.when_to_run:
            return False
        return True

    @staticmethod
    def from_spec(spec):
        return Once(strptime(spec, standard_time_format))

class Interval(BaseTimer):

    def __init__(self, start_run, period_by_seconds, end_run=None):
        self.start_run = start_run
        self.period = period_by_seconds
        self.end_run = end_run

    def next_run(self, last_run):
        secs = (last_run - self.start_run).total_seconds()
        if secs <= 0 :
            return self.start_run
        else:
            return last_run + timedelta(0, self.period - secs % self.period)

    def finished(self, last_run):
        if not self.end_run:
            return False
        return last_run > self.end_run

    @staticmethod
    def from_spec(spec):
        start,step,end = spec.split('/')
        return Interval(strptime(start, standard_time_format), int(step), strptime(end, standard_time_format))

class Cron(BaseTimer):

    @staticmethod
    def from_spec(spec):
        pass

class TimerBuilder(object):

    @staticmethod
    def build_timer(period_type, period_spec):
        if period_type == 'once':
            return Once.from_spec(period_spec)
        elif period_type == 'interval':
            return Interval.from_spec(period_spec)
        elif period_type == 'cron':
            return Cron.from_spec(period_spec)

class TaskManager(object):

    def __init__(self, store):
        self.tq = []
        self.store = store
        self.init()

    def init(self):
        # init task heapq
        task_infos = self.store.load_all_tasks()
        tasks_overdue = []
        for task_info in task_infos:
            _timer = TimerBuilder.build_timer(task_info.period_type, task_info.period_spec)
            _now = datetime.now()
            future_time = _timer.next_run(_now)
            if not future_time:
                tasks_overdue.append(task_info)
            self.tq.append((future_time, task_info.id))
        heapify(self.tq)
        # remove overdue tasks
        for task_info in tasks_overdue:
            self.store.remove_task(task_info.id)

    def add_task(self, task_info):
        _timer = TimerBuilder.build_timer(task_info.period_type, task_info.period_spec)
        _now = datetime.now()
        future_time = _timer.next_run(_now)
        if not future_time:
            return False
        self.store.save_task(task_info)
        heappush(self.tq, (future_time, task_info.id))

    def clear_all(self):
        self.store.clear()
        self.tq = []

    def pop_task(self):
        if self.is_empty():
            return None,None
        return heappop(self.tq)

    def peek_task(self):
        if self.is_empty():
            return None,None
        future_time,task_id = self.tq[0]
        return future_time, task_id

    def on_finish(self, task_id):
        task_info = self.store.get_task(task_id)
        if not task_info:
            return
        _timer = TimerBuilder.build_timer(task_info.period_type, task_info.period_spec)
        _now = datetime.now()
        if _timer.finished(_now):
            self.store.remove_task(task_id)
            return
        future_time = _timer.next_run(_now)
        heappush(self.tq, (future_time, task_id))

    def is_empty(self):
        return not self.tq
