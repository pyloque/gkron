# -*- coding: utf-8 -*-
import re

from datetime import datetime, timedelta
from heapq import heapify, heappush, heappop

from utils import ParseError
from utils import parse_array

def strptime(spec, _format):
    return datetime.strptime(spec, _format)

class BaseTimer(object):

    def next_run(self, last_run):
        pass

standard_time_format = '%Y-%m-%d %H:%M:%S'

class Once(BaseTimer):

    def __init__(self, when_to_run):
        self.when_to_run = when_to_run

    def next_run(self, last_run=None):
        if last_run and last_run > self.when_to_run:
            return None
        return self.when_to_run

    @staticmethod
    def from_spec(spec):
        return Once(strptime(spec, standard_time_format))

class Interval(BaseTimer):

    def __init__(self, start_run, period_by_seconds, end_run=None):
        self.start_run = start_run
        self.period = period_by_seconds
        self.end_run = end_run

    def next_run(self, last_run):
        if self.end_run and last_run > self.end_run:
            return None
        secs = (last_run - self.start_run).total_seconds()
        if secs <= 0 :
            return self.start_run
        else:
            return last_run + timedelta(0, self.period - secs % self.period)

    @staticmethod
    def from_spec(spec):
        start,step,end = spec.split('/')
        return Interval(strptime(start, standard_time_format), int(step), strptime(end, standard_time_format))

cron_pattern = re.compile('^[\d| |\*|\-|/]+$')
unit_types = ['year', 'month', 'day', 'hour', 'minute', 'second']
part_num = 6

class Unit(object):

    def __init__(self, unit_type, choices):
        self.unit_type = unit_type
        self.choices = choices # already sorted

    def calc_pos(self, value):
        for i in range(self.length):
            choice = self.choices[i]
            if choice == value:
                return 'equal',i
            elif choice > value:
                if i > 0:
                    return 'more',i-1 # between postion i-1 and i
                else:
                    return 'less',0 # not yet arrived at position 0
        return 'overflow',None # overflow

    def get_value(self, pos):
        return self.choices[pos]

    @property
    def length(self):
        return len(self.choices)

    def __repr__(self):
        return '%s:%s' % (self.unit_type, self.choices)

class CronUnit(object):

    def __init__(self, unit_choices):
        self.units = []
        for i in range(part_num):
            self.units.append(Unit(unit_types[i], unit_choices[i]))

    def calc_pos(self, t):
        t = [t.year, t.month, t.day, t.hour, t.minute, t.second]
        positions = [0] * 6
        rel = 'less'
        for i in range(part_num):
            unit = self.units[i]
            rel, pos = unit.calc_pos(t[i])
            positions[i] = pos # if overflow,pos is marked as None here
            if rel in('less', 'overflow'):
                break
        return positions, rel

    def next_pos(self, cur_pos, rel):
        if rel in('less', 'equal'):
            return cur_pos
        if rel in('more',):
            cur_pos[part_num-1] += 1
            return cur_pos
        # overflow is hardy to handle
        cur_pos = self.carry_pos(cur_pos)
        return cur_pos

    def carry_pos(self, cur_pos):
        of_pos = None # find overflow position first
        for i in range(part_num):
            if cur_pos[i] is None:
                of_pos = i
        # need to add carry flag to father(like add operation)
        father_pos = None
        for i in range(of_pos-1, -1, -1):
            if self.units[i].length > cur_pos[i]+1:
                father_pos = i
                break
        if father_pos is None:
            return None
        cur_pos[father_pos] += 1 # add carry
        # clear zero for postion between father pos and overflow pos
        for i in range(father_pos+1, of_pos+1):
            cur_pos[i] = 0
        return cur_pos

    def next_day(self, cur_pos):
        '''
        days length is different for months and the leap year
        '''
        pos_day = 2
        cur_pos[pos_day] = None # mark for overflow
        # clear zero for hour-min-sec
        for i in range(pos_day, part_num):
            cur_pos[i] = 0
        cur_pos = self.carry_pos(cur_pos)
        return cur_pos

    def calc_time(self, cur_pos):
        t = []
        for i in range(part_num):
            t.append(self.units[i].get_value(cur_pos[i]))
        try:
            xtime = datetime(*t)
            return xtime
        except ValueError, ex:
            if ex[0] <> 'day is out of range for month':
                raise
        cur_pos = self.next_day(cur_pos)
        if not cur_pos:
            return None
        return self.calc_time(cur_pos)

    def __repr__(self):
        return repr(self.units)

class Cron(BaseTimer):

    def __init__(self, cron_unit):
        self.cron_unit = cron_unit

    def next_run(self, last_run):
        cur_pos, rel = self.cron_unit.calc_pos(last_run)
        if not cur_pos:
            return None
        next_pos = self.cron_unit.next_pos(cur_pos, rel)
        if not next_pos:
            return None
        return self.cron_unit.calc_time(next_pos)

    @staticmethod
    def from_spec(spec):
        if not cron_pattern.match(spec):
            raise ParseError('illegal cron format, only digit,whitespace,*,-,/ is allowed')
        parts = spec.split(' ')
        parts = filter(lambda x:bool(x), parts)
        if len(parts) <> part_num:
            raise ParseError('illegal cron format, must be six part seperated by whitespace')
        unit_choices = []
        for i in range(part_num):
            unit_choices.append(parse_array[i](parts[i]))
        unit = CronUnit(unit_choices)
        return Cron(unit)

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
        for task_info in task_infos.values():
            _timer = TimerBuilder.build_timer(task_info.period_type, task_info.period_spec)
            _now = datetime.now()
            future_time = _timer.next_run(_now)
            if not future_time:
                tasks_overdue.append(task_info)
            else:
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
        # if task is executed too fast less than 1 second, task will be repeated execute
        # add 1s timedelta to keep bother away
        _now = datetime.now() + timedelta(0,1)
        future_time = _timer.next_run(_now)
        if not future_time:
            self.store.remove_task(task_id)
            return
        heappush(self.tq, (future_time, task_id))

    def is_empty(self):
        return not self.tq
