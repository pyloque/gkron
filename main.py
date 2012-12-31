# -*- coding: utf-8 -*-

from datetime import datetime, timedelta

from process import Process
from timer import standard_time_format
from task import MemoryTask

i = 0
_now = datetime.now()
_delta = timedelta(0, 1)

def _create_interval_memory_task():
    return MemoryTask(999, 'interval', '%s/%s/%s' % ((_now + _delta).strftime(standard_time_format), 2, (_now + _delta * 11).strftime(standard_time_format)))

def _create_once_memory_task(k):
    return MemoryTask(k, 'once', (_now + _delta * k).strftime(standard_time_format))

def main():
    process = Process(1)
    process.add_task(_create_interval_memory_task())
    for i in range(1,11):
        process.add_task(_create_once_memory_task(i))
    process.start()

if __name__ == '__main__':
    main()
