# -*- coding: utf-8 -*-

import time
from datetime import datetime, timedelta

from process import Process
from timer import standard_time_format
from task import MemoryTask, HttpTask

i = 0
_now = datetime.now()
_delta = timedelta(0, 1)

_total = 14 * 10

def _create_interval_memory_task():
    return MemoryTask('interval', '%s/%s/%s' % ((_now + _delta).strftime(standard_time_format), 2, (_now + _delta * _total).strftime(standard_time_format)), value=999)

def _create_once_memory_task(k):
    return MemoryTask('once', (_now + _delta * k).strftime(standard_time_format), value=k)

def _create_cron_memory_task():
    return MemoryTask('cron', '* * * * */2 */3', value=888)

def _create_cron_http_task():
    return HttpTask('cron', '* * * * * */2', url='http://www.guokr.com', method='GET')

def main():
    process = Process(10)
    process.add_task(_create_interval_memory_task())
    for i in range(1,_total):
        process.add_task(_create_once_memory_task(i))
    process.add_task(_create_cron_memory_task())
    for i in range(10):
        process.add_task(_create_cron_http_task())
    process.start()

if __name__ == '__main__':
    main()
