# -*- coding: utf-8 -*-

from datetime import datetime, timedelta

from process import Process
from timer import standard_time_format
from task import MemoryTask, HttpTask

i = 0
_now = datetime.now()
_delta = timedelta(0, 1)

_total = 14 * 10

def _create_interval_memory_task():
    return MemoryTask(999, 'interval', '%s/%s/%s' % ((_now + _delta).strftime(standard_time_format), 2, (_now + _delta * _total).strftime(standard_time_format)))

def _create_once_memory_task(k):
    return MemoryTask(k, 'once', (_now + _delta * k).strftime(standard_time_format))

def _create_cron_memory_task():
    return MemoryTask(888, 'cron', '* * * * */2 */3')

def _create_cron_http_task():
    return HttpTask('http://www.guokr.com', 'GET', None, 'cron', '* * * * * */2')

def main():
    process = Process(10)
    #process.add_task(_create_interval_memory_task())
    #for i in range(1,_total):
    #    process.add_task(_create_once_memory_task(i))
    #process.add_task(_create_cron_memory_task())
    process.add_task(_create_cron_http_task())
    process.start()

if __name__ == '__main__':
    main()
