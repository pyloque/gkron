# -*- coding: utf-8 -*-

import redis
import json

from task import TaskBuilder

class TaskIdGenerator(object):

    def __init__(self, id_min=0):
        self.id_min = id_min

    def next_id(self):
        self.id_min += 1
        return self.id_min

class TaskFetcher(object):

    def get_task(self, task_id):
        pass

class TaskSaver(object):

    def save_task(self, task_info):
        pass

    def remove_task(self, task_id):
        pass

    def clear(self):
        pass

class TaskLoader(object):

    def load_all_tasks(self):
        pass

class RedisTaskFetcher(TaskFetcher):

    def __init__(self, host, port, db):
        self.client = redis.Redis(host=host, port=port, db=db)
        self.key_pattern = 'TASK_INFO_%s'

    def get_task(self, task_id):
        task_raw = self.client.get(self.key_pattern % task_id)
        if not task_raw:
            return None
        task_json = eval(task_raw)
        return TaskBuilder.from_json(task_json)

class RedisTaskStore(RedisTaskFetcher, TaskSaver, TaskLoader):

    def __init__(self, host, port, db):
        super(RedisTaskStore, self).__init__(host, port, db)
        self.id_task = TaskIdGenerator()
        self.init()

    def save_task(self, task_info):
        task_id = self.id_task.next_id()
        task_info.id = task_id
        self.client.set(self.key_pattern % task_id, task_info.json())

    def remove_task(self, task_id):
        self.client.delete(self.key_pattern % task_id)

    def clear(self):
        keys = self.client.keys(self.key_pattern % '*')
        if not keys:
            return
        self.client.mdel(keys)
        self.id_task.id_min = 0

    def init(self):
        keys = self.client.keys(self.key_pattern % '*')
        id_max = 0
        for key in keys:
            task_id = int(key[len(self.key_pattern)-2:])
            id_max = max(id_max, task_id)
        self.id_task.id_min = id_max

    def load_all_tasks(self):
        keys = self.client.keys(self.key_pattern % '*')
        if not keys:
            return {}
        task_raws = self.client.mget(keys)
        tasks = {}
        for task_raw in task_raws:
            task_json = eval(task_raw)
            task = TaskBuilder.from_json(task_json)
            tasks[task.id] = task
        return tasks

