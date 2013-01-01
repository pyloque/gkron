# -*- coding: utf-8 -*-

import requests

RequestError = requests.RequestException

singleton = None

def session():
    global singleton
    if singleton is None:
        singleton = requests.Session()
    return singleton

request = lambda *args, **kwargs: session().request(*args, **kwargs)
head = lambda url, **kwargs: session().head(url, **kwargs)
get = lambda url, **kwargs: session().get(url, **kwargs)
post = lambda url, data=None, **kwargs: session().post(url, data, **kwargs)
put = lambda url, data=None, **kwargs: session().put(url, data, **kwargs)
patch = lambda url, data=None, **kwargs: session().patch(url, data, **kwargs)
delete = lambda url, **kwargs: session().delete(url, **kwargs)
