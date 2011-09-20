#! /usr/bin/env python

from downpour import BaseRequest, BaseFetcher

f = file('urls.txt')
reqs = [BaseRequest(u) for u in f.read().strip().split('\n')]
f.close()
BaseFetcher(20, reqs).start()