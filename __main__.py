#! /usr/bin/env python

from downpour import BaseRequest, BaseFetcher, logger
import logging

logger.setLevel(logging.DEBUG)

f = file('urls.txt')
reqs = [BaseRequest(u) for u in f.read().strip().split('\n')]
f.close()
BaseFetcher(100, reqs).start()