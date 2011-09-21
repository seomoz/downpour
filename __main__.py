#! /usr/bin/env python

from downpour import BaseRequest, BaseFetcher, logger
import logging

logger.setLevel(logging.DEBUG)
# Make a formatter
formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s:%(funcName)s => %(message)s')
# Add a handler
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)

f = file('urls.txt')
reqs = [BaseRequest(u) for u in f.read().strip().split('\n')]
f.close()
BaseFetcher(10, reqs).start()