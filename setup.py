#!/usr/bin/env python

import os
from setuptools import setup

setup(name       = 'downpour',
	version      = '0.1.0',
	description  = 'Fetch urls. Fast.',
	long_description = 'Uses twisted to fetch URLs asynchronously, with callbacks. Fast.',
	url          = 'http://github.com/dlecocq/freshscape',
	author       = 'Dan Lecocq',
	author_email = 'dan@seomoz.org',
	keywords     = 'Twisted, fetching, url, download, asynchronous',
	license      = 'SEOmoz',
	packages     = ['downpour'],
	classifiers  = [
		'Programming Language :: Python',
		'Intended Audience :: Developers',
		'Operating System :: OS Independent',
	]
)
