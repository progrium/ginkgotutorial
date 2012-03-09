#!/usr/bin/python

import commands

BRANCHES = [
    'chapter1',
    'chapter2a',
    'chapter2b',
    'chapter3a',
    #'chapter3b',
    'chapter4a',
    'chapter4b',
    'chapter4c',
    #'chapter5a',
    #'chapter5b',
    'chapter6',
    ]

def checkout(branch):
	return commands.getoutput('git checkout %s' % branch)

for branch in BRANCHES:
    output = checkout(branch)
    raw_input('%-32s [continue] ' % output)
