#!/usr/bin/python

import sys
import json

if len(sys.argv) != 2:
    print "Usage python {0} [filename]".format(sys.argv[0])
    exit(0)

with open(sys.argv[1], 'r') as f:
    data = map(lambda x: json.loads(x)['results'], f.read().split('\n')[:-1])


for it, line in enumerate(data):
    print "iteration {0}".format(it + 1)
    for num, queryresult in enumerate(line):
        print "\tTPCH #{0}".format(num + 1)
        for key, val in queryresult.iteritems():
            if "Time" in key:
                print "\t\t- {0}: {1}".format(key, val)
