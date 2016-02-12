#!/usr/bin/python

import sys
import json

if len(sys.argv) != 2:
    print "Usage python {0} [filename]".format(sys.argv[0])
    exit(0)

with open(sys.argv[1], 'r') as f:
    data = map(lambda x: json.loads(x)['results'], f.read().split('\n')[:-1])

query = [None] * 22

legend = ['optimization', 'execution', 'parsing', 'planning', 'analysis', 'total']

for it, line in enumerate(data):
    for num, queryresult in enumerate(line):
        if not query[num]:
            query[num] = []
        if len(query[num]) == it:
            query[num].append([it])
        for key, val in queryresult.iteritems():
            if "Time" in key:
                query[num][-1].append(val)


with open('result.csv', 'wb') as f:
    for num, q in enumerate(query):
        f.write(str(num + 1) + ',' + ','.join(legend) + '\n')
        tmp = [it[::] + [sum(it)] for it in q]
        tmp.append([e / len(tmp) for e in reduce(lambda acc, l: map(lambda x, y: x + y, acc, l), tmp)])
        f.write('\n'.join([','.join(map(str, l)) for l in tmp]) + '\n')
