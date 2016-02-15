#!/usr/bin/python

import sys
import json

if len(sys.argv) != 2:
    print "Usage python {0} [filename]".format(sys.argv[0])
    exit(0)

with open(sys.argv[1], 'r') as f:
    data = map(lambda x: json.loads(x)['results'], f.read().split('\n')[:-1])

query = {}

legend = ['optimization', 'execution', 'parsing', 'planning', 'analysis', 'total']

for it, line in enumerate(data):
    print "iteration {0}".format(it + 1)
    for queryresult in line:
        num = queryresult['name']
        print "\t{0}".format(num)
        if num not in query:
            query[num] = []
        if len(query[num]) == it:
            query[num].append([it + 1])
        for key, val in queryresult.iteritems():
            if "Time" in key:
                query[num][-1].append(val)
                print "\t\t{0} {1}".format(key, val)


with open('result.csv', 'wb') as f:
    i = 0
    j = 0
    while j < len(query):
        i += 1
        name = "tpch{0}".format(i)
        if name not in query:
            continue
        q = query[name]
        f.write(name + ',' + ','.join(legend) + '\n')
        tmp = [it[::] + [sum(it)] for it in q]
        # tmp.append([e / len(tmp) for e in reduce(lambda acc, l: map(lambda x, y: x + y, acc, l), tmp)])
        f.write('\n'.join([','.join(map(str, l)) for l in tmp]) + '\n\n')
        j += 1
