#!/usr/bin/env python
"""mapper_mean.py"""

import sys


def map(line):
    try:
        val = line.split(",")[-7]
        if val != "price":
            return float(val)
        else:
            return "no"
    except:
        return "no"
        

if __name__ == "__main__":
    cs = 0
    count = 0
    for line in sys.stdin:
        line = line.strip()
        val = map(line)
        if val != "no":
            count = count + 1
            cs = cs + val

        
    cm = cs / count
    print(count, cm, sep="\t")