#!/usr/bin/env python
"""reducer_mean.py"""

import sys

        
if __name__ == "__main__": 
    cnt = 0
    for line in sys.stdin:
        split = line.split("\t")
        if cnt == 0:
            c_j = float(split[0])
            m_j = float(split[1])
            cnt = cnt + 1
        else:
            c_k = float(split[0])
            m_k = float(split[1])
            m_i = (c_j * m_j + c_k * m_k) / (c_j + c_k)
            m_j = m_i
            c_j = c_k 
        
    print(f"Среднее значение поля price (with mapreduce): {m_j}")

    