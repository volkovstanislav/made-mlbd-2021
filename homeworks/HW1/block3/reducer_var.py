#!/usr/bin/env python
"""reducer_var.py"""

import sys

        
if __name__ == "__main__": 
    cnt = 0
    for line in sys.stdin:
        split = line.split("\t")
        if cnt == 0:
            c_j = float(split[0])
            m_j = float(split[1])
            v_j = float(split[2])
            cnt = cnt + 1
        else:
            c_k = float(split[0])
            m_k = float(split[1])
            v_k = float(split[2])
            m_i = (c_j * m_j + c_k * m_k) / (c_j + c_k)
            v_i = ((c_j * v_j + c_k * v_k) / (cj + ck)) + c_j * c_k * ((mj - mk) / (cj + ck)) * ((mj - mk) / (cj + ck))
            v_j = v_i
            m_j = m_i
            c_j = c_k 
        

    print(f"Дисперсия поля price (with mapreduce): {v_j}")

    