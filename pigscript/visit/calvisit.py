#!/usr/bin/python
import time
import sys 
from org.apache.pig.scripting import Pig

if __name__ == '__main__':
    P = Pig.compileFromFile("""calvisit.pig""")

    defaulttime = time.time()
    deadlinesec = defaulttime - 1800
    deadline = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(deadlinesec))
    
    if len(sys.argv) > 1:
        deadline = sys.argv[1]

    Q = P.bind({'deadline':deadline, 'input':'input', 'result':'result', 'inputtmp':'inputtmp'})
    results = Q.runSingle()
    if results.isSuccessful() == "FAILED":
       raise "Pig job failed"
    else:
       print result
