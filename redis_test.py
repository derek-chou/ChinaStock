import redis
import time
from datetime import datetime
from time import sleep
import getopt,sys

opts,args = getopt.getopt(sys.argv[1:], 'a')
r = redis.StrictRedis(host='60.250.174.213', port=6379, db=0)
#print r.get('b')

#ts = time.time()
tsStart = datetime.now()

print '\n********************************************'
print 'start :', tsStart
#for i in range(0,101):
for i in range(1):
	tsCurrent = datetime.now()
	strTS = tsCurrent.strftime('%Y/%m/%d %H%M%S %f')
	strTS += args[0]
	strTS += str(i)
	#print tsCurrent

	r.set(strTS, strTS)
	tsDelta = tsCurrent - tsStart
	#print tsDelta.microseconds
	#print tsDelta
print 'end :', tsCurrent
print 'duration =', tsDelta
print 'count =',i+1
print '********************************************'