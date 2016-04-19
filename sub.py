import redis
import datetime
import time

r = redis.StrictRedis(host='60.250.174.213', port=6379, db=0, password="apex@tw")
p = r.pubsub()
p.psubscribe("*")

receiveCount = 0
totalDT = 0.0
maxDelta = 0
minDelta = 999999
# while True:
# for i in range(1):
beginKey = ""

for item in p.listen():
	remoteDT = str(item["data"])
	if len(remoteDT) < 10:
		continue

	receiveCount += 1
	if receiveCount == 1:
		beginKey = item["channel"]
	dtNow = datetime.datetime.now()
	dtRemote = datetime.datetime.strptime(remoteDT, "%Y-%m-%d %H:%M:%S.%f")

	dtDelta = (dtNow - dtRemote).microseconds
	totalDT += dtDelta

	if dtDelta > maxDelta:
		maxDelta = dtDelta
	if dtDelta < minDelta:
		minDelta = dtDelta

	print item["channel"], ":", item["data"], "local :", dtNow, ">>", dtDelta/1000.0
	if receiveCount == 100:
		print "begin", beginKey, ", end", item["channel"]
		break;

print "count:", receiveCount, ", avg.", totalDT/receiveCount/1000.0, ", min:", minDelta/1000.0, ", max:", maxDelta/1000.0