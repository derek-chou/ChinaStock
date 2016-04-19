import redis
import datetime
import time
# import sys
# from rdbtools import RdbParser, RdbCallback

r = redis.StrictRedis(host='127.0.0.1', port=6379, db=0, password="apex@tw")
#while True:
rdb_1 = r.execute_command('psync ? -1')
print datetime.datetime.now(), rdb_1

rdb_2 = r.execute_command('psync ? -1')
print datetime.datetime.now(), type(rdb_2), len(rdb_2)

# print rdb
# for line in rdb:
# 	print line

