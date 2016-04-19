# -*- coding: utf8 -*-

import sys
import getopt
# from dbfpy import dbf as dbf_py
import os.path
import collections
import datetime
import dbf
import redis
import codecs
# import chardet
import threading
import logging
import logging.config
import signal
import time
import thread

logger = None
dbfFileHandle = None
dbfFileIndex = None

class RedisSubThread(threading.Thread):
	def __init__(self, r, channels, filePattern, fullFilePath):
		threading.Thread.__init__(self)
		self.redis = r
		self.subChannels = channels
		self.filePattern = filePattern
		self.fullFilePath = fullFilePath
		self.pubsub = self.redis.pubsub()
		self.subQueue = {}
		self.stopFlag = False
		self.dtBatchWriteFlag = datetime.datetime.now()
		self.lock = thread.allocate_lock()

	def insertQueue(self, item):
		# print(item["channel"], ":", item["data"])

		if self.stopFlag:
			return

		try:
			strData = str(item["data"])
			if len(strData) < 10:
				return
			vList = strData.split("##")

			key = vList[0]
			vList.pop(0)
			value = "##".join(vList)

			self.lock.acquire()
			self.subQueue[key] = value
			self.lock.release()
		except:
			logger.info("thread insertQueue error" + str(sys.exc_info()[0]))

	def getQueue(self):
		self.lock.acquire()
		queue = self.subQueue
		self.subQueue = {}
		self.lock.release()
		return queue

	def run(self):
		#避免斷線重連後接收不到tick
		#self.pubsub = self.redis.pubsub()
		self.pubsub.subscribe(self.subChannels)
		while not self.stopFlag:
			try:
				msg = self.pubsub.get_message()
				if msg:
					self.insertQueue(msg)
				else:
					time.sleep(0.001)
			except:
				logger.error("RedisSubThread run error " + str(sys.exc_info()[0]))
				time.sleep(1)
			# 	self.pubsub.subscribe(self.subChannels)

	def stop(self):
		self.stopFlag = True

def main():
	global logger

	dicRead = {}
	bReadDBF = False
	bWriteDBF = False
	bRedisHost = False
	readFileName = ""
	writeFileName = ""
	redisHost = ""
	redisDB = 0
	filePattern = None
	redisServer = None	
	dtFullWriteFlag = None
	redisSubThread = None

	#創建log目錄
	if not os.path.isdir(os.getcwd()+"/log"):
		os.makedirs(os.getcwd() + "/log")

	logging.config.fileConfig("logger.conf")
	logger = logging.getLogger("group1")

	def signal_handler(signal, frame):
		global logger

		logger.info("pressed Ctrl+C")
		if redisSubThread != None:
			redisSubThread.stop()
		sys.exit(1)

	signal.signal(signal.SIGINT, signal_handler)

	opts, args = getArguments()
	if opts == None:
		usage()
		sys.exit(1)
	for opt,arg in opts:  
		if opt in ("--readDBF"):  
			bReadDBF = True
			readFileName = arg
		elif opt in ("--writeDBF"):  
			bWriteDBF = True
			writeFileName = arg
		elif opt in ("--redisHost"):
			bRedisHost = True
			redisHost = arg
		elif opt in ("--redisDB"):
			redisDB = arg
		elif opt in ("--filePattern"):
			filePattern = arg

	if filePattern == None:
		usage()
		return

	if bRedisHost:
		if redisServer == None:
			redisServer = redis.StrictRedis(host=redisHost, port=6379, db=redisDB, password="apex@tw")
		try:
			logger.info("test redis connection")
			redisServer.execute_command('ping')
			logger.info("redis connected")
		except:
			logger.error("redis can't connect")
			sys.exit(0)
	logger.info("system start")


	while True:
	# if True:
		# logger.debug("\n******************************************************************************")
		# logger.debug("loop start")
		if bWriteDBF:
			#初始化subscribe thread
			if not redisSubThread and redisServer:
				if filePattern == "0":
					redisSubThread = RedisSubThread(redisServer, ['show2003'], filePattern, writeFileName)
				elif filePattern == "1":
					redisSubThread = RedisSubThread(redisServer, ['sjshq'], filePattern, writeFileName)
				logger.info("redis sub thread start")
				redisSubThread.start()

			#約五分鐘進行一次全DBF更新
			if dtFullWriteFlag == None or (datetime.datetime.now() - dtFullWriteFlag).seconds > 60 * 5:
				dtFullWriteFlag = datetime.datetime.now()
				logger.info("DBF full rewrite")

				dtReadRedisStart = datetime.datetime.now()
				logger.debug("read Redis start")
				dicRead = readRedis(redisServer)
				dtReadRedisEnd = datetime.datetime.now()
				logger.debug("read Redis end (" + str(dtReadRedisEnd - dtReadRedisStart) + ")")

				dicRead = collections.OrderedDict(sorted(dicRead.items()))
				writeDBF(filePattern, writeFileName, dicRead)

			if dtFullWriteFlag != None:
				dtNow = datetime.datetime.now()
				deltaSeconds = (dtNow - dtFullWriteFlag).seconds * 1000 * 1000
				deltaMicroSeconds = (dtNow - dtFullWriteFlag).microseconds + deltaSeconds
			#約200ms進行一次DBF更新
			if dtFullWriteFlag == None or deltaMicroSeconds > 200 * 1000:
				dtFullWriteFlag = datetime.datetime.now()
				queue = redisSubThread.getQueue()
				if len(queue) > 0:
					logger.info("DBF partial update")
					writeDBF(filePattern, writeFileName, queue)
				else:
					time.sleep(0.1)

		if bReadDBF:
			dtReadDBFStart = datetime.datetime.now()
			logger.debug("read DBF start")
			dicRead = readDBF(filePattern, readFileName, redisServer)
			dtReadDBFEnd = datetime.datetime.now()
			logger.debug("read DBF end (" + str(dtReadDBFEnd - dtReadDBFStart) + ")")

		# logger.debug("loop end\n")
		# logger.debug("\n******************************************************************************\n")

def readRedis(redisServer):
	dicResult = {}
	if redisServer == None:
		return dicResult

	try:
		keys = redisServer.keys('*')
		if len(keys) == 0:
			return dicResult

		values = redisServer.mget(keys)
		for i in range(len(keys)):
			dicResult[keys[i]] = values[i]
	except:
		logger.error("readRedis error " + str(sys.exc_info()[0]))
		return dicResult

	return dicResult

# writeMax = 0
def writeDBF(filePattern, fullFilePath, dicInput):
	global dbfFileHandle
	global dbfFileIndex
	global writeMax
	# dbfFileHandle = None
	# dbfFileIndex = None

	def createIndex(fileHandle):
		global dbfFileIndex

		with fileHandle:
			logger.info("create file index start")
			if filePattern == "0":
				dbfFileIndex = fileHandle.create_index(lambda rec: rec.s1)
				# print datetime.datetime.now(), "idx=", prod_idx
			elif filePattern == "1":
				dbfFileIndex = fileHandle.create_index(lambda rec: rec.hqzqdm)
			logger.info("create file index end")

	insertCount = 0; updateCount = 0;
	bFileExists = os.path.exists(fullFilePath) 

	dtWriteDBFStart = datetime.datetime.now()
	# logger.debug("write DBF start")

	# print dbfFileHandle
	if dbfFileHandle == None:
		if bFileExists == True:
			dbfFileHandle = dbf.Table(fullFilePath, codepage="utf8")
		else:
			if filePattern == "0": #show2003.dbf
				dbfFileHandle = dbf.Table(fullFilePath, "S1 C(6); S2 C(18); S3 N(10,3); S4 N(10,3); S5 N(12,0); S6 N(12,0); "
					"S7 N(10,3); S8 N(10,3); S9 N(10,3); S10 N(10,3); S11 N(10,0); S13 N(10,3); S15 N(10,0); S16 N(10,3); "
					"S17 N(10,0); S18 N(10,3); S19 N(10,0); S21 N(10,0); S22 N(10,3); S23 N(10,0); S24 N(10,3); S25 N(10,0); "
					"S26 N(10,3); S27 N(10,0); S28 N(10,3); S29 N(10,0); S30 N(10,3); S31 N(10,0); S32 N(10,3); S33 N(10,0)"
					, codepage="utf8")
			elif filePattern == "1": #sjshq.dbf
				dbfFileHandle = dbf.Table(fullFilePath, "HQZQDM C(6); HQZQJC C(18); HQZRSP N(9,3); HQJRKP N(9,3); HQZJCJ N(9,3); "
					"HQCJSL N(12,0); HQCJJE N(17,3); HQCJBS N(9,0); HQZGCJ N(9,3); HQZDCJ N(9,3); HQSYL1 N(7,2); HQSYL2 N(7,2); "
					"HQJSD1 N(9,3); HQJSD2 N(9,3); HQHYCC N(12,0); HQSJW5 N(9,3); HQSSL5 N(12,0); HQSJW4 N(9,3); HQSSL4 N(12,0); "
					"HQSJW3 N(9,3); HQSSL3 N(12,0); HQSJW2 N(9,3); HQSSL2 N(12,0); HQSJW1 N(9,3); HQSSL1 N(12,0); HQBJW1 N(9,3); "
					"HQBSL1 N(12,0); HQBJW2 N(9,3); HQBSL2 N(12,0); HQBJW3 N(9,3); HQBSL3 N(12,0); HQBJW4 N(9,3); HQBSL4 N(12,0); "
					"HQBJW5 N(9,3); HQBSL5 N(12,0);"
					, codepage="utf8")
		# dbfFileHandle.open()

	if dbfFileIndex == None:
		createIndex(dbfFileHandle)

	# print dbfFileHandle
	with dbfFileHandle:
	# if True:
		# print datetime.datetime.now(), "input len1 = ", len(dicInput), "filePattern=", filePattern

		# print datetime.datetime.now(), "input len2 = ", len(dicInput)
		for key, value in dicInput.iteritems():
			# print key, value
			if value == None:
				continue
			vList = value.split("##")
			vListLen = len(vList)
			if filePattern == "0":
				if vListLen < 29:
					continue
			elif filePattern == "1":
				if vListLen < 34:
					continue

			match = dbfFileIndex.search(match=key)
			# logger.debug("check match")

			if len(match) == 0:
				insertCount += 1
				vList[0] = vList[0].decode("utf8")
				vList.insert(0, key)

				if filePattern == "0":
					if vListLen > 30:
						# del vList[-1]
						vList[30, -1] = []
				elif filePattern == "1":
					if vListLen > 35:
						# del vList[-1]
						vList[35, -1] = []

				dbfFileHandle.append(tuple(vList))
			else:
				updateCount += 1
				with match[0] as rec:
					if filePattern == "0":
						rec.s2 = vList[0].decode("utf8")
						#rec.s2 = vList[0]
						rec.s3 = vList[1]; rec.s4 = vList[2]; rec.s5 = vList[3];
						rec.s6 = vList[4]; rec.s7 = vList[5]; rec.s8 = vList[6];
						rec.s9 = vList[7]; rec.s10 = vList[8]; rec.s11 = vList[9];
						rec.s13 = vList[10]; rec.s15 = vList[11]; rec.s16 = vList[12];
						rec.s17 = vList[13]; rec.s18 = vList[14]; rec.s19 = vList[15];
						rec.s21 = vList[16]; rec.s22 = vList[17]; rec.s23 = vList[18];
						rec.s24 = vList[19]; rec.s25 = vList[20]; rec.s26 = vList[21];
						rec.s27 = vList[22]; rec.s28 = vList[23]; rec.s29 = vList[24];
						rec.s30 = vList[25]; rec.s31 = vList[26]; rec.s32 = vList[27];
						rec.s33 = vList[28];
					elif filePattern == "1":
						rec.hqzqjc = vList[0].decode("utf8")
						# rec.hqzqjc = vList[0]
						rec.hqzrsp = vList[1]; rec.hqjrkp = vList[2]; rec.hqzjcj = vList[3];
						rec.hqcjsl = vList[4]; rec.hqcjje = vList[5]; rec.hqcjbs = vList[6];
						rec.hqzgcj = vList[7]; rec.hqzdcj = vList[8]; rec.hqsyl1 = vList[9];
						rec.hqsyl2 = vList[10]; rec.hqjsd1 = vList[11]; rec.hqjsd2 = vList[12];
						rec.hqhycc = vList[13]; rec.hqsjw5 = vList[14]; rec.hqssl5 = vList[15];
						rec.hqsjw4 = vList[16]; rec.hqssl4 = vList[17]; rec.hqsjw3 = vList[18];
						rec.hqssl3 = vList[19]; rec.hqsjw2 = vList[20]; rec.hqssl2 = vList[21];
						rec.hqsjw1 = vList[22]; rec.hqssl1 = vList[23]; rec.hqbjw1 = vList[24];
						rec.hqbsl1 = vList[25]; rec.hqbjw2 = vList[26]; rec.hqbsl2 = vList[27];
						rec.hqbjw3 = vList[28]; rec.hqbsl3 = vList[29]; rec.hqbjw4 = vList[30];
						rec.hqbsl4 = vList[31]; rec.hqbjw5 = vList[32]; rec.hqbsl5 = vList[33];

	dtWriteDBFEnd = datetime.datetime.now()

	#有新增的record時，則更新index
	if insertCount > 0:
		createIndex(dbfFileHandle)

	# dtDelta = (dtWriteDBFEnd - dtWriteDBFStart).microseconds
	# print writeMax
	# if dtDelta > writeMax or dtDelta > 10 * 1000:
	# writeMax = dtDelta
	logger.debug("write count : " + str(insertCount) + "/" + str(updateCount))
	logger.debug("write DBF end (" + str(dtWriteDBFEnd - dtWriteDBFStart) + ")")
	# dbfFileHandle.close()
	# dbfFileHandle = None

def readDBF(filePattern, fullFilePath, redisServer):
	dbfObj = dbf.Table(fullFilePath, codepage="cp936")
	# dbfObj.open()
	# logger.debug('open DBF file complete')
	currKey = ""; currValue = ""; pubChannel = ""

	if filePattern == "0":
		pubChannel = "show2003"
	elif filePattern == "1":
		pubChannel = "sjshq"

	recCount = 0
	keyList = []
	valueList = []
	publishCount = 0

	def redisHandle(keyList, valueList):
		count = 0
		if redisServer != None:
			try:
				lastValueList = redisServer.mget(keyList)
				#key不存在或與最後一筆不相符則更新
				with redisServer.pipeline() as pipe:
					for i in range(len(keyList)):
						if lastValueList[i] == None or lastValueList[i].decode("utf8") != valueList[i]:
						# if True:
							pipe.set(keyList[i], valueList[i])
							pipe.publish(pubChannel, keyList[i]+"##"+valueList[i])
							# print ret
							count += 1
					pipe.execute()
			except:
				logger.error("redis handle error" + str(sys.exc_info()[0]))
		return count

	with dbfObj:
	# dbfObj.open(mode='read-only')
	# if True:
		# recList = [ rec for rec in dbfObj]
		# for rec in recList:
		# print dbfObj
		for rec in dbfObj:
			def getStrFieldValue(fieldName, encoding):
				result = ""
				try:
					result = rec[fieldName]
				except:
					result = rec[fieldName].decode(encoding).encode("utf8")
				return result

			try:
				if filePattern == "0":
					# print rec
					currKey = rec.s1
					#920ms
					# 三種取出欄位值的方式(rec["s3"], rec[2], rec.s3)，以rec.s3的效率最佳
					# currValue = (u"%s##%.3f##%.3f##%d##%.3f##%.3f##%.3f##%.3f##%.3f##%d##%.3f##%d"
					# 		"##%.3f##%d##%.3f##%d##%d##%.3f##%d##%.3f##%d##%.3f##%d##%.3f##%d"
					# 		"##%.3f##%d##%.3f##%d") % (getStrFieldValue("s2", "cp936"),
					# 	rec.s3, rec.s4, rec.s5, rec.s6, rec.s7, rec.s8, rec.s9, rec.s10, 
					# 	rec.s11, rec.s13, rec.s15, rec.s16, rec.s17, rec.s18, rec.s19, rec.s21, 
					# 	rec.s22, rec.s23, rec.s24, rec.s25, rec.s26, rec.s27, rec.s28, rec.s29, 
					# 	rec.s30, rec.s31, rec.s32, rec.s33)
					# print currValue

					# print repr(rec)
					recList = repr(rec).split()
					del recList[0]
					if len(recList) == 27:
						recList.insert(9, "0.000")
					currValue = "##".join(recList)
					currValue = (u"%s##%s") % (getStrFieldValue("s2", "cp936"), currValue)
					# print currValue
				elif filePattern == "1":
					currKey = rec.hqzqdm
					# currValue = (u"%s##%.3f##%.3f##%.3f"
					# 	"##%d##%.3f##%d##%.3f##%.3f##%.2f##%.2f##%.3f##%.3f##%d##%.3f"
					# 	"##%d##%.3f##%d##%.3f##%d##%.3f##%d##%.3f##%d##%.3f##%d##%.3f"
					# 	"##%d##%.3f##%d##%.3f##%d##%.3f##%d") % (getStrFieldValue("hqzqjc", "cp936"), 
					# 	rec.hqzrsp, rec.hqjrkp, rec.hqzjcj, rec.hqcjsl, rec.hqcjje, rec.hqcjbs, 
					# 	rec.hqzgcj, rec.hqzdcj, rec.hqsyl1, rec.hqsyl2, rec.hqjsd1, rec.hqjsd2, 
					# 	rec.hqhycc, rec.hqsjw5, rec.hqssl5, rec.hqsjw4, rec.hqssl4, rec.hqsjw3, 
					# 	rec.hqssl3, rec.hqsjw2, rec.hqssl2, rec.hqsjw1, rec.hqssl1, rec.hqbjw1, 
					# 	rec.hqbsl1, rec.hqbjw2, rec.hqbsl2, rec.hqbjw3, rec.hqbsl3, rec.hqbjw4, 
					# 	rec.hqbsl4, rec.hqbjw5, rec.hqbsl5)

					recList = repr(rec).split()
					del recList[0]
					currValue = "##".join(recList)
					# currValue = (u"%s##%s") % (getStrFieldValue("hqzqjc", "cp936"), currValue)
			except:
				logger.error("get record fail " + str(sys.exc_info()[0]) + " key = " + currKey)

			keyList.append(currKey)
			valueList.append(currValue)
			recCount += 1

			#每100筆rec一併處理
			if recCount % 100 == 0:
				publishCount += redisHandle(keyList, valueList)
				keyList = []
				valueList = []

	#處理最後剩餘的rec
	publishCount += redisHandle(keyList, valueList)
	logger.info("read rec. count " + str(recCount))
	logger.info("publish count " + str(publishCount))
	# dbfObj.close()

def usage():
	print("Usage:%s [--filePattern|--readDBF|--writeDBF|--redisHost|--redisDB]" %sys.argv[0]);

def getArguments():
	global logger

	try:
		opts,args = getopt.getopt(sys.argv[1:], "", ["help", "filePattern=", "readDBF=", "writeDBF=", 
			"redisHost=", "redisDB="])
		for opt,arg in opts:  
			if opt in ("-h", "--help"):  
				usage()
				sys.exit(1)
	except getopt.GetoptError:  
		return (None, None)

	return (opts, args)

if "__main__" == __name__: 
	main()
