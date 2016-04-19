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
from hanziconv import HanziConv

logger = None
dbfFileHandle = None
dbfFileIndex = None

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
	redisSub = None

	#創建log目錄
	if not os.path.isdir(os.getcwd()+"/log"):
		os.makedirs(os.getcwd() + "/log")

	logging.config.fileConfig("logger.conf")
	logger = logging.getLogger("group1")

	def signal_handler(signal, frame):
		global logger

		logger.info("pressed Ctrl+C")
		if redisSub != None:
			redisSub.stop()
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
		elif opt in ("--filePattern"):
			filePattern = arg

	if filePattern == None:
		usage()
		return

	logger.info("system start")

	if bReadDBF:
		dtReadDBFStart = datetime.datetime.now()
		logger.debug("read DBF start")
		dicRead = readDBF(filePattern, readFileName, redisServer)
		dtReadDBFEnd = datetime.datetime.now()
		logger.debug("read DBF end (" + str(dtReadDBFEnd - dtReadDBFStart) + ")")

		dicRead = collections.OrderedDict(sorted(dicRead.items()))
		writeDBF(filePattern, writeFileName, dicRead)

# writeMax = 0
def writeDBF(filePattern, fullFilePath, dicInput):
	global dbfFileHandle
	global dbfFileIndex
	global writeMax
	# dbfFileHandle = None
	# dbfFileIndex = None

	insertCount = 0; updateCount = 0;
	bFileExists = os.path.exists(fullFilePath) 

	dtWriteDBFStart = datetime.datetime.now()
	# logger.debug("write DBF start")
	today = dtWriteDBFStart.strftime("%Y%m%d")
	fileName = today
	strToken = ""
	if filePattern == "0":
		strToken = "SH"
		fileName += ".SH.txt"
	elif filePattern == "1":
		strToken = "SZ"
		fileName += ".SZ.txt"

	with open(fileName, "w") as text_file:
		for key, value in dicInput.iteritems():
			insertCount += 1

			value = HanziConv.toTraditional(value)
			try:
				value = value.decode("utf8")
			except:
				pass

			strWrite = (u"%s.%s,%s\n" % (key, strToken, value))
			text_file.write(strWrite.encode('utf8'))

	dtWriteDBFEnd = datetime.datetime.now()

	logger.debug("write count : " + str(insertCount) + "/" + str(updateCount))
	logger.debug("write DBF end (" + str(dtWriteDBFEnd - dtWriteDBFStart) + ")")

def readDBF(filePattern, fullFilePath, redisServer):
	dbfObj = dbf.Table(fullFilePath, codepage="cp936")
	# dbfObj.open()
	# logger.debug('open DBF file complete')
	dicResult = {}
	currKey = ""; currValue = ""; pubChannel = ""

	if filePattern == "0":
		pubChannel = "show2003"
	elif filePattern == "1":
		pubChannel = "sjshq"

	recCount = 0
	with dbfObj:
		for rec in dbfObj:
			def getStrFieldValue(fieldName, encoding):
				result = ""
				try:
					result = rec[fieldName]
				except:
					result = rec[fieldName].decode(encoding).encode("utf8")
				return result

			# try:
			if filePattern == "0":
				currKey = rec["s1"]

				currValue = (u"%s") % (getStrFieldValue("s2", "cp936"))
			elif filePattern == "1":
				currKey = rec["hqzqdm"]
				currValue = (u"%s") % (getStrFieldValue("hqzqjc", "cp936"))

			recCount += 1
			dicResult[currKey] = currValue
	logger.info("read rec. count " + str(recCount))

	return dicResult

def usage():
	print("Usage:%s [--filePattern|--readDBF]" %sys.argv[0]);

def getArguments():
	global logger

	try:
		opts,args = getopt.getopt(sys.argv[1:], "", ["help", "filePattern=", "readDBF="])
		for opt,arg in opts:  
			if opt in ("-h", "--help"):  
				usage()
				sys.exit(1)
	except getopt.GetoptError:  
		return (None, None)

	return (opts, args)

if "__main__" == __name__: 
	main()
