# -*- coding:utf-8 -*-
__author__ = 'Jackie'

import shutil
import os
from socket import *
import math
import logging
import pickle
import threading
import time
import datetime
from utils import constant as CONS

class Filemonitor:
	#initialize the monitor
	def __init__(self, port):
		self.ip = gethostbyname(gethostname())
		self.port = port
		self.aliveServerData = []#活着的服务器信息
		self.fileInfo = [] #上传文件的文件名和block大小

		logging.info('monitor running at {0:s}:{1:4d}'.format(self.ip, self.port))
		try:
			self.monitor = socket(AF_INET, SOCK_STREAM)
			self.monitor.bind(('', self.port))
			self.monitor.listen(20) # Max connections
			logging.info('monitor start successfully...')
		except Exception as e:
			logging.warning("socket create fail{exec}".format(exce=e))

		#开启服务心跳检测
		thread = threading.Thread(target=self.checkServerStatus, args=())
		thread.start()

		#start to listen port
		self.waitConnection()

	def waitConnection(self):
		while True:
			try:
				conn, addr = self.monitor.accept()
				logging.info('Connection from {address} connected!'.format(address=addr))

				helloMsg = conn.recv(1024)
				dicData = pickle.loads(helloMsg)
				if 'msgType' in dicData:
					msgType = dicData['msgType']
					print("Message type received:", msgType)
					if msgType == CONS.from_client_03: #上传
						self.sendServerUploadInfoToClient(dicData, conn)
					elif msgType == CONS.from_client_04: #下载
						self.sendServerDownloadInfoToClient(dicData, conn)
					elif msgType == CONS.from_server_01: #服务器创建状态
						self.initServerInfo(dicData)
						conn.close()
					elif msgType == CONS.from_server_02: #服务器运行状态
						thread = threading.Thread(target=self.refreshServerInfor, args=(dicData, conn))
						thread.start()
				else:
					logging.info("can't get message type")
					continue
			except Exception as e:
				logging.error('monitor fail.{0}'.format(e))
			finally:
				conn.close()

	#向客户端返回上传服务器信息
	def sendServerUploadInfoToClient(self, dicData, client):
		if 'blockSize' in dicData and 'fileName' in dicData:
			aliveLength = len(self.aliveServerData) #只考虑为四台或三台
		else:
			logging.warning("can not get necessary information(blocksize) form client")
			return -1

		#文件名不能已经存在
		for fileInfo in self.fileInfo:
			if fileInfo["fileName"] == dicData['fileName']:
				logging.warning("上传文件已存在，请检查文件名")
				return -1

		# 两台服务器直接接受客户端文件  and 一台或两台通过服务器备份
		firstServerFileEndSize = math.ceil(dicData['blockSize']/2)

		self.aliveServerData[0]['blockStart'] = 1
		self.aliveServerData[0]['blockEnd'] = firstServerFileEndSize
		self.aliveServerData[1]['blockStart'] = firstServerFileEndSize + 1
		self.aliveServerData[1]['blockEnd'] = dicData['blockSize']

		if aliveLength == 3: #上传时就已经损坏一台
			self.aliveServerData[2]['blockStart'] = 1
			self.aliveServerData[2]['blockEnd'] = dicData['blockSize']
		elif aliveLength == 4:
			self.aliveServerData[2]['blockStart'] = 1
			self.aliveServerData[2]['blockEnd'] = firstServerFileEndSize
			self.aliveServerData[3]['blockStart'] = firstServerFileEndSize + 1
			self.aliveServerData[3]['blockEnd'] = dicData['blockSize']
		else:
			logging.warning("the server number is not right")

		fileInfo = dict()#保存文件信息
		fileInfo['fileName'] = dicData['fileName']
		fileInfo['blockSize'] = dicData['blockSize']
		fileInfo["serverPosition"] = []
		for server in self.aliveServerData:
			tempPostionInfo = dict()
			tempPostionInfo["ip"] = server["ip"]
			tempPostionInfo["port"] = server["port"]
			tempPostionInfo["blockStart"] = server["blockStart"]
			tempPostionInfo["blockEnd"] = server["blockEnd"]
			fileInfo["serverPosition"].append(tempPostionInfo)

		self.fileInfo.append(fileInfo)

		packetData = pickle.dumps(self.aliveServerData)
		client.send(packetData)
		client.close()
		logging.info("send upload file information to client: '{0}' to monitor".format(pickle.unpack(packetData)))

	#向客户端返回下载服务器信息
	def sendServerDownloadInfoToClient(self, dicData, client):
		fileName = dicData['fileName']
		fileExist = False
		fileServerInfor = []
		for fileInfo in self.fileInfo:
			if fileInfo["fileName"] == fileName:
				fileExist = True
				blockSize = fileInfo["blockSize"]
				#返回两个server供下载
				for serverPosition in fileInfo["serverPosition"]:
					if serverPosition["blockEnd"] < blockSize:
						fileServerInfor.append(dict(serverPosition))
						break
				for serverPosition in fileInfo["serverPosition"]:
					if serverPosition["blockEnd"] == blockSize:
						fileServerInfor.append(dict(serverPosition))
						break
			else:
				logging.warning("file information is not right!!")
		if not fileExist:
			logging.warning("No such file exists!")
		if len(fileServerInfor) < 2:
			logging.warning("alive server is not enough!")
		packetData = pickle.dumps(fileServerInfor)
		client.send(packetData)
		client.close()
		logging.info("send download file information to client: '{0}' to monitor".format(pickle.unpack(packetData)))

	# 服务器信息初始化
	def initServerInfo(self, dicData):
		if 'ip' in dicData and 'port' in dicData:
			serverDic = dict()
			serverDic['ip'] = dicData['ip']
			serverDic['port'] = dicData['port']
			serverDic['blockStart'] = 0
			serverDic['blockEnd'] = 0
			serverDic['heartTime'] = datetime.datetime.now()
			self.aliveServerData.append(serverDic)
		else:
			logging.warning('Can not get server connection necessary info')

	#检查服务器状态
	def checkServerStatus(self):
		while True:
			time.sleep(2.5)
			now = datetime.datetime.now()
			for each in self.aliveServerData[:]:
				heartTime = each['heartTime']
				if (now - heartTime).seconds >= 5:
					logging.info("server{0}:{1} refresh fail！".format(each["ip"], str(each["port"])))
					self.aliveServerData.remove(each)
					for fileInfo in self.fileInfo:
						for serverPosition in fileInfo["serverPosition"][:]:
							if serverPosition["ip"] == each["ip"] and serverPosition["port"] == each["port"]:
								fileInfo["serverPosition"].remove(serverPosition)

	# 刷新服务器信息
	def refreshServerInfor(self, dicData, client):
		client.close()
		if 'ip' in dicData and 'port' in dicData:
			ip = dicData['ip']
			port = dicData['port']

			now = datetime.datetime.now()
			for each in self.aliveServerData[:]:
				if each["ip"] == ip and each["port"] == port:
					each["heartTime"] = now
					logging.info("server{0}:{1} refreshed！".format(each["ip"], str(each["port"])))
		else:
			logging.warning('更新消息有误！')

# initialize the log
def initLog(logName):
	# createa log folder
	if not os.path.isdir('log'):
		os.mkdir('log')
	logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M', filename='log/' + logName)
	# define a Handler which writes INFO messages or higher to the sys.stderr
	console = logging.StreamHandler()
	console.setLevel(logging.INFO)
	# set a format which is simpler for console use
	formatter = logging.Formatter('%(levelname)-6s:%(message)s')
	# tell the handler to use this format
	console.setFormatter(formatter)
	# add the handler to the root logger
	logging.getLogger('').addHandler(console)

if __name__ == "__main__":
	initLog('monitor.log')
	monitor = Filemonitor(CONS.PORT_START)





























