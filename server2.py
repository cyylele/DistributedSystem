# -*- coding:utf-8 -*-
__author__ = 'Jackie'

import shutil
import os
from socket import *
import logging
import pickle
import threading
import datetime
import time
from utils import constant as CONS

class FileServer:
	#initialize the server
	def __init__(self, port):
		self.ip = gethostbyname(gethostname())
		self.port = port
		self.threads = []
		logging.info('Server running at {0:s}:{1:4d}'.format(self.ip, self.port))
		try:
			self.server = socket(AF_INET, SOCK_STREAM)
			self.server.bind(('', self.port))
			self.server.listen(20) # Max connections
			logging.info('Server start successfully...')
		except Exception as e:
			logging.warning("Socket create fail.{0}".format(e)) #socket create fail

		# notify the monitor
		self.notifyConnect()

		# 更新服务器状态线程开启
		thread = threading.Thread(target=self.notityAlive, args=())
		thread.start()

		#start to listen port
		self.waitConnection()


	def notifyConnect(self):
		try:
			socketConnect = socket(AF_INET, SOCK_STREAM)
			socketConnect.connect((CONS.MONITOR__SERVER_IP, CONS.MONITOR__SERVER_PORT))

			packetDataDic = dict()
			packetDataDic['msgType'] = CONS.from_server_01
			packetDataDic['ip'] = self.ip
			packetDataDic['port'] = self.port
			packetData = pickle.dumps(packetDataDic)

			socketConnect.send(packetData)
			logging.info("Notify the monitor")
			# logging.info("send server info: '{0}' to monitor".format(packetDataDic))
		except Exception as e:
			socketConnect.close()
			logging.error('Connect to monitor fail! {0}'.format(e))

	def waitConnection(self):
		upLoadTime = -1
		downLoadTime = -1
		while True:
			try:
				conn, addr = self.server.accept()
				logging.info('Connection from {address} connected!'.format(address=addr))

				msgDic = conn.recv(CONS.HEAD_FILE_LENGTH)
				# print("msgDic length:{1}", len(msgDic))
				dicData = pickle.loads(msgDic)
				if 'msgType' in dicData and 'fileName'in dicData:
					msgType = dicData['msgType']
					print("Message type received:", msgType)
					if msgType == CONS.from_client_01: #上传与备份同一个方法处理
						threads = []
						if upLoadTime == -1:
							upLoadTime = 1
							starttime = datetime.datetime.now()  #统计服务器从开始接收文件到结束文件上传时间
							logging.info('服务器开始接收上传文件时间：{0}'.format(starttime))

						thread = threading.Thread(target=self.fromClientUploadmsgHandler,
												  args=(conn, dicData))#开启进程处理文件上传

						self.threads.append(thread)
						thread.start()
						for t in self.threads:
							t.join()

						endtime = datetime.datetime.now()
						logging.info('服务器结束上传文件时间：{0}'.format(endtime))
						logging.info('当前所用时间：{0}'.format(endtime - starttime))
					elif msgType == CONS.from_client_02: #下载
						self.threads = []
						if downLoadTime == -1:
							upLoadTime = 1
							starttime = datetime.datetime.now()  #统计服务器从开始下载文件到结束文件下载时间
							logging.info('服务器开始发送下载文件：{0}'.format(starttime))

						thread = threading.Thread(target=self.fromClientDownloadmsgHandler,
												  args=(conn, dicData))#开启进程处理文件下载
						self.threads.append(thread)

						thread.start()
						for t in self.threads:
							t.join()

						endtime = datetime.datetime.now()
						logging.info('服务器结束发送下载文件：{0}'.format(endtime))
					else:
						conn.close()
						logging.info('message type error')

			except Exception as e:
				logging.error('connect to with client error.{0}'.format(e))

	def fromClientUploadmsgHandler(self, client, dicData = []):
		#back file list
		if 'fileName'in dicData:
			fileName = dicData['fileName'].rstrip()
			backServerIP = dicData["backServerIP"].rstrip()
			if backServerIP:
				try:
					serverIP = backServerIP
					serverPort = dicData["backServerPort"].rstrip()
					logging.info("发送备份到{0} {1}".format(serverIP, serverPort))
					dicData["backServerIP"] = " " * CONS.SERVER_IP_LENGTH
					dicData["backServerPort"] = " " * CONS.SERVER_PORT_LENGTH

					backServerConnect = socket(AF_INET, SOCK_STREAM)
					backServerConnect.connect((serverIP, int(serverPort)))

					packetData = pickle.dumps(dicData)
					#传递上传文件基本信息
					# print("packetData length:", len(packetData))
					backServerConnect.send(packetData)

				except Exception as e: #不考虑上传失败
					backServerConnect.close()
					logging.error("Send back file:{0} to server:{1} fail! {2}".format(fileName, serverIP, e))
			else:
				logging.info("接收客户端文件{0}".format(fileName))

			#read and write together
			# fileSizeCount = 0
			if not os.path.isdir('data-{0}'.format(self.port)):
				os.mkdir('data-{0}'.format(self.port))
			with open('data-{0}/{1}'.format(self.port, fileName), 'wb') as f:
				while True:
					revContent = client.recv(CONS.ONCE_READ_FILE_SIZE)
					if backServerIP:
						backServerConnect.send(revContent)
					# fileSizeCount = fileSizeCount + len(revContent)
					if not revContent:
						logging.info("Write file {0} finished, and file size:{1}".format(fileName, f.tell()))
						break
					f.write(revContent)

			client.close()
		else:
			logging.info('message error')

	def fromClientDownloadmsgHandler(self, client, dicData = []):
		if 'fileName'in dicData:
			fileName = dicData['fileName'].rstrip()

			# fileSizeCount = 0
			with open('data-{0}/{1}'.format(self.port, fileName), 'rb') as f:
				for i in range(int(CONS.BLOCK_SIZE/CONS.ONCE_READ_FILE_SIZE)):
					content = f.read(CONS.ONCE_READ_FILE_SIZE)
					# fileSizeCount = fileSizeCount + len(content)
					if not content:
						logging.info("Send download file {0} finished, and file size:{1}".format(fileName, f.tell()))
						break
					client.send(content)
			# print("length of file send", fileSizeCount)
			client.close()
		else:
			client.close()
			logging.info('Message error!')

	# visite the monitor continuously
	def notityAlive(self):
		while True:
			time.sleep(2)
			try:
				socketConnect = socket(AF_INET, SOCK_STREAM)
				socketConnect.connect((CONS.MONITOR__SERVER_IP, CONS.MONITOR__SERVER_PORT))

				packetDataDic = dict()
				packetDataDic['msgType'] = CONS.from_server_02
				packetDataDic['ip'] = self.ip
				packetDataDic['port'] = self.port
				packetData = pickle.dumps(packetDataDic)

				socketConnect.send(packetData)
				# logging.info("Notify the monitor")
				socketConnect.close()
				# logging.info("send server info: '{0}' to monitor".format(packetDataDic))
			except Exception as e:
				socketConnect.close()
				logging.error('Connect to monitor fail! {0}'.format(e))


	def destroy(self):
		self.server.close()

# initialize the log
def initLog(logName):
	# createa log folder
	if not os.path.isdir('log'):
		os.mkdir('log')
	logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M', filename='log/' + logName)
	console = logging.StreamHandler()
	console.setLevel(logging.INFO)
	formatter = logging.Formatter('%(levelname)-6s:%(message)s')
	console.setFormatter(formatter)
	logging.getLogger('').addHandler(console)

if __name__ == "__main__":
	initLog('server2.log')
	server = FileServer(CONS.PORT_START + 2)

