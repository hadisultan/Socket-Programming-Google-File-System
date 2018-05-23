import socket
import thread
import sys
import hashlib
import math
import os.path
import uu
import time

clientslist = []
serverslist = []
controller = []
files = []
filesdict = {}
string = ""
port = 0
system = 0

def requester(reqport, filename):
	sn = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
	sn.connect(('127.0.0.1',reqport))
	f = open(filename, 'r')
	data = f.read()
	f.close()
	time.sleep(0.05)
	sn.send("receive: "+filename)
	time.sleep(0.1)
	sn.send(data)		
	time.sleep(0.1)
	sn.send('///end///')
	sn.close()
		

def changehamsai(data, filename):
	sn = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
	sn.connect(('127.0.0.1',8000))
	sn.send("haha")
	time.sleep(0.2)
	msg = "hamsai: " + filename
	sn.send(msg)
	time.sleep(0.07)
	msg = str(port)
	time.sleep(0.07)
	sn.send(msg)
	portstr = sn.recv(1024)
	temp = ""
	portarr = []
	for i in range(len(portstr)):
		if portstr[i] == "/":
			portarr.append(int(temp))
			temp = ""
		else:
			temp = temp + portstr[i]
	for i in portarr:
		v = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
		v.connect(('127.0.0.1', i))
		v.send("receive: "+filename)
		time.sleep(0.1)
		v.send(data)		
		time.sleep(0.1)
		v.send('///end///')
		v.close()
	sn.close()		

def initwriter(filename, init):
	if os.path.isfile(filename) == False:
		f = open(filename, 'w')
		f.write(" ")
		f.close()
	f = open(filename, 'r')
	data = f.read()
	f.close()
	data = init + data
	if len(data)>65503:
		extra = data[65503:]
		data = data[:65503]
		initfile = ""
		for v in range(len(filename)-1, 0, -1):
			if(filename[v] == '_'):
				initfile = filename[:v]
		chunkid = ""
		for v in range(len(filename)-1, 0, -1):
			if(filename[v] == 'k'):
				chunkid = filename[v+1:]
		controller[0].send("cascade")
		time.sleep(0.15)
		controller[0].send(initfile)
		time.sleep(0.15)
		controller[0].send(chunkid)
		nport = int(controller[0].recv(1024))
		v = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
		v.connect(('127.0.0.1', nport))		
		time.sleep(0.15)
		newfname = initfile+"_chunk"+str(int(chunkid)+1)
		newmsg = "initwrite: "+newfname
		v.send(newmsg)
		time.sleep(0.15)
		v.send(extra)
		time.sleep(0.15)
		v.send('///end///')
		v.close()
	f = open(filename, 'w')
	f.write(data)
	f.close()
	filesdict[filename] = hasher(data)
	changehamsai(data, filename)
		

def writer(filename, client):
	f = open(filename, 'r')
	data = f.read()
	f.close()
	if(hasher(data) == filesdict[filename]):
		client.send('ok')
		time.sleep(0.05)
		client.send(data)
		time.sleep(0.05)
		client.send('///end///')
		offset = int(client.recv(1024))
		txt = ""
		while True:
			nstring = client.recv(1024)
			txt = txt + nstring
			if(len(nstring)>=9):
				if (nstring[len(nstring)-9:len(nstring)] == "///end///") or (nstring == "///end///"):
					txt = txt[:len(txt)-9]
					break
		data = data[:offset]+txt+data[offset:]
		if len(data)>65503:
			extra = data[65503:]
			data = data[:65503]
			initfile = ""
			for v in range(len(filename)-1, 0, -1):
				if(filename[v] == '_'):
					initfile = filename[:v]
			chunkid = ""
			for v in range(len(filename)-1, 0, -1):
				if(filename[v] == 'k'):
					chunkid = filename[v+1:]
			controller[0].send("cascade")
			time.sleep(0.15)
			controller[0].send(initfile)
			time.sleep(0.15)
			controller[0].send(chunkid)
			nport = int(controller[0].recv(1024))
			v = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
			v.connect(('127.0.0.1', nport))		
			time.sleep(0.02)
			newfname = initfile+"_chunk"+str(int(chunkid)+1)
			newmsg = "initwrite: "+newfname
			v.send(newmsg)
			time.sleep(0.02)
			v.send(extra)
			time.sleep(0.02)
			v.send('///end///')


		f = open(filename, 'w')
		f.write(data)
		f.close()
		filesdict[filename] = hasher(data)
		changehamsai(data, filename)
		client.send(data)
		time.sleep(0.02)
		client.send('///end///')
	else:
		client.send('err')


def integritychecker():
	global files
	global filesdict
	global system
	while True:
		checker = 0
		time.sleep(15)
		if sys == 1:
			continue
		for q in files:
			f = open(q, 'r')
			data = f.read()
			x = 0
			hashint = 0
			while True:
				if(x-1)*8192 > len(data):
					break
				sha1 = hashlib.sha1()
				s = data[x*8192:(x+1)*8192]
				sha1.update(s)
				hashint = hashint + int(sha1.hexdigest(), 16)
				x = x+1
			if sys == 1:
				continue
			if(hashint == filesdict[q]):
				checker = checker
			else:
				print "CORRUPTION: Checksum failed for", q
				checker = 1
		if checker == 0:
			print "Checksum done"

def hasher(data):
	x = 0
	hashstr = 0
	while True:
		if(x-1)*8192 > len(data):
			break
		sha1 = hashlib.sha1()
		s = data[x*8192:(x+1)*8192]
		sha1.update(s)
		hashstr = hashstr + int(sha1.hexdigest(), 16)
		x = x+1
	return hashstr

def sender(filename, s):
	global port
	f = open(filename, 'r')
	data = f.read()
	f.close()
	hashstr = hasher(data)
	# print "len:", len(data)
	# print "size:", sys.getsizeof(data)
	if hashstr == filesdict[filename]:
		s.send("ok")
		time.sleep(0.02)
		s.send(data)
		time.sleep(0.02)
		s.send('///end///')
	else:
		s.send("err")
		msg = "writereq: "+filename+"/"+str(port)
		controller[0].send(msg)			

def getchunk(filename, cont):
	sys = 1
	global string
	data = ""
	while True:
		string = cont.recv(1024)
		if(len(string)>=9):
			if (string[len(string)-9:len(string)] == "///end///"):
				data = data + string[:len(string)-9]
				break
		data = data + string
	file = open(filename, 'w')
	file.write(data)
	file.close()
	print filename, "written"
	# print "length:", len(data)
	x = 0
	hashstr = 0
	while True:
		if(x-1)*8192 > len(data):
			break
		sha1 = hashlib.sha1()
		s = data[x*8192:(x+1)*8192]
		sha1.update(s)
		hashstr = hashstr + int(sha1.hexdigest(), 16)
		x = x+1
	if filename not in files:
		files.append(filename)
	filesdict[filename] = hashstr
	sys = 0


def listenerthread(client):
		while True:
			string = client.recv(1024)
			print "Client said:", string
			if(string[:10] == "initwrite:"):
				filename = string[11:]
				data = ""
				while True:
					string = client.recv(1024)
					if(len(string)>=9):
						if (string[len(string)-9:len(string)] == "///end///"):
							data = data + string[:len(string)-9]
							break
					data = data + string
				initwriter(filename, data)
			if(string == ''):
				client.close()
				break
			if(string == 'write'):
				nstring = client.recv(1024)
				writer(nstring, client)
			if(string == 'read'):
				nstring = client.recv(1024)
				sender(nstring, client)
			if(string[:8] == "receive:"):
				getchunk(string[9:], client)
			if(string[:9] == "writereq:"):
				for x in range(len(string)-1, 0, -1):
					if string[x] == '/':
						reqport = int(string[x+1:])
						filename = string[10:x]
				requester(reqport, filename)

def clienter(ip, port):
	s=  socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
	s.bind((ip,port))
	s.listen(10)
	while True:
		client, addr = s.accept()
		thread.start_new_thread(listenerthread, (client,))

def listener(cont):
	global port
	while True:
		try:
			string = cont.recv(1024)
			print "Controller said:", string 
			if(string[:5] == "port:"):
				port = int(string[6:])
				print "port: ", port
				thread.start_new_thread(clienter, ('127.0.0.1', port)) 
				break
		except socket.error as msg:
			print "Controller disconnected."
			break

def Main(): 
	ip='127.0.0.1' 
	port=8000 
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
	s.connect((ip,port))
	s.send("server")
	controller.append(s)
	thread.start_new_thread(integritychecker, ())
	thread.start_new_thread(listener,(s,))
	while True:
		y = 0
if __name__=="__main__":
	Main()

