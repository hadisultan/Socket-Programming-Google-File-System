import socket
import thread
import sys
import hashlib
import math
import os.path
import uu
import random
import time

clientslist = []
serverslist = []
files = {}
buff = {}

def serverfail(sport):
	for x in files:
		for y in files[x]:
			for z in files[x][y]:
				serverd, addrd = z
				ipd, portd = addrd
				if sport == portd:
					files[x][y].remove(z)
					if len(files[x][y])>0:
						if len(serverslist)>2:
							newserver, newaddr = files[x][y][0]
							newip, newport = newaddr
							for a in serverslist:
								aserver, aaddr = a
								aip, aport = aaddr
								check = 0
								for b in files[x][y]:
									bserver, baddr = b
									bid, bport = baddr
									if aport == bport:
										check = 1
								if check == 0:
									msg = "writereq: "+x+"_chunk"+str(y)+"/"+str(aport)
									print msg
									print newport
									sn = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
									sn.connect(('127.0.0.1',newport))
									time.sleep(0.05)
									sn.send(msg)
									time.sleep(0.02)
									sn.close()
									break
								# if aport not in files[x][y]:



def filefinder(client):
	filestr = ""
	temp = []
	for x in files:
		if x in temp:
			continue
		else:
			filestr = filestr + x +"/"
			temp.append(x)
	client.send(filestr)
	string = client.recv(1024)
	if string not in files:
		client.send("NA")
	else:
		client.send(str(len(files[string])))
		while(True):
			message = client.recv(1024)
			if(message == "done"):
				break
			chunkid = int(message)
			print files[string][chunkid]
			nstring = ""
			for x in range(len(files[string][chunkid])):
				(server, addr) = files[string][chunkid][x]
				(ipaddr, port) = addr
				nstring = nstring + str(port) +"/"
			print nstring
			client.send(nstring)

def filefinder2(client):
	global buff
	filestr = ""
	temp = []
	for x in files:
		if x in temp:
			continue
		else:
			filestr = filestr + x +"/"
			temp.append(x)
	client.send(filestr)
	string = client.recv(1024)
	if string not in files:
		client.send("NA")
	else:
		client.send(str(len(files[string])))
		fname = ""
		while(True):
			message = client.recv(1024)
			if(message == "buffer"):
				fname = client.recv(1024)
				if fname not in buff:
					buff[fname] = 0
				if buff[fname] == 1:
					while True:
						# print "blocked"
						if buff[fname] == 0:
							break
				client.send('ok')
				message = client.recv(1024)
				buff[fname] = 1
			if(message == "done"):
				buff[fname] = 0
				break
			chunkid = int(message)
			print files[string][chunkid]
			nstring = ""
			for x in range(len(files[string][chunkid])):
				(server, addr) = files[string][chunkid][x]
				(ipaddr, port) = addr
				nstring = nstring + str(port) +"/"
			print nstring
			client.send(nstring)

def sender(data, filename):
	# print "Going to make a new file called", filename
	if(serverslist == 0):
		print "There are no servers available."
		return
	numberchunks = int(len(data)/65503)+1
	files[filename] = {}
	for x in range(numberchunks):
		files[filename][x] = []
		temp = []
		for y in range(3):
			r = random.randint(0,len(serverslist)-1)
			if(r in temp):
				if(len(serverslist)>2):
					while True:
						r = random.randint(0,len(serverslist)-1)
						if r not in temp:
							break
			# print "r is", r
			if r not in temp:
				temp.append(r)
				(server, addr) = serverslist[r]
				server.send("receive: "+filename+"_chunk"+str(x))
				time.sleep(0.05)
				server.send(data[x*65503:(x+1)*65503])
				time.sleep(0.05)
				server.send("///end///")
				files[filename][x].append((server, addr))

def receive(client, addr1, iden):
	while True:
		try:
			string = client.recv(1024)
			print string
			if(string[:9] == "writereq:"):
				temp=""
				reqport = 0
				filename = ""
				for x in range(len(string)-1, 0, -1):
					if string[x] == '/':
						reqport = int(string[x+1:])
						filename = string[10:x]
				initfile = ""
				for v in range(len(filename)-1, 0, -1):
					if(filename[v] == '_'):
						initfile = filename[:v]
				chunkid = 0
				for v in range(len(filename)-1, 0, -1):
					if(filename[v] == 'k'):
						chunkid = int(filename[v+1:])
				for v in files[initfile][chunkid]:
					(nserver, naddr) = v
					(ipaddr, vport) = naddr
					if vport != reqport:
						print vport==reqport
						print "vport", vport
						print "reqport", reqport
						sn = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
						sn.connect(('127.0.0.1',vport))
						time.sleep(0.05)
						sn.send(string)
						time.sleep(0.02)
						sn.close()
						break
				
			if(string[:7] == "hamsai:"):
				filename = string[8:]
				nport = int(client.recv(1024))
				initfile = ""
				for v in range(len(filename)-1, 0, -1):
					if(filename[v] == '_'):
						initfile = filename[:v]
				chunkid = 0
				for v in range(len(filename)-1, 0, -1):
					if(filename[v] == 'k'):
						chunkid = int(filename[v+1:])
				msg = ""
				for v in files[initfile][chunkid]:
					(nserver, naddr) = v
					(ipaddr, vport) = naddr
					if vport is not nport:
						msg = msg+str(vport)+"/"
				client.send(msg)
			if(string == "cascade"):
				filename = client.recv(1024)
				chunkid = client.recv(1024)
				chunkno = int(chunkid)+1
				if chunkno not in files[filename]:
					files[filename][chunkno] = []
					temp = []
					ports = []
					for y in range(3):
						r = random.randint(0,len(serverslist)-1)
						if(r in temp):
							if(len(serverslist)>2):
								while True:
									r = random.randint(0,len(serverslist)-1)
									if r not in temp:
										break
						# print "r is", r
						if r not in temp:
							temp.append(r)
							(server, addr) = serverslist[r]
							(ipaddr, nport) = addr
							ports.append(nport)
							files[filename][chunkno].append((server, addr))
					print files[filename][chunkno]
					msg = str(ports[0])
					client.send(msg)
				else:
					(server, addr) = files[filename][chunkno][0]
					(ipaddr, nport) = addr
					msg = str(nport)
					print "Sending server:", msg
					client.send(msg)

			if(string == "filereq"):
				filefinder(client)
			if(string == "filereq2"):
				filefinder2(client)
			if(string[0:7] == 'newfile'):
				if(len(serverslist)==0):
					print "No servers available."
				filename = string[8:]
				if filename not in files:
					files[filename]= {}
				chunkno = int(client.recv(1024))
				files[filename][chunkno] = []
				temp = []
				ports = []
				portsmsg = ""
				for y in range(3):
					r = random.randint(0,len(serverslist)-1)
					if(r in temp):
						if(len(serverslist)>2):
							while True:
								r = random.randint(0,len(serverslist)-1)
								if r not in temp:
									break
					# print "r is", r
					if r not in temp:
						temp.append(r)
						(server, addr) = serverslist[r]
						(ipaddr, nport) = addr
						ports.append(nport)
						files[filename][chunkno].append((server, addr))
						portsmsg = portsmsg + str(nport) + "/"
				print files[filename][chunkno]
				client.send(portsmsg)
			if(string == ''):
				if(iden == "client"):
					print "disconnected with client:", addr1
					clientslist.remove((client, addr1))
				elif(iden == "server"):
					print "disconnected with a chunk server:", addr1
					serverslist.remove((client, addr1))
					ipaddr, nport = addr1
					serverfail(nport)
				break
		except socket.error as msg:
			if(iden == "client"):
				print "disconnected with client:", addr1
				clientslist.remove((client, addr1))
			elif(iden == "server"):
				print "disconnected with a chunk server:", addr1
				serverslist.remove((client, addr1))
				ipaddr, nport = addr1
				serverfail(nport)
			break


def connector(s):
	while True:
		client, addr = s.accept()
		iden = client.recv(1024)
		if(iden == "client"):
			print "A client connected."
			clientslist.append((client, addr))
		elif(iden == "server"):
			ips, port = addr
			print "A new chunk server connected", port
			client.send("port: "+str(port))
			# data = "hello friend"
			serverslist.append((client, addr))
			# sender(data, 'data.txt')
		thread.start_new_thread(receive, (client, addr, iden))
		# print "client: "
		# print client
		# print "address"
		# print addr
		# client.send("hello from controller")


def Main():
	host = '127.0.0.1'
	port = 8000
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind((host,port))
	s.listen(10)
	thread.start_new_thread(connector, (s,))
	while True:
		query = raw_input()
		f = open("lol.txt", "r")
		data = f.read()
		sender(data, 'l.txt')

if __name__=="__main__":
	Main()