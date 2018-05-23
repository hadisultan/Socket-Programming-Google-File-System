import socket
import thread
import sys
import hashlib
import math
import os.path
import uu
import time

controller = []
ports = []
servers = []

def sender(data, s, filename):
	numberchunks = int(len(data)/65503)+1
	print "Total chunks:", numberchunks
	for x in range(numberchunks):	
		s.send('newfile '+filename)
		time.sleep(0.05)
		s.send(str(x))
		string = s.recv(1024)
		storep = []
		temp = ""
		for z in range(len(string)):
			if string[z] is not '/':
				temp = temp + string[z]
			else:
				storep.append(int(temp))
				temp = ""
		for y in storep:
			v = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
			v.connect(('127.0.0.1', y))
			time.sleep(0.03)
			v.send("receive: "+filename+"_chunk"+str(x))
			time.sleep(0.03)
			# print "len", len(data[x*65503:(x+1)*65503])
			# print "size", sys.getsizeof(data[x*65503:(x+1)*65503])
			v.send(data[x*65503:(x+1)*65503])
			time.sleep(0.03)
			v.send("///end///")
			v.close()
	
	# time.sleep(0.1)
	# s.send(data)
	# time.sleep(0.1)
	# s.send("///end///")
	
def Main(): 
	ip='127.0.0.1' 
	port=8000 

	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
	# v = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
	# v.connect((ip, 8200))
	s.connect((ip,port))
	s.send("client")
	# v.send("client")
	# v.close()s
	#0001 specifies that this is a client
	controller.append(s)
	# thread.start_new_thread(listener,(s,0))
	inputtxt = ""
	while True:
		query = raw_input("Enter 1 to enter a file, 2 to read a pre-existing file, 3 to write to a pre-existing file and -1 to exit: ")
		if(query == "-1"):
			break
		if(query == "1"):
			q2 = raw_input("Enter 1 to enter the path to an existing file or 2 to enter the text for the file: ")
			if(q2 == "1"):
				filepath = raw_input("Enter file path: ")
				f = open(filepath, 'r')
				data = f.read()
				filename = raw_input("Enter file name: ")
				sender(data, s, filename)
			if(q2 == "2"):
				data = raw_input("Enter the data: ")
				filename = raw_input("Enter file name: ")
				sender(data, s, filename)
		if(query == "2"):
			files = []
			s.send("filereq")
			string = s.recv(1024)
			files = []
			temp = ""
			for x in range(len(string)):
				if string[x] is not '/':
					temp = temp + string[x]
				else:
					files.append(temp)
					temp = ""
			print "The files available are: "
			for x in files:
				print x
			while True:
				filename = raw_input("Which file do you want to read? ")
				if filename in files:
					break
			s.send(filename)
			chunks = int(s.recv(1024))
			qt = "There are " + str(chunks) + " chunks [0- "+str(chunks-1) +"]. Which one do you want to read? (Enter -1 for all): "
			q3 = raw_input(qt) 
			requests = []
			if q3 == "-1":
				for x in range(chunks):
					requests.append(str(x))
			else:
				requests.append(q3)
			for x in requests:
				s.send(x)
				portstr = s.recv(1024)
				portsread = []
				temp = ""
				for i in range(len(portstr)):
					if portstr[i] is not '/':
						temp = temp + portstr[i]
					else:
						portsread.append(int(temp))
						temp = ""
				for con in range(len(portsread)):
					v = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
					v.connect((ip, portsread[con]))
					time.sleep(0.02)
					v.send('read')
					chunkname = filename+"_chunk"+x
					v.send(chunkname)
					dataread = ""
					err = 0
					errstat = v.recv(1024)
					if(errstat == "err"):
						print "Some error."
						err = err+1
					if(err == 0):	
						while True:
							nstring = v.recv(1024)
							dataread = dataread + nstring
							if(len(nstring)>=9):
								if (nstring[len(nstring)-9:len(nstring)] == "///end///"):
									dataread = dataread[:len(dataread)-9]
									break
						print "Chunk", x, "is:", dataread
					v.close()
					if(err == 0):
						break
					else:
						print "File was tampered, looking up at another copy."
						if(err == len(portsread)):
							print "No untampered copy available, sorry."
			s.send("done")
		if(query == "3"):
			files = []
			s.send("filereq2")
			string = s.recv(1024)
			files = []
			temp = ""
			for x in range(len(string)):
				if string[x] is not '/':
					temp = temp + string[x]
				else:
					files.append(temp)
					temp = ""
			print "The files available are: "
			for x in files:
				print x
			while True:
				filename = raw_input("Which file do you want to write to? ")
				if filename in files:
					break
			s.send(filename)
			chunks = int(s.recv(1024))
			qt = "There are " + str(chunks) + " chunks [0- "+str(chunks-1) +"]. Which one do you want to write to? "
			q3 = raw_input(qt) 
			requests = []
			if q3 == "-1":
				for x in range(chunks):
					requests.append(str(x))
			else:
				requests.append(q3)
			for x in requests:
				chunkname = filename+"_chunk"+x
				s.send('buffer')
				s.send(chunkname)
				s.recv(1024)
				s.send(x)
				portstr = s.recv(1024)
				portsread = []
				temp = ""
				for i in range(len(portstr)):
					if portstr[i] is not '/':
						temp = temp + portstr[i]
					else:
						portsread.append(int(temp))
						temp = ""
				for con in range(len(portsread)):
					v = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
					v.connect((ip, portsread[con]))
					time.sleep(0.02)
					v.send('write')
					v.send(chunkname)
					dataread = ""
					err = 0
					errstat = v.recv(1024)
					if(errstat == "err"):
						print "Some error."
						err = err+1
					if(err == 0):	
						while True:
							nstring = v.recv(1024)
							dataread = dataread + nstring
							if(len(nstring)>=9):
								if (nstring[len(nstring)-9:len(nstring)] == "///end///"):
									dataread = dataread[:len(dataread)-9]
									break
						print "Chunk", x, "is:", dataread
						offset = raw_input("What is the offset that you want to write to? (0-65502 or 0-(max chars-1)): ")
						txt = raw_input("What do you want to write here? ")
						v.send(offset)
						time.sleep(0.05)
						v.send(txt)
						time.sleep(0.02)
						v.send("///end///")
						nstring = ''
						while True:
							nnstring = v.recv(1024)
							nstring = nstring+ nnstring
							if (nnstring[len(nnstring)-9:len(nnstring)] == "///end///"):
								nstring = nstring[:len(nstring)-9]
								break
					s.send('done')
					v.close()
					if(err == 0):
						print "Edited file is:", nstring 
						break
					else:
						print "File was tampered, looking up at another copy."
						if(err == len(portsread)):
							print "No untampered copy available, sorry."

			# s.send("done")
		# 	filetxt = raw_input("Enter file text: ")
		# 	s.send(filetxt)
		# 	s.send("end")
		# 	time.sleep(0.5)
		# 	while (resend != 0 ):
		# 		s.send(send)
		# 		s.send(filetxt)
		# 		s.send("end")
		# 		resend = 0
		# 		time.sleep(0.5)
		# if(query == 2):
		# 	s.send("filereq")
			


if __name__=="__main__":
	Main()
