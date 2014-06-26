#!/usr/bin/env python2
#-*-encoding:utf-8-*-
import socket
import time
import os
import sys
import thread
import struct
import threading
import select
from collections import defaultdict

info=None
dic=defaultdict(lambda:defaultdict(lambda:[None]))#dic [GID][FID]=[(mac1,ip1),(mac2,ip2)] 
dicp=defaultdict(lambda:defaultdict(lambda:None)) #dic [GID][FID]=port
host={}                                           #dic [mac]=GID 
mactoip={}                                        #dic [mac]=ip
timer={}                                          #dic [mac]=time.time()          
con=threading.Condition()

dicp[0][0]=50002
dicp[0][1]=50003
dicp[0][2]=50004
#berrypie:'B8:27:EB:F1:3C:DE'+'#'+'10.0.0.50'
controllerip = '10.0.0.200'

def recv_videolist():    #接收来自media server的视频列表
	global m_list	
	video_num = 0
	s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	port = 40000
	s1.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1) 
        s1.bind((s1.getsockname()[0],port))  #绑定管理服务器本身的ip
        s1.listen(100) 
	connection,address=s1.accept()
	m_list = connection.recv(1024)  #此时接受到的list为字符串['dayi720p2.264', 'fast720p.txt', 'multith.py', 'dayi720p1.264', 'final.264', 'a.264', 'da2.264', 'a1.264', 'fast720p.txt~', 'out.264', 'dayi720p5.264', 'h264.h'],不是列表
	print m_list
	s1.close()

def listen_client():   #接收客户端发来的客户端的ip 并发送视频列表
    while 1:
	s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	port = 40004  #侦听client请求
	s1.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        s1.bind((s1.getsockname()[0],port))  #绑定自身即manage的ip
        s1.listen(100) 
	connection,address=s1.accept()
	serverip = connection.recv(1024)
	print serverip
	send_videolist(serverip)
	s1.close()
    thread.exit_thread()

def send_videolist(serverip):   #向客户端发送视频列表
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	port = 40006
	s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
	print '#####################################'
	print serverip
	print m_list
	s.connect((serverip,port))  #连接客户端ip
	s.send(m_list)  #list是一个列表，不能被send，要转换为str或buf才行 
	s.close()

class clientToserver(threading.Thread):	  #客户端到服务器  服务器侦听端口消息，等待客户端发信息，接受info消息str(GID)+'#'+str(FID)+'#'+getHwAddr('eth0')+'#'+(s.getsockname()[0])		  
    def __init__(self):
        threading.Thread.__init__(self)
        self.thread_stop=False
    
    def run(self):
        while True:
            global info,con
            if con.acquire():  #获得锁
                print 'thread clienttoserver'
                print info
                self.socket=socket.socket(socket.AF_INET, socket.SOCK_STREAM)    
                port=50972
                self.socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)  #这里value设置为1，表示将SO_REUSEADDR标记为TRUE，操作系统会在服务器socket被关闭或服务器进程终止后马上释放该服务器的端口，否则操作系统会保留几分钟该端口。
                self.socket.bind((self.socket.getsockname()[0],port))  
                self.socket.listen(100)  #利用listen()函数进行侦听连接。该函数只有一个参数，其指明了在服务器实际处理连接的时候，允许有多少个未决（等待）的连接在队列中等待。作为一个约定，很多人设置为5。如：s.listen(5)
                if info is None:
                    connection,address=self.socket.accept()  #调用accept方法时，socket会进入'waiting'（或阻塞）状态。客户请求连接时，方法建立连接并返回服务器。accept方法返回一个含有俩个元素的元组，形如(connection,address)。第一个元素（connection）是新的socket对象，服务器通过它与客户通信；第二个元素（address）是客户的internet地址。
                    info=connection.recv(1024) #服务器使用recv方法从客户接受信息。调用recv时，必须指定一个整数来控制本次调用所接受的最大数据量。recv方法在接受数据时会进入'blocket'状态，最后返回一个字符串，用它来表示收到的数据。如果发送的量超过recv所允许，数据会被截断。多余的数据将缓冲于接受端。以后调用recv时，多余的数据会从缓冲区删除。

#                    connection.close() #传输结束，服务器调用socket的close方法以关闭连接
		    self.socket.close()
                    print info
                    #con.notify()
                #con.wait()
                con.release() #释放锁
#		self.socket.close()
                time.sleep(0.1)
            	
    def stop(self):
            self.thread_stop=True


class sertocon(threading.Thread):  #虚拟主机  发送虚拟主机的ip mac以及请求的视频和层数给控制器
  def __init__(self):
    threading.Thread.__init__(self)
    self.thread_stop=False
  
  def run(self):
    a='D4:3D:7E:67:59:5A'   #LAB4 mac
    
    info = {}
#    for i in range(1, 5):
#      info[i] = "0#" + "2#" + "00:00:00:00:11:0" + str(i) + "#" + "10.0.0.10" + str(i)
#    for i in range(5,10):
#      info[i] = "0#" + "1#" + "00:00:00:00:11:0" + str(i) + "#" + "10.0.0.10" + str(i)
    for i in range(1, 10):  #对于主机1-20分配ip和mac地址以及请求的视频和层数
      info[i] = "0#" + "0#" + "00:00:00:00:11:0" + str(i) + "#" + "10.0.0.10" + str(i)
    for i in range(10, 21):  #对于主机1-20分配ip和mac地址以及请求的视频和层数
      info[i] = "0#" + "0#" + "00:00:00:00:11:" + str(i) + "#" + "10.0.0.1" + str(i)
    self.s1=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port=8001
    self.s1.connect(('10.0.0.200',port))   #connect连接控制器
    self.s1.send('b'+'#'+'00'+'#'+'B8:27:EB:F1:3C:DE'+'#'+'10.0.0.50'+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[0][0]))  #socket对象的getpeername()和getsockname()方法都返回包含一个IP地址和端口的二元组（这个二元组的形式就像你传递给connect和bind的）。getpeername返回所连接的远程socket的地址和端口，getsockname返回关于本地socket的相同信息。 
    print 'b'+'#'+'00'+'#'+'B8:27:EB:F1:3C:DE'+'#'+'10.0.0.50'+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[0][0])
    print '8001===========00'
    self.s1.close()
    time.sleep(5)
    print '8001===========11'
    self.s1=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port=8001
    self.s1.connect(('10.0.0.200',port))
    print '8001===========22'
    self.s1.send('b'+'#'+'01'+'#'+'B8:27:EB:F1:3C:DE'+'#'+'10.0.0.50'+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[0][1]))
    print 'b'+'#'+'01'+'#'+'B8:27:EB:F1:3C:DE'+'#'+'10.0.0.50'+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[0][1])
    self.s1.close()
    time.sleep(3)
    self.s1=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port=8001
    self.s1.connect(('10.0.0.200',port))
    self.s1.send('b'+'#'+'02'+'#'+'B8:27:EB:F1:3C:DE'+'#'+'10.0.0.50'+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[0][2]))
    print 'b'+'#'+'02'+'#'+'B8:27:EB:F1:3C:DE'+'#'+'10.0.0.50'+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[0][2])
    self.s1.close()
    time.sleep(3)
    
    for i in range(2, 4):
      inf = info[i].split('#')  #字符串的split函数默认分隔符是空格 ' '，这里为#，功能把字符串拆成列表
      gid = int(inf[0])  #请求视频类型
      fid = int(inf[1])  #视频层数0 1 2
      mac=inf[2]
      ip=inf[3]
      for j in range(0,fid+1):
        self.s1=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        port=8001
        self.s1.connect(('10.0.0.200',port))
        
        self.s1.send('i'+'#'+str(gid)+str(j)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[gid][j]))
        print 'i'+'#'+str(gid)+str(j)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[gid][j])
        self.s1.close()
        time.sleep(3)

    def stop(self):
      self.thread_stop=True

class serverTocontroller(threading.Thread):     #服务器到控制器           
    def __init__(self):
        threading.Thread.__init__(self)
        self.thread_stop=False          

    def run(self):
        global a
        a='D4:3D:7E:67:59:5A'
        while True:
            global info,con
            if con.acquire():
                    print "server to controller"
                    if info is None:
                        print 'Waiting the messenger from client...'
                    else:
                        inf=info.split('#')
                        mac=inf[2]
                        ip=inf[3] 
                        fid = int(inf[1])
                        gid = int(inf[0])
                        if mac in host.keys():  #mac为字典host的键值，如果客户端之前就在host里，则删除之前客户端的所在的视频组和层数信息，从新分配
                            G=host[mac]         #G为这个键对应的value
                            print '该客户端的mac地址已经在host{}里存在'
#                          if int(inf[4]) is 0:  #控制器发来的自适应信息
                            if gid is not G:
                                print 'gid is not G'
                                for i in dic[G].keys():
                                    print 'dic###################'
                                    print dic
                                    if (mac, ip) in dic[G][i]:
                                            dic[G][i].remove((mac, ip))    #？？？？？？？？？？
                                            print 'dic###################'
                                            print dic
                                            self.s1=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                            port=8001
                                            self.s1.connect(('10.0.0.200',port))
                                            self.s1.send('d'+'#'+str(G)+str(i)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[G][i]))
                                            print '新发送的mac在原来的host里，请求的gid和原来mac对应的gid不一样，删除'
                                            print 'd'+'#'+str(G)+str(i)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[G][i])
                                            self.s1.close()
                                            if dic[G][i] == []:
                                                del dic[G][i]
                                            if dic[G].keys() is None:
                                                del dic[G]
                                            time.sleep(0.5)
                                G=int(inf[0])
                                FID=inf[1]
                                host[mac]=G
                                mactoip[mac] = ip
                                if G in dic.keys():   #G为视频组
                                    for i in range(int(FID)+1):  #层数
                                        if i in dic[G].keys():
                                            dic[G][i].append((mac,ip))  #将新的client的mac ip加入到dic字典里
                                            self.s1=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                            port=8001
                                            self.s1.connect(('10.0.0.200',port))
                                            self.s1.send('i'+'#'+str(G)+str(i)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[G][i]))
                                            
                                            print 'i'+'#'+str(G)+str(i)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[G][i])
                                            self.s1.close()
                                        else:
                                            dic[G][i]=[(mac,ip)]  #如果客户端所请求的层数之前没有，则加入一个mac ip
                                            self.s1=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                            port=8001
                                            self.s1.connect(('10.0.0.200',port))
                                            self.s1.send('i'+'#'+str(G)+str(i)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[G][i]))
                                            
                                            print 'i'+'#'+str(G)+str(i)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[G][i])
                                            self.s1.close()
                                        time.sleep(0.5)
                                else:  #如果客户端所请求的视频不再之前的视频组里
                                    for i in range(int(FID)+1):
                                        dic[G][i]=[(mac,ip)]
                                        self.s1=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                        port=8001
                                        self.s1.connect(('10.0.0.200',port))
                                        self.s1.send('i'+'#'+str(G)+str(i)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[G][i]))
                                        
                                        print 'i'+'#'+str(G)+str(i)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[G][i])
                                        self.s1.close()
                                        time.sleep(0.5)  
                                                                        	
                            else:   #if gid is G:请求的还是原来的视频
                            		print 'gid is G 000000000000000000'
                            		n=0	
                                	for i in dic[G].keys():
                                    	    if (mac, ip) in dic[G][i]:
                                    		n=n+1
                                	n=n-1
                                	print '之前客户端所播放的层数，在dic字典里该视频对应的keys'
                                	print n
#                                for i in dic[G].keys():	
#                                    if (mac, ip) in dic[G][i]:
                                        if n > fid:
                                            delayer = n-fid
                                            print 'delayer^^^^^^^^^^^^^^^^'
                                            print delayer
                                            
                                            self.s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                            port=9001
                                            self.s.connect((ip,port))
                                            self.s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                                            self.s.send('L'+str(delayer))
                                            time.sleep(8)
                                            
                                            for j in range(0,delayer):
                                                dic[G][n-j].remove((mac, ip))
                                                self.s1=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                                port=8001
                                                self.s1.connect(('10.0.0.200',port))
                                                print '新发送的mac在原来的host里，请求的gid和原来mac对应的gid一样，新请求的视频层数没有原来的高，删除指定流'
                                                self.s1.send('d'+'#'+str(G)+str(n-j)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[G][n-j]))
                                                print 'd'+'#'+str(G)+str(n-j)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[G][n-j])
                                                if dic[G][n-j] == []:
                                                	del dic[G][n-j]
                                                if dic[G].keys() is None:
                                                	del dic[G]
                                            print 'request fid less than origin fid(n) : n > fid , deleted dic'        	
                                            print dic
                                                
                                        if n < fid:
                                            delayer = fid-n
                                            print 'insertlayer^^^^^^^^^^^^^^^^'
                                            print delayer
                                            
                                            self.s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                            port=9001
                                            self.s.connect((ip,port))
                                            self.s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                                            self.s.send('B'+str(delayer))
                                            time.sleep(8)
                                            
                                            for j in range(1,delayer+1):	
                                                self.s1=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                                port=8001
                                                if i in dic[G].keys():
                                                	dic[G][n+j].append((mac,ip))
                                                else:
                                                	dic[G][n+j]=[(mac,ip)]
                                                self.s1.connect(('10.0.0.200',port))
                                                print '新发送的mac在原来的host里，请求的gid和原来mac对应的gid一样，新请求的视频层数比原来的高，加入指定流'
                                                self.s1.send('i'+'#'+str(G)+str(n+j)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[G][n+j]))
                                                print 'i'+'#'+str(G)+str(n+j)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[G][n+j])
                                            print 'request fid more than origin fid(n) : n < fid , added dic'        
                                            print dic
                                        
                                                
#                                        if n == fid:
#                                            continue
                                        
                                            
                            info = None
                            con.release()
                            time.sleep(0.1)        			
                            continue   #重新while循环  
                            
                        print '该客户端的mac地址不在host{}里，即该客户端第一次请求视频'    
                        G=int(inf[0])
                        FID=inf[1]
                        host[mac]=G
                        mactoip[mac] = ip
                        if G in dic.keys():   #G为视频组
                            for i in range(int(FID)+1):  #层数
                                if i in dic[G].keys():
                                    dic[G][i].append((mac,ip))  #将新的client的mac ip加入到dic字典里
                                    self.s1=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                    port=8001
                                    self.s1.connect(('10.0.0.200',port))
                                    self.s1.send('i'+'#'+str(G)+str(i)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[G][i]))
                                    print 'i'+'#'+str(G)+str(i)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[G][i])
                                    self.s1.close()
                                else:
                                    dic[G][i]=[(mac,ip)]  #如果客户端所请求的层数之前没有，则加入一个mac ip
                                    self.s1=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                    port=8001
                                    self.s1.connect(('10.0.0.200',port))
                                    self.s1.send('i'+'#'+str(G)+str(i)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[G][i]))
                                    print 'i'+'#'+str(G)+str(i)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[G][i])
                                    self.s1.close()
                                time.sleep(0.5)
                        else:  #如果客户端所请求的视频不在之前的视频组里
                            for i in range(int(FID)+1):
                                dic[G][i]=[(mac,ip)]
                                self.s1=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                port=8001
                                self.s1.connect(('10.0.0.200',port))
                                self.s1.send('i'+'#'+str(G)+str(i)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[G][i]))
                                print 'i'+'#'+str(G)+str(i)+'#'+mac+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[G][i])
                                self.s1.close()
                                time.sleep(0.5)
                    info = None
                    
                    print "dic******************************"
                    print dic
                    print "dicp*****************************"
                    print dicp
                    print "host*****************************"
                    print host
                    #con.notify()
                    #con.wait()
                    con.release()
                    time.sleep(0.1)
                                

    def stop(self):
            self.thread_stop=True

class receving(threading.Thread):   #接受客户端mac地址
    def __init__(self):

        threading.Thread.__init__(self)
        self.thread_stop=False
    def run(self):
        print "recevingggggggggggggggggggggggggggg"
        while True:
            self.socket=socket.socket(socket.AF_INET, socket.SOCK_STREAM)    
            port=7874
            self.socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)  
            self.socket.bind((self.socket.getsockname()[0],port))
            self.socket.listen(100)
            connection,address=self.socket.accept()
            buf=connection.recv(1024)  #客户端mac
#            connection.close()
	    self.socket.close()
            global timer
            print 'buffffffffffffffffffffffff'
            print buf
            timer[buf]=time.time()
            #print "receiving timer"
            #print timer
#            self.socket.close()
          
    def stop(self):
            self.thread_stop=True

class heartbeat(threading.Thread):  #向控制器发送信息，防止客户端不正常退出，管理服务器对其进行的处理
    def __init__(self):
        threading.Thread.__init__(self)
        self.thread_stop=False



    def run(self):
        
        global a
        a='D4:3D:7E:67:59:5A'
        while True:
            t1=time.time()
            global timer
            
            
            for i in timer.keys():
                t2=t1-timer[i]
#                print '%%%%%%%%%%%%%%%%%%%%%%%%%%%'
#                print t2
#                print '%%%%%%%%%%%%%%%%%%%%%%%%%%%'
                if t2>12:
                    print 'The host whose mac is %s has lost' %(i)
                    Gid=host[i]
                    ip=mactoip[i]
                    for f in dic[Gid].keys():
                        if (i,ip) in dic[Gid][f]:
                            self.s1=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            port=8001
                            self.s1.connect(('10.0.0.200',port))
                            self.s1.send('d'+'#'+str(Gid)+str(f)+'#'+i+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[Gid][f]))
                            print 'd'+'#'+str(Gid)+str(f)+'#'+i+'#'+ip+'#'+a+'#'+(self.s1.getsockname())[0]+'#'+str(dicp[Gid][f])
                            self.s1.close()
                            dic[Gid][f].remove((i, ip))
                            if dic[Gid][f] == []:
                                del dic[Gid][f]
                            if dic[Gid].keys() is None:
                                del dic[Gid]    
                    del host[i]
                    del mactoip[i]
                    del timer[i]    
                """     
                print 'mactoip'
                print mactoip
                print 'timer'
                print timer
                """

def test():
    thread.start_new_thread(recv_videolist,())
    thread.start_new_thread(listen_client,())
#    recv_videolist()
#    listen_client()
    thread0=heartbeat()
    thread0.start()
    thread1=clientToserver()
    thread1.start() 
    thread2 = sertocon()
    thread2.start()
    thread3=serverTocontroller()
    thread3.start()
    thread4=receving()
    thread4.start()
    
    time.sleep(1000)

if __name__=='__main__':
    test()
