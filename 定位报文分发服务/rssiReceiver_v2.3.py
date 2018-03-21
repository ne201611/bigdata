# -*- coding=utf-8 -*-
'''
功能描述：
    搭建UDP服务器，解析H3C AP上报的定位数据实现本地原始数据存储，并复制转发一份二进制数据至图聚ginkgo服务器。
版本发行说明：
    1）增加mac地址过滤，以免影响数据准确性   2016/12/29
    2）V2.1在前一版本上，增加自动学习filter-list加载。2017/02/27
    3) v2.2 测试sockserver
    4) v2.3 换回使用socket搭建udp server. 另外，使用mutiprocess 多进程代替多线程，解决了Recv-Q Send-Q 堆积问题。
       另外，使用os.fork 搭建Daemon守护进程
'''
import threading,sys,os,re,socket
import struct,time,datetime
from pymongo import MongoClient
from multiprocessing import Process

def readMacFilter():
    filter_list = []
    try:
        client = MongoClient("mongodb://%s:27017/"%('172.31.1.137'))
        db = client['config']
        collection = db['mac_filter']
    except:
        print ("数据库连接异常")
    filter_list_temp = collection.find({'value':{"$gte":3}}).distinct('_id')
    for mac in filter_list_temp:
        mac = mac.replace(':','').lower()
        filter_list.append(mac)
    return filter_list

class RunUDPServer():
    LocalHost,LocalPort = '0.0.0.0',30013
    def __init__(self):
        # udp 监听函数
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.LocalHost, self.LocalPort))
    def listenConn(self):
        # 读取client 发送的信息
        try:
            while 1:
                msg, client = self.sock.recvfrom(1024)
                self.packetDecode(msg)
        except (KeyboardInterrupt, SystemExit):
            sys.exit()
        except:
            print ("服务启动失败！")
            sys.exit()
        finally:
            sys.stdout.flush()
    def packetDecode(self,msg):
        clientMac, apMac = '', ''
        clientMacBuffer = struct.unpack('>6B',msg[12:18])
        for intval in clientMacBuffer:
            if intval > 15:
                replacestr = '0x'
            else:
                replacestr = 'x'
            clientMac = ''.join([clientMac, hex(intval).replace(replacestr, '')])
        #filter_list = list(set(self.filter_List).union(set(filter_list_db)))
        if (clientMac in filter_list_db):
            pass
        else:
            self.dispath(msg)
            apMacBuffer = struct.unpack('>6B',msg[18:24])
            for intval in apMacBuffer:
                if intval > 15:
                    replacestr = '0x'
                else:
                    replacestr = 'x'
                apMac = ''.join([apMac, hex(intval).replace(replacestr, '')])
            timestamp = time.strftime('%H:%M:%S', time.localtime(time.time()))
            rssi, = struct.unpack('>b',msg[40:41])
            line = "[%s][ap:%s,client:%s,rssi:%s]" %(timestamp,apMac,clientMac ,rssi)
            self.saveLog(line)
    def saveLog(self,data):
        rootpath = "/data/rssiReceiver/log/"
        filepath = "rssi-" + datetime.datetime.strftime(datetime.datetime.now(),"%Y%m%d")
        filepath = os.path.join(rootpath,filepath)
        filename = "rssi-" + datetime.datetime.strftime(datetime.datetime.now(),"%H") + ".log"
        try:
            if os.path.exists(filepath) == False:
                os.mkdir(filepath)
                os.chdir(filepath)
            else:
                os.chdir(filepath)
            with open(filename,"a") as f:
                f.write(data + '\n')
        except:
            print ("写入日志文件失败！！")

    def dispath(self,data):
        try:
            self.sock.sendto(data, ('172.31.1.139',30013))
        except:
            print( '转发定位数据到ginkgo失败')
            sys.stdout.flush()

if __name__ == '__main__':
    filter_list_db = readMacFilter()
    instance = RunUDPServer()
    for i in range(16):
        d = Process(target=instance.listenConn,args=())
        d.start()
