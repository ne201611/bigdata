# -*- coding=utf-8 -*-
import csv,os,sys,re,urllib,simplejson
from pymongo import MongoClient
import datetime,thread,time
from threading import Thread

#数据库连接
def mongoConn(ip,dbName,collectionName):
    try:
        client = MongoClient("mongodb://%s:27017/"%(ip))
        db = client[dbName]
        collection = db[collectionName]
        return collection
    except:
        print "数据库连接异常"

#获取csv文件名
def getFileList():
    rootpath = "/opt/csv/"
    d1 = datetime.datetime.now()
    d2 = datetime.timedelta(days=1)
    filepath02 = "location-" + datetime.datetime.strftime(d1-d2,"%Y%m%d")
    filepath = os.path.join(rootpath,filepath02)
    fileList = []
    list = ["09","10","11","12","13","14","15","16","17","18","19","20","21"] #只转存09:00~22:00之间定位数据
    for i in list:
        filename = filepath02+"-" + i +".csv"
        fileList.append(filename)
    os.chdir(filepath)
    #fileList = ["location-20170208-16.csv"]
    return fileList

def shopinfo(x,y,floorid):
    shopname,shopaddress = "",""
    url = 'http://172.31.1.114/shopinfo.php?x=%s&y=%s&distance=1&parents=%s'%(x,y,floorid)
    try:
        f = urllib.urlopen(url)
        data = simplejson.loads(f.read())
        if str(data['category']['id']).startswith("1"):
            shopname,shopaddress = data['name'],data['address']
        else:
            shopname = data['category']["name4"]
            shopaddress = data['category']["name1"]
    except:
        shopname,shopaddress = " "," "
    finally:
        return shopname,shopaddress

#读取数据，并写入数据库
def InsertDataFromCVS(collection,filename):
    patternMac="^[A-F0-9]{2}(:[A-F0-9]{2}){5}$"
    starttime = datetime.datetime.now()
    line_count = 0
    documents = []
    if os.path.isfile(filename):
        with open(filename, "rb") as f:
            reader = csv.reader( (line.replace('\0','') for line in f) )
            for line in reader:
                try:
                   mac,floorid,x,y,ts = line[1],line[2],line[3],line[4],line[5]
                   if re.match(patternMac, mac, flags=0): #判断包含合法mac地址的原始数据
#                       shopname,shopaddress = shopinfo(x,y,floorid)
                       sqlDict = {
                                 'mac':mac,
                                 'floorid':int(floorid),
                                 'x':float(x),
                                 'y':float(y),
                                 'ts':float(ts),
 #                                'shopname':shopname,
 #                                'shopaddress':shopaddress
                                 }
                       documents.append(sqlDict)
                   else:
                       pass
                   if (line_count != 399):
                       line_count = line_count + 1
                   else:
                       #print documents
                       collection.insert_many(documents)
                       documents,line_count= [],0
                except:
                    pass
        endtime = datetime.datetime.now()
        time = endtime - starttime
        info = "%s,转存数据库成功." %(filename)
        print (info + "总耗时:" + str(time))
    else:
        info = "%s,文件不存在" %(filename)
        print (info)

class MyThread(Thread):
	"""docstring for MyThread"""
	def __init__(self, collection, filename):
		super(MyThread, self).__init__()
		self.collection = collection
		self.filename = filename
	def run(self):
		InsertDataFromCVS(self.collection, self.filename)

if __name__ == '__main__':
    d1 = datetime.datetime.now()
    d2 = datetime.timedelta(days=1)
    dbname = "location" + "-" + datetime.datetime.strftime(d1-d2,"%Y%m%d")
    collectionname = "raw_data"
    collection = mongoConn("172.31.1.137",dbname,collectionname)
    fileList = getFileList()
    for filename in fileList:
        try:
           a = MyThread(collection,filename)
           a.start()
        except:
           print "线程启动失败！"
    collection.create_index("mac")
    sys.stdout.flush()
