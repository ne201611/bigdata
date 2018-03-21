# -*- coding=utf-8 -*-
from pymongo import MongoClient
import datetime,time,sys
from bson.code import Code
from module.api import  *
from threading import Thread

#数据库连接
def mongoConn(ip,dbName,collectionName):
    try:
        client = MongoClient("mongodb://%s:27017/"%(ip))
        db = client[dbName]
        if (ip == '172.31.1.10'):
            db.authenticate("root","111111")
        collection = db[collectionName]
        return collection
    except:
        print "数据库连接异常"

def mongoQueryOne(collection,mac):
        res = collection.find_one({"endUserMac":mac})
        return res

def mongoUpdate(collection,res,phone):
        res["phone"] = phone
        collection.save(res)

def mongoSave(dic,collection):
        collection.insert_one(dic)

def portalInterface(collection_Flow,collection_Radius):
    num = 0
    for doc in collection_Flow.find():
        #print doc["IdData"]
        mac = str(doc["mac"]).replace(':','-')
        res = mongoQueryOne(collection_Radius,mac)
        if res :
            num = num + 1
            mongoUpdate(collection_Flow,doc,res["phone"])

def mongo_count(collection):
    print "总记录条数：%d"%(collection.count())
    print "单一MAC地址数 %d"%(len(collection.distinct('IdData')))

def mongo_distinct(collection,collection_Flow):
    for mac in collection.distinct('mac'):
        count = collection.count({"mac":mac})
        dic = {
            "mac":mac,
            "count":count
          }
        mongoSave(dic,collection_Flow)

def mapReduce(collection,timestamp):
    map = Code("function() {"
            "var value = {'x':this.x,'y':this.y,'ts':this.ts,'floorid':this.floorid};"
            "var position = new Array();"
            "position = position.concat(value);"
            "emit(this.mac,{counts:1,positions:position});"
            "}"
              )
    reduce = Code("function(key,values){"
            "var count = 0;"
            "var position= new Array();"
            "for(var i = 0; i < values.length; i++) {"
            "   if (values[i].counts == 1){"
            "       count +=1;"
            "       position=position.concat(values[i].positions);"
            "                             }"
            "   else{"
            "       for(var num = 0; num < values[i].counts ; num++) {"
            "           count +=1;"
            "           position=position.concat(values[i].positions[num]);"
            "           }"

            "       }"
            "                                       }"
            "var res = {counts:count,positions:position};"
            "return res;"
            "                           }"
                 )
    #results = collection.map_reduce(map,reduce,"dailyFlow",query={"IdData":"00:00:5E:00:01:16"},full_response=True)
    #results = collection.map_reduce(map,reduce,"dailyFlow",query={"IdData":usermac})
    results_01 = collection.map_reduce(map,reduce,{'reduce':"mapreduce"},{"sort":"mac"},query={"ts":{ "$lte":timestamp }})
    results_02 = collection.map_reduce(map,reduce,{'reduce':"mapreduce"},{"sort":"mac"},query={"ts":{ "$gt":timestamp }})
'''
def groupReduce(collection):
    reduce = Code("function(obj,prev){"
            "var value = {'x':obj.x,'y':obj.y,'ts':obj.ts,'foorId':obj.floorId};"
            "prev.counts ++;"
            "prev.positions[prev.counts-1] = value"

            "}"
                 )
    #results = collection.group(['IdData'],{"IdData":"00:00:5E:00:01:16"},{'counts':0,'positions':[]},reduce)
    results = collection.group(['IdData'],None,{'counts':0,'positions':[]},reduce)
    #分析结果转存到dailyFlow
    collection_dailyFlow = mongoConn("172.31.1.136","location","dailyFlow")
    try:
        collection_dailyFlow.insert_one(results[0])
    except:
        print("collection_dailyFlow 数据转存失败")
    print results
'''

def dailyflow(collection_flow,collection_distinct):
    total = collection_distinct.count()
    wifi = collection_distinct.find({"phone":{'$exists':True}}).count()
    d1 = datetime.datetime.now()
    d2 = datetime.timedelta(days=1)
    date = datetime.datetime.strftime(d1-d2,"%Y-%m-%d")
    startDate = date
    endDate = datetime.datetime.strftime(d1,"%Y-%m-%d")
    TotalVistors = totalVistorsYesterday(startDate,endDate)
    doc = {
        'DateTime':date,
        'CaptureVistors':total,
        'TotalVistors':TotalVistors,
        'WifiUsers':wifi
    }
    collection_flow.insert_one(doc)
    collection_flow.create_index("DateTime")

def shopinfo(collection,ts01,ts02):
    shopname,shopaddress = "",""
    for doc in collection.find({"ts":{"$gt":ts01,"$lte":ts02}}):
        floorid,x,y = doc['floorid'],doc['x'],doc['y']
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
            doc['shopname'] = shopname
            doc['shopaddress'] = shopaddress
            collection.save(doc)

class MyThread(Thread):
    def __init__(self, collection,ts01,ts02):
        super(MyThread, self).__init__()
        self.collection = collection
        self.ts01 = ts01
        self.ts02 = ts02
    def run(self):
        shopinfo(self.collection, self.ts01,self.ts02)
if __name__ == '__main__':
    d1 = datetime.datetime.now()
    d2 = datetime.timedelta(days=1)
    dbname = "location" + "-" + datetime.datetime.strftime(d1-d2,"%Y%m%d")
    collection_distinct = mongoConn("172.31.1.137",dbname,"mac_phone")
    if (len(sys.argv) == 1):
        print ('''
            1.distinct
              Get distinct mac and position count.
            2.radius
              Get phone number from Radius Server for distinct mac.
            3.flow
              Get the dailyflow of Vistors.
            4.shopinfo
              Add shopin into raw data.
        ''')
    elif (sys.argv[1] == 'distinct'):
        #time_str = datetime.datetime.strftime(d1-d2,"%Y-%m-%d") + ' ' + "17:00:00"
        #ts = timeArray = time.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        #timestamp = int(time.mktime(timeArray)*1000)
        collection_Raw = mongoConn("172.31.1.137",dbname,"raw_data")
        #mapReduce(collection_Raw,timestamp)
        #mongo_count(collection_Raw)
        mongo_distinct(collection_Raw,collection_distinct)
        collection_distinct.create_index("mac")
    elif (sys.argv[1] == 'radius'):
        collection_Radius = mongoConn("172.31.1.10","radius","rad_EndUser")
        portalInterface(collection_distinct,collection_Radius)
    elif (sys.argv[1] == 'flow'):
        collection_flow = mongoConn("172.31.1.137","bi","dailyflow")
        dailyflow(collection_flow,collection_distinct)
    elif (sys.argv[1] == 'shopinfo'):
        collection_Raw = mongoConn("172.31.1.137",dbname,"raw_data")
        time_tmp = datetime.datetime.strftime(d1-d2,"%Y-%m-%d")+ ' ' + "09:00:00"
        ts = int(time.mktime(time.strptime(time_tmp,"%Y-%m-%d %H:%M:%S"))) #当天9点为分割点
        ts02 = ts + int(sys.argv[2])
        ts01 = ts02 - 1800
        shopinfo(collection_Raw,ts01*1000,ts02*1000)
        '''
        for num in range(13):
            ts01 = ts * 1000
            ts = ts + 3600
            try:
               a = MyThread(collection_Raw,ts01,ts*1000)
               a.start()
            except:
               print "线程启动失败！"
               '''
    else:
         print ('''
        1.distinct
          Get distinct mac and position count.
        2.radius
          Get phone number from Radius Server for distinct mac.
        3.flow
          Get the dailyflow of Vistors.
        4.shopinfo
          Add shopin into raw data.
        ''')




