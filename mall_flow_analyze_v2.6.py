#date:2017-08-11
#author:wujiyong

# -*- coding=utf-8 -*-
from module.bi import *
import time
from multiprocessing import Process,Pool,Manager

'''
    v2.1版本，shopflow、usertrack采用每小时结果累加的方式，代替现有全天计算，加块统计效率。
    v2.2版本，改变用户轨迹数据结构.
    v2.3版本，mallflow使用set集合内置方法，计算交集。
    v2.4版本，轨迹列表mac列表，从shopflow读取，废除res_data获取.
    v2.5版本
        1) 优化停留时间算法，去除跳动点.
        2) 按每小时入库有效mac，后续商场、店铺客流量全部来源于mac的子集。
        3) shopflow来源于usertrack.
        4) mallflow中，wifiusers更正为成功连接人数
    v2.6版本
        1) 引入spark大数据分析框架

'''
interval = 300000   #位置ts差大于5分钟，则认为二次到店铺
sequential_num = 3  #连续出现3次，则认为是一次到店铺

@deco
def mallflow():
    #商场客流量统计
    #阶段一：统计当天位移点超过5次的终端，update的方式入库到userinfo表
    collection_raw = get_mongo_collection("172.31.1.143", "location", "raw_data")
    collection_userinfo = get_mongo_collection("172.31.1.143", 'historical', "users")
    collection_dailyflow = get_mongo_collection("172.31.1.137", "bi", "dailyflow")
    collection_radius = get_mongo_collection("172.31.1.137", "config", "rad_EndUser")
    collection_radius_prod = get_mongo_collection("172.31.1.10", "radius", "sys_RadiusVerifyLog")
   #阶段二：生成最终到店人数、上网人数 dailyflow报表，
    total_mac = collection_userinfo.find({'datetime':date_time}).distinct('mac')
    num = collection_userinfo.find({"start_ts":{'$eq':start_ts}}).count()
    crm_mac_tmp = collection_radius.find({'vip':'Y'}).distinct('endUserMac')
    crm_mac = [i.replace('-', ':') for i in crm_mac_tmp]
    total = len(total_mac)
    wifi = wifi_has_connected(date_time,collection_radius_prod)
    vip = len(set(crm_mac) & set(total_mac))
    doc = {
        'CaptureVistors':total,
        'WifiUsers':wifi,
        'VIPUsers':vip,
    }
    collection_dailyflow.update_one({'DateTime':date_time},{'$set':doc,'$push':{'hour_visitors':{ '$each': [{datetime.datetime.strftime(datetime.datetime.fromtimestamp(start_ts/1000),"%H:%M"):num}]}}},upsert=True)
    info = "【完成商场客流量分析】"
    return info

@deco
def shopflow():
    #店铺客流量统计
    collection_usertrack = get_mongo_collection("172.31.1.137", "bi", "usertrack")
    collection_shopflow = get_mongo_collection("172.31.1.137", "bi", "shopflow")
    collection_shopinfo = get_mongo_collection("172.31.1.137", 'config', "shopinfo")
    #phrase 1： 读取shopid列表
    shop_list,num = [],0
    cusor = collection_shopinfo.find({'tag':1})
    for doc in cusor:
        shop_list.append({"shopid":doc['id'],"category":doc['category'],"floorid":doc['floorid']})
    for dic in shop_list:
        num += 1
        list_mac = collection_usertrack.find({'datetime':date_time,'shopid':dic['shopid']}).distinct('mac')
        dic01 = {
            'category':dic['category'],
            'floorid':dic['floorid'],
            'usercount': len(list_mac),
            'mac': list_mac
        }
        collection_shopflow.update_one({'shopid':dic['shopid'],'datetime':date_time},{'$set':dic01},upsert=True)
    info = "【店铺客流明细统计】店铺数:%d"%(num)
    return info

@deco
def userTrack():
    #统计、分析用户店铺轨迹列表
    collection_res = get_mongo_collection("172.31.1.143", 'location', "res_data")
    collection_usertrack = get_mongo_collection("172.31.1.137", "bi", "usertrack")
    collection_shopinfo = get_mongo_collection("172.31.1.137", 'config', "shopinfo")
    shop_list, num = [], 0
    shopinfo_cusor = collection_shopinfo.find({'tag': 1})
    shopinfo_trip = [(doc['id'],doc['floorid'],doc['category']) for doc in shopinfo_cusor]
    mac_list_onehour = collection_res.find({'ts':{'$gte':start_ts}}).distinct('mac')
    for mac in mac_list_onehour:
        num += 1
        shopid_list_onehour = collection_res.find({'mac':mac,'ts':{'$gte':start_ts}}).distinct('shopid')
        for shopid in shopid_list_onehour:
            index,flag,count,shopid_ts_finnal = 0,0,0,[]
            shopid_ts = collection_res.find({'mac': mac, 'shopid':shopid, 'ts': {'$gte': start_ts}}).distinct('ts')
            diff = [j-i for i,j in zip(shopid_ts[:-1],shopid_ts[1:]) ]
            #二个位置点，时间戳不大于5分钟，且有连续的3个点。则算一次正常的停留
            while (index < len(diff)):
                if (diff[index] > interval and (index-flag) >= sequential_num):
                    shopid_ts_finnal.append(shopid_ts[flag:(index + 1)])
                    flag = index + 1
                #判断是否遍历到了列表末尾，
                if(index == (len(diff)-1) and (index-flag) >= sequential_num):
                    shopid_ts_finnal.append(shopid_ts[flag:(index + 1)])
                    break
                index += 1
            if (len(shopid_ts_finnal) > 0):
                res = collection_usertrack.find_one({'mac': mac, 'shopid':shopid, 'datetime': date_time})
                if res:
                    period_list, dwelltime = res['period'], res['dwelltime']
                    for shopid_ts_item in shopid_ts_finnal:
                        # 判断时间戳差是否足1分钟，则按1分钟统计
                        if (max(shopid_ts_item) - min(shopid_ts_item) <= 60000):
                            max_ts = max(shopid_ts_item)
                            if (max_ts - res['max_ts']) >= 300000: #邻近2小时，跨小时差内间隔大于5分钟,则认为是二次到店
                                period = timestamp2string(min(shopid_ts_item)) + '-' + timestamp2string(max(shopid_ts_item))
                                period_list.append(period)
                            else:
                                period_list[-1] = period_list[-1].split('-')[0] + '-' + timestamp2string(max_ts)
                            dwelltime = res['dwelltime'] + 1
                        else:
                            # dwell_dic_shop ={'shopid':shopid,'dwelltime':(max(ts_list_shopid) - min(ts_list_shopid))//60000}
                            max_ts = max(shopid_ts_item)
                            if (max_ts - res['max_ts']) >= 300000:  # 间隔大于5分钟
                                period = timestamp2string(min(shopid_ts_item)) + '-' + timestamp2string(max(shopid_ts_item))
                                period_list.append(period)
                            else:
                                period_list[-1] = period_list[-1].split('-')[0] + '-' + timestamp2string(max_ts)
                            dwelltime += (max(shopid_ts_item) - min(shopid_ts_item)) // 60000
                    doc = {
                        'dwelltime':dwelltime,
                        'period':period_list,
                        'max_ts':max_ts
                    }
                    collection_usertrack.update_one({'mac':mac,'shopid':shopid,'datetime':date_time},{'$set':doc})
                else:
                    period_list, dwelltime = [], 0
                    for shopid_ts_item in shopid_ts_finnal:
                        if (max(shopid_ts_item) - min(shopid_ts_item) <= 60000):  # 时差不足1分钟，则按1分钟统计
                            period = timestamp2string(min(shopid_ts_item)) + '-' + timestamp2string(max(shopid_ts_item))
                            dwelltime += 1
                        else:
                            # dwell_dic_shop ={'shopid':shopid,'dwelltime':(max(ts_list_shopid) - min(ts_list_shopid))//60000}
                            period = timestamp2string(min(shopid_ts_item)) + '-' + timestamp2string(max(shopid_ts_item))
                            dwelltime += ((max(shopid_ts_item) - min(shopid_ts_item)) // 60000)
                        period_list.append(period)
                    for trip in shopinfo_trip:
                        if (trip[0] == shopid):
                            floorid,category = trip[1],trip[2]
                            break
                    doc = {
                        'mac': mac,
                        'datetime': date_time,
                        'shopid': shopid,
                        'floorid':floorid,
                        'category':category,
                        'dwelltime': dwelltime,
                        'period': period_list,
                        'max_ts':max(shopid_ts_item)
                    }
                    collection_usertrack.insert_one(doc)
    info = "【完成用户停留时间分析】用户数：%d" % (num)
    return info

@deco
def shopinfo():
    #原始定位坐标数据----->店铺id
    manager = Manager()
    count_list = manager.list()
    for i in range(2):
        count_list.append(0)
    dic_poi = createCache(start_ts)
    process_record = []
    floorids = [956505, 956575, 956668, 956733, 956758, 956789, 1145927, 1146008]
    for floorid in floorids:
        try:
            d = Process(target=shopinfo_inner, args=(dic_poi,floorid,date_time_local,start_ts,count_list))
            process_record.append(d)
            d.start()
        except Exception as e:
            print("进程启动失败！\n", e)
    for process in process_record:
        process.join()
    info = "【完成坐标到店铺转换】位置总数:%d，匹配成功:%d"%(count_list[0],count_list[1])
    return info

@deco
def loyalty():
    #忠诚度统计
    mallLoyalty(date_time)
    shop_list = get_shopList()
    pool = Pool(processes=5)
    for shopid_tmp in shop_list:
        shopid = shopid_tmp
        pool.apply_async(getShopFlowToday, (shopid,date_time,))
    pool.close()
    pool.join()
    info = '【完成商场+店铺忠诚度分析】店铺数：%d'%(len(shop_list))
    return info

if __name__ == '__main__':
    now = datetime.datetime.now()
    oneHour = datetime.timedelta(hours=1)
    UTC_OFFSET = datetime.timedelta(hours=8)
    date_time_local = now + UTC_OFFSET
    date_time = now.strftime('%Y-%m-%d')
    str = datetime.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d %H:00:00")
    start_ts = (time.mktime(datetime.datetime.strptime(str,'%Y-%m-%d %H:00:00').timetuple()))*1000 - (1*3600000) #前一个时间整点
    mallflow()  #phrase 2 统计广场日客流
    shopinfo()  #phrase 3 匹配POI店铺信息
    userTrack()  # phrase 5 跟踪用户轨迹列表
    shopflow()  #phrase 4 统计店铺客流明细
    loyalty()   #统计客流量

