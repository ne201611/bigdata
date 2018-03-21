# -*- coding=utf-8 -*-
'''
发行说明
    1.2版本
        #新增楼层客流量统计
'''
from __future__ import print_function
import datetime,time,os
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit

def get_csv_filename():
    rootpath = "/opt/csv/"
    dir_sub = "location-" + now.strftime("%Y%m%d")
    filename_sub = "location-" + now.strftime("%Y%m%d") + "/location-" + (now - oneHour).strftime("%Y%m%d-%H") + ".csv"
    dir = os.path.join(rootpath, dir_sub)
    filename = os.path.join(rootpath,filename_sub)
    return dir,filename

def spark_func():
    spark = SparkSession.builder \
        .appName("handle_csv_file") \
        .config("spark.mongodb.input.uri", "mongodb://172.31.1.143/location.raw_data") \
        .config("spark.mongodb.output.uri", "mongodb://172.31.1.143/location.raw_data") \
        .getOrCreate()
    schema = StructType([
        StructField("idtype", StringType(), True),
        StructField("mac", StringType(), True),
        StructField("floorid", IntegerType(), True),
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True),
        StructField("ts", LongType(), True),
        StructField("mallid", IntegerType(), True),
        StructField("other", StringType(), True)
    ])
    csv_file_path,csv_file_name = get_csv_filename()
    if os.path.isfile(csv_file_name):
        df_raw = spark.read.csv(csv_file_name, schema)
        df_raw.registerTempTable("location")
        #中间DF，为了筛选出有效Mac
        df_count = spark.sql("select mac,count(mac) as num from location group by mac")
        df_filter = df_count.filter("num >= 50")
        #从海量原始坐标点位信息点，筛选出有效mac的坐标信息点，并入库
        df_vaild = df_raw.join(df_filter,df_raw["mac"] == df_filter["mac"]).withColumn('datetime',lit(date_time_LOCAL)) #datetime字段为了使用Mongodb ttl
        df_vaild.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()
        #生成该时段，有效的mac，入库到users集合中
        df_users = df_filter.select('mac').withColumn('datetime',lit(date_time)).withColumn('start_ts',lit(start_ts))
        df_users.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","historical")\
            .option("collection","users").save()
        #加载当天所有csv文件，统计当天楼层客流量
        df_raw_init = spark.read.csv((csv_file_path + '/*.csv'), schema)
        pipeline = {'$match':{'datetime':date_time}}
        df_vaild_mac = spark.read.format("com.mongodb.spark.sql.DefaultSource")\
            .option("uri","mongodb://172.31.1.143/historical.users")\
            .option("pipeline", pipeline).load()
        df_raw_fin = df_raw_init.join(df_vaild_mac,df_raw_init["mac"] == df_vaild_mac["mac"])\
            .select(df_raw_init.floorid,df_raw_init.mac)
        df_raw_fin.registerTempTable("location")
        df_floor_flow= spark.sql("select floorid,count(distinct(mac)) as num from location group by floorid")
        floor_flow = df_floor_flow.toJSON().collect()
        floor_flow_convert = []
        for each in floor_flow:
            floor_flow_convert.append(eval(each))
        dic ={
            'datetime':date_time,
            'floor_flow':floor_flow_convert
        }
        get_mongo_collection('172.31.1.137','bi','floor_Flow').update({'datetime':date_time},dic,upsert=True)
        # 停止SparkSession
        spark.stop()
    else:
        print("file not exists!!!")
def get_mongo_collection(ip,dbName,collectionName):
    try:
        client = MongoClient("mongodb://%s:27017/"%(ip))
        db = client[dbName]
        if (ip == '172.31.1.10'):
            db.authenticate('root','111111')
        collection = db[collectionName]
        return collection
    except:
        print ("数据库连接异常")

if __name__ == "__main__":
    now = datetime.datetime.now()
    oneHour = datetime.timedelta(hours=1)
    date_time = now.strftime('%Y-%m-%d')
    UTC_OFFSET = datetime.timedelta(hours=8)
    date_time_LOCAL = now + UTC_OFFSET
    str = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:00:00")
    start_ts = (time.mktime(datetime.datetime.strptime(str, '%Y-%m-%d %H:00:00').timetuple())) * 1000 - (1 * 3600000)  # 前一个时间整点
    spark_func()