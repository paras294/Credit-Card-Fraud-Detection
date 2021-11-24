# Importing required function and libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Establishing Spark Session
spark = SparkSession  \
        .builder  \
        .appName("CapStone_Project")  \
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
sc = spark.sparkContext

sc.addPyFile('db/dao.py')
sc.addPyFile('db/geo_map.py')
sc.addFile('rules/rules.py')

import dao
import geo_map
import rules

# Reading data from Kafka & Topic given	
lines = spark  \
        .readStream  \
        .format("kafka")  \
        .option("kafka.bootstrap.servers","18.211.252.152:9092")  \
        .option("subscribe","transactions-topic-verified")  \
        .option("failOnDataLoss","false").option("startingOffsets", "earliest")  \
        .load()

# Schema definition to read data properly
schema =  StructType([
                StructField("card_id", StringType()),
                StructField("member_id", StringType()) ,
                StructField("amount", IntegerType()),
                StructField("pos_id", StringType()),
                                StructField("postcode", StringType()),
                                StructField("transaction_dt", StringType())
            ])

# Casting raw data as string and aliasing    
Readable = lines.select(from_json(col("value") \
                                    .cast("string") \
                                    ,schema).alias("parsed"))

# Parsed df
parsed_df = Readable.select("parsed.*")
parsed_df = parsed_df.withColumn('transaction_dt_ts',unix_timestamp(parsed_df.transaction_dt, 'dd-MM-YYYY HH:mm:ss').cast(TimestampType()))

def score_data(a):
	hdao = dao.HBaseDao.get_instance()
	#hdao_ = ast.literal_eval(hdao_bc)
	data_fetch = hdao.get_data(key=a,table='look_up_table')
	return data_fetch['info:score']

Score_udf = udf(score_data,StringType())
parsed_df = parsed_df.withColumn("score",Score_udf(parsed_df.card_id))

def postcode_data(a):
	hdao = dao.HBaseDao.get_instance()
	#hdao_ = ast.literal_eval(hdao_bc)
	data_fetch = hdao.get_data(key=a,table='look_up_table')
	return data_fetch['info:postcode']


postcode_udf = udf(postcode_data,StringType())
parsed_df = parsed_df.withColumn("last_postcode",postcode_udf(parsed_df.card_id))

def lTransD_data(a):
	hdao = dao.HBaseDao.get_instance()
	data_fetch = hdao.get_data(key=a,table='look_up_table')
	return data_fetch['info:transaction_date']


lTransD_udf = udf(lTransD_data,StringType())
parsed_df = parsed_df.withColumn("last_transaction_date",lTransD_udf(parsed_df.card_id))
parsed_df = parsed_df.withColumn('last_transaction_date_ts',unix_timestamp(parsed_df.last_transaction_date, 'YYYY-MM-dd HH:mm:ss').cast(TimestampType()))

def ucl_data(a):
	hdao = dao.HBaseDao.get_instance()
	data_fetch = hdao.get_data(key=a,table='look_up_table')
	return data_fetch['info:UCL']


UCL_udf = udf(ucl_data,StringType())
parsed_df = parsed_df.withColumn("UCL",UCL_udf(parsed_df.card_id))


#Distance calculation
def distance_calc(last_postcode,postcode):
	gmap = geo_map.GEO_Map.get_instance()
	last_lat = gmap.get_lat(last_postcode)
	#last_lat = round(last_lat.values[0],2)
	last_lon = gmap.get_long(last_postcode)
	#last_lon = round(last_lon.values[0],2)
	lat = gmap.get_lat(postcode)
	#lat = round(lat.values[0],2)
	lon = gmap.get_long(postcode)
	#lon = round(lon.values[0],2)
	final_dist = gmap.distance(last_lat.values[0],last_lon.values[0],lat.values[0],lon.values[0])
	return final_dist

distance_udf = udf(distance_calc,DoubleType())
parsed_df = parsed_df.withColumn("distance",distance_udf(parsed_df.last_postcode,parsed_df.postcode))

# Time calculation
def time_cal(last_date, curr_date):	
	diff= curr_date-last_date
	return (diff.total_seconds())/3600

def lTransD_data(a):
	hdao = dao.HBaseDao.get_instance()
	data_fetch = hdao.get_data(key=a,table='look_up_table')
	return data_fetch['info:transaction_date']


lTransD_udf = udf(lTransD_data,StringType())
parsed_df = parsed_df.withColumn("last_transaction_date",lTransD_udf(parsed_df.card_id))
time_udf = udf(time_cal,DoubleType())
parsed_df = parsed_df.withColumn('transaction_dt_ts',unix_timestamp(parsed_df.transaction_dt, 'dd-MM-YYYY HH:mm:ss').cast(TimestampType()))
parsed_df = parsed_df.withColumn('last_transaction_date_ts',unix_timestamp(parsed_df.last_transaction_date, 'YYYY-MM-dd HH:mm:ss').cast(TimestampType()))
parsed_df = parsed_df.withColumn('time_taken',time_udf(parsed_df.last_transaction_date_ts,parsed_df.transaction_dt_ts))

# Speed calculation
def speed_cal(dist,time):
	if time<0:
		time=time*(-1)
	if time==0:
		return -1.0
	return (dist)/time
	
speed_udf = udf(speed_cal, DoubleType())

parsed_df = parsed_df.withColumn('speed',speed_udf(parsed_df.distance,parsed_df.time_taken))

def status_find(card_id,member_id,amount,pos_id,postcode,transaction_dt,transaction_dt_ts,last_transaction_date_ts,score,speed):
        hdao = dao.HBaseDao.get_instance()
        geo = geo_map.GEO_Map.get_instance()
        look_up = hdao.get_data(key=card_id,table='look_up_table')
        status = 'FRAUD'
        if rules.rules_check(data_fetch['info:UCL'],score,speed,amount):
                
				status= 'GENUINE'
				data_fetch['info:transaction_date'] = str(transaction_dt_ts)
				data_fetch['info:postcode']=str(postcode)
				hdao.write_data(card_id,data_fetch,'look_up_table')
				
        row = {'info:postcode':bytes(postcode),'info:pos_id':bytes(pos_id),'info:card_id':bytes(card_id),'info:amount':bytes(amount),'info:transaction_dt':bytes(transaction_dt),'info:member_id':bytes(member_id),'info:status':bytes(status)}
        key = '{0}.{1}.{2}.{3}'.format(card_id,member_id,str(transaction_dt),str(datetime.now())).replace(" ","").replace(":","")
        hdao.write_data(bytes(key),row,'card_transactions')
        return status

status_udf = udf(status_find,StringType())
parsed_df = parsed_df.withColumn('status',status_udf(parsed_df.card_id,parsed_df.member_id,parsed_df.amount,parsed_df.pos_id,parsed_df.postcode,parsed_df.transaction_dt,parsed_df.transaction_dt_ts,parsed_df.last_transaction_date_ts,parsed_df.score,parsed_df.speed))

parsed_df = parsed_df.select("card_id","member_id","amount","pos_id","postcode","transaction_dt_ts","status")

query1 = parsed_df \
        .writeStream  \
        .outputMode("append")  \
        .format("console")  \
        .option("truncate", "False")  \
        .start()

# query termination command
query1.awaitTermination()