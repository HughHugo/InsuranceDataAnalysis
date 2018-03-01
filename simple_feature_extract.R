args <- commandArgs(trailing = TRUE)   
if (length(args) != 2) {  
  print("Please input the start date;")
  print("Please input the last date.")
  q()
}
start.date <- args[[1]]
last.date <- args[[2]]
days <- as.character(format(seq(from=as.Date(start.date), to=as.Date(last.date), by='day'),'%Y%m%d'))

library(SparkR)
library(magrittr)
sparkR.session.stop()

sc <- sparkR.session("yarn-client", "ciitc",
                     sparkConfig=list(spark.executor.memory="40g",
                                      spark.executor.cores="12",
                                      spark.network.timeout="3600",
                                      spark.sql.autoBroadcastJoinThreshold="104857600"))
# SparkR:::includePackage(sqlContext, 'lubridate')
suppressPackageStartupMessages(library(lubridate))

for (i in 1:length(days)){
  gc()
  
  sql("DROP TABLE IF EXISTS trip_stat_temp")
  sql(paste("CREATE EXTERNAL TABLE trip_stat_temp (", 
            "deviceid STRING,", 
            "tid STRING,", 
            "vid STRING,", 
            "start INT,", 
            "actual_start INT,", 
            "s_end INT,", 
            "dura DOUBLE,", 
            "period INT,", 
            "lat_st_ori DOUBLE,", 
            "lon_st_ori DOUBLE,", 
            "lat_en_ori DOUBLE,", 
            "lon_en_ori DOUBLE,", 
            "m_ori DOUBLE,", 
            "lat_st_def DOUBLE,", 
            "lon_st_def DOUBLE,", 
            "lat_en_def DOUBLE,", 
            "lon_en_def DOUBLE,", 
            "m_def DOUBLE,", 
            "speed_mean DOUBLE,", 
            "gps_speed_sd DOUBLE,", 
            "gps_acc_sd DOUBLE", 
            ")", 
            "ROW format delimited FIELDS TERMINATED BY \',\'", 
            paste("LOCATION \'/user/kettle/obdbi/original/trip_stat/stat_date=", days[i], "\'", sep = ''), 
            sep = '\n'))
  day.stat <- sql("select tid,vid,deviceid,start,s_end from trip_stat_temp")
  # day.stat <- SparkR:::filter(day.stat, day.stat$vid %in% as.character(250000:300000))
  sql("DROP TABLE IF EXISTS trip_stat_temp")
  sql("DROP TABLE IF EXISTS gps_data_temp")
  sql(paste("CREATE EXTERNAL TABLE gps_data_temp (", 
            "imei STRING,", 
            "lat double,",
            "lon double,", 
            "alat double,", 
            "alon double,", 
            "speed double,", 
            "direction int,", 
            "altitude int,", 
            "pdop double,", 
            "time_stamp bigint,", 
            "protocal_type int", 
            ")", 
            "STORED AS PARQUET", 
            paste("LOCATION \'/user/kettle/obdbi/original/obd_gps_second_parquet/stat_date=", days[i], "\'", sep = ''), 
            sep = '\n'))
  gps.data <- sql("SELECT * FROM gps_data_temp")
  sql("DROP TABLE IF EXISTS gps_data_temp")
  gps.data$time_stamp <- gps.data$time_stamp/1000
  
  
  createOrReplaceTempView(gps.data, "gps_data")
  createOrReplaceTempView(day.stat, "day_stat")
  
  day.data <- sql("SELECT day_stat.tid,day_stat.vid,day_stat.start as tripnumber,day_stat.s_end,gps_data.* FROM gps_data RIGHT JOIN day_stat on gps_data.imei=day_stat.deviceid and gps_data.time_stamp>=day_stat.start and gps_data.time_stamp<=day_stat.s_end")
  
  # day.data <- SparkR:::join(day.stat, gps.data, day.stat$deviceid == gps.data$imei & day.stat$start <= gps.data$time_stamp & day.stat$s_end >=gps.data$time_stamp)
  # names(day.data) <- c('tid','vid','imei_stat','tripnumber','s_end','imei','lat','lon','alat','alon','speed','direction','altitude','pdop','time_stamp','pro_type')
  # createOrReplaceTempView(day.data, "day_data")
  
  day.data <- SparkR:::filter(day.data, day.data$tripnumber > 0)
  
  createOrReplaceTempView(day.data, "day_data")
  
  day.data <- sql(paste("SELECT * , LEAD(lat, 1, 0) OVER (PARTITION BY imei,tripnumber ORDER BY time_stamp) AS lat_0, ", 
                        "LEAD(lon, 1, 0) OVER (PARTITION BY imei,tripnumber ORDER BY time_stamp) AS lon_0 ", 
                        "FROM day_data", sep = ''))
  
  GPSDist <- function(Lat,Lon,Lat0,Lon0){
    Lat <- Lat*pi/180
    Lon<-Lon*pi/180
    Lat0<-Lat0*pi/180
    Lon0<-Lon0*pi/180
    m <- asin(sqrt(cos(Lat0) * cos(Lat) * (sin((Lon-Lon0) / 2)) ^ 2 + (sin((Lat - Lat0) / 2)) ^ 2)) *2*6378.178
    return(m)
  }
  
  
  day.data$am <- GPSDist(day.data$lat, day.data$lon, day.data$lat_0, day.data$lon_0)
  day.data <- SparkR:::filter(day.data, day.data$am < 10)
  
  day.data$period <- SparkR:::from_unixtime(day.data$time_stamp, 'HH')
  
  
  eval(parse(text = paste("day.data$period_", 0:23, " <- SparkR:::ifelse(day.data$period==", 0:23, ",day.data$am,0)", sep = '')))
  
  eval(parse(text = paste("day.data$speed_", 0:20, " <- SparkR:::ifelse(ceil(day.data$speed/10)==", 0:20, ",1,0)", sep = '')))
  day.data$speed_21 <- SparkR:::ifelse(day.data$speed > 200, 1, 0)
  
  day.data <- repartition(day.data, numPartitions	= 1000, col = day.data$'imei')
  
  trip.feature <- summarize(groupBy(day.data, "imei", "vid", "tripnumber"), 
                            start_time = min(day.data$time_stamp), 
                            end_time = max(day.data$time_stamp), 
                            dura = max(day.data$s_end) - min(day.data$tripnumber), 
                            dura_valid = n_distinct(day.data$time_stamp), 
                            lat_st = first(day.data$lat), 
                            lon_st = first(day.data$lon), 
                            lat_end = last(day.data$lat), 
                            lon_end = last(day.data$lon), 
                            mileage = sum(day.data$am), 
                            m_0 = sum(day.data$period_0), 
                            m_1 = sum(day.data$period_1), 
                            m_2 = sum(day.data$period_2), 
                            m_3 = sum(day.data$period_3), 
                            m_4 = sum(day.data$period_4), 
                            m_5 = sum(day.data$period_5), 
                            m_6 = sum(day.data$period_6), 
                            m_7 = sum(day.data$period_7), 
                            m_8 = sum(day.data$period_8), 
                            m_9 = sum(day.data$period_9), 
                            m_10 = sum(day.data$period_10), 
                            m_11 = sum(day.data$period_11), 
                            m_12 = sum(day.data$period_12), 
                            m_13 = sum(day.data$period_13), 
                            m_14 = sum(day.data$period_14), 
                            m_15 = sum(day.data$period_15), 
                            m_16 = sum(day.data$period_16), 
                            m_17 = sum(day.data$period_17), 
                            m_18 = sum(day.data$period_18), 
                            m_19 = sum(day.data$period_19), 
                            m_20 = sum(day.data$period_20), 
                            m_21 = sum(day.data$period_21), 
                            m_22 = sum(day.data$period_22), 
                            m_23 = sum(day.data$period_23),
                            speed_0 = sum(day.data$speed_0), 
                            speed_0_10 = sum(day.data$speed_1), 
                            speed_10_20 = sum(day.data$speed_2), 
                            speed_20_30 = sum(day.data$speed_3), 
                            speed_30_40 = sum(day.data$speed_4), 
                            speed_40_50 = sum(day.data$speed_5), 
                            speed_50_60 = sum(day.data$speed_6), 
                            speed_60_70 = sum(day.data$speed_7), 
                            speed_70_80 = sum(day.data$speed_8), 
                            speed_80_90 = sum(day.data$speed_9), 
                            speed_90_100 = sum(day.data$speed_10), 
                            speed_100_110 = sum(day.data$speed_11), 
                            speed_110_120 = sum(day.data$speed_12), 
                            speed_120_130 = sum(day.data$speed_13), 
                            speed_130_140 = sum(day.data$speed_14), 
                            speed_140_150 = sum(day.data$speed_15), 
                            speed_150_160 = sum(day.data$speed_16), 
                            speed_160_170 = sum(day.data$speed_17), 
                            speed_170_180 = sum(day.data$speed_18), 
                            speed_180_190 = sum(day.data$speed_19), 
                            speed_190_200 = sum(day.data$speed_20), 
                            speed_200_Inf = sum(day.data$speed_21)
  )
  
  
  trip.feature.sc <- coalesce(trip.feature, 100)
  write.parquet(trip.feature.sc, paste("/user/kettle/obdbi/original/ciitc/all_users/stat_data=", days[i], sep = ''), mode = 'overwrite')
  
  gc()
}
sparkR.session.stop()
