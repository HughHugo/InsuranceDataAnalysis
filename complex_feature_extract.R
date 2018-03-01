args <- commandArgs(trailing = TRUE)   
if (length(args) != 3) {  
  print("Please input the start date;")
  print("Please input the last date;")
  print('Please input execute mode (default/prod).')
  q()
}
start.date <- args[[1]]
last.date <- args[[2]]
execute.mode <- args[[3]]

days <- as.character(format(seq(from=as.Date(start.date), to=as.Date(last.date), by='day'),'%Y%m%d'))

library(SparkR)
library(magrittr)
sparkR.session.stop()


sc <- sparkR.session("yarn-client", "ciitc", 
                     sparkConfig=list(spark.executor.memory="40g", 
                                      spark.executor.cores="12", 
                                      spark.r.backendConnectionTimeout="300",
                                      spark.network.timeout="3600", 
                                      spark.sql.autoBroadcastJoinThreshold="104857600", 
                                      spark.yarn.queue=execute.mode,
                                      spark.dynamicAllocation.enabled='FALSE'))



for (i in 1:length(days)){
  gc()
  while (hour(Sys.time()) %in% c(6, 7)){
    Sys.sleep(600)
    }
  
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
  day.stat <- sql("SELECT tid,vid,deviceid,start,s_end FROM trip_stat_temp")
  users.ciitc <- sql("SELECT * FROM users_ciitc")
  gps.data <- read.parquet(paste("/user/kettle/obdbi/original/obd_gps_second_parquet/stat_date=", days[i], sep = ''))
  
  gps.data$time_stamp <- gps.data$time_stamp/1000
  coltypes(gps.data) <- c("character","numeric","numeric","numeric","numeric","numeric",
                          "integer","integer","numeric","integer","integer") 
  
  users.stat <- SparkR:::join(users.ciitc, day.stat, users.ciitc$imei == day.stat$deviceid)
  day.data.join <- SparkR:::join(users.stat, gps.data, users.stat$imei == gps.data$imei & users.stat$start <= gps.data$time_stamp & users.stat$s_end >=gps.data$time_stamp)
  names(day.data.join) <- c('imei_ciitc','tid','vid','imei_stat','tripnumber','s_end','imei','lat','lon','alat','alon','speed','heading','altitude','pdop','time_stamp','pro_type')
  
  day.data <- select(day.data.join, c('imei','tid','vid','tripnumber','time_stamp','lat','lon','heading','speed'))
  day.data <- SparkR:::repartition(day.data, 2000, day.data$imei)
  
  arrange(day.data, "imei", "tripnumber", decreasing = c(FALSE, FALSE))
  
  
  day.data.rdd <- SparkR:::toRDD(day.data)
  
  day.features <- SparkR:::lapplyPartition(day.data.rdd, function(x){
    require(lubridate)
    GPSDist <- function(Lat, Lon, Lat0, Lon0){
      Lat <- Lat*pi/180;Lon<-Lon*pi/180;Lat0<-Lat0*pi/180;Lon0<-Lon0*pi/180
      m <- 2*6378.178*asin(sqrt(
        cos(Lat0)*cos(Lat)*(sin((Lon-Lon0)/2))^2+
          (sin((Lat-Lat0)/2))^2
      ))
      return(m)
    }
    GPS2XY<-function(lat, lon, lat0, lon0){
      yDist <- GPSDist(lat, lon0, lat0, lon0)
      xDist <- GPSDist(lat0, lon, lat0, lon0)
      cbind(X=xDist*sign(lon-lon0),Y=yDist*sign(lat-lat0))
    }
    IntervalTime <- function(series, name, min, max , gap, inf.plus = T, inf.minus = F){
      if (inf.plus & !inf.minus){
        interval.matrix <- cbind(seq(min,max,gap),c(seq(min+gap,max,gap),Inf))
      } else if (inf.plus & inf.minus){
        interval.matrix <- cbind(c(-Inf,seq(min,max,gap)),c(seq(min,max,gap),Inf))
      } else if (!inf.plus & inf.minus){
        interval.matrix <- cbind(c(-Inf,seq(min,max-gap,gap)),seq(min,max,gap))
      } else {
        interval.matrix <- cbind(seq(min,max-gap,gap),seq(min+gap,max,gap))
      }
      interval.time <- apply(interval.matrix, 1, function(x) length(which(series > x[1] & series <= x[2])))
      names(interval.time) <- paste(name, interval.matrix[,1], interval.matrix[,2], sep='_')
      return(interval.time)
    }
    IntervalTimes <- function(series, name, min, max , gap, threshold, inf.plus = T, inf.minus = F){
      series <- series[!is.na(series)]
      if (inf.plus & !inf.minus){
        interval.matrix <- cbind(seq(min,max,gap),c(seq(min+gap,max,gap),Inf))
      } else if (inf.plus & inf.minus){
        interval.matrix <- cbind(c(-Inf,seq(min,max,gap)),c(seq(min,max,gap),Inf))
      } else if (!inf.plus & inf.minus){
        interval.matrix <- cbind(c(-Inf,seq(min,max-gap,gap)),seq(min,max,gap))
      } else {
        interval.matrix <- cbind(seq(min,max-gap,gap),seq(min+gap,max,gap))
      }
      interval.times <- apply(interval.matrix,1,
                              function(x){
                                series.rle <- rle(as.numeric(series > x[1] & series <= x[2])) 
                                length(which(series.rle$length >= threshold & series.rle$values))
                              })                         
      names(interval.times) <- paste(name, interval.matrix[,1], interval.matrix[,2], 'times',sep='_')
      return(interval.times)
    }
    XY2radius <- function(ABCxy){
      Ax <- ABCxy[1];Ay <- ABCxy[2]
      Bx <- ABCxy[3];By <- ABCxy[4]
      Cx <- ABCxy[5];Cy <- ABCxy[6]
      D <- 2*(Ax*(By-Cy)+Bx*(Cy-Ay)+Cx*(Ay-By))
      Ux <-((Ax^2+Ay^2)*(By-Cy)+(Bx^2+By^2)*(Cy-Ay)+(Cx^2+Cy^2)*(Ay-By))/D
      Uy <-((Ax^2+Ay^2)*(Cx-Bx)+(Bx^2+By^2)*(Ax-Cx)+(Cx^2+Cy^2)*(Bx-Ax))/D
      R <- sqrt((Ax-Ux)^2+(Ay-Uy)^2)
      return(R)
    }
    XY2speedheading <- function(x,y){
      if (is.na(x) | is.na(y)){
        return(c(NA,NA))
      }
      speed <- sqrt(x^2+y^2)
      if (x > 0){
        angle <- pi/2-atan(y/x)
      } else if (x < 0){
        angle <- 3*pi/2-atan(y/x)
      } else if (x==0 & y>0){
        angle <- 0
      } else if (x==0 & y<0){
        angle <- pi
      } else{
        angle <- NA
      }
      angle <- angle*180/pi
      return(c(speed,angle))
    }
    XY2TanAcc <- function(ABCxy){
      Ax <- ABCxy[1];Ay <- ABCxy[2]
      Bx <- ABCxy[3];By <- ABCxy[4]
      Cx <- ABCxy[5];Cy <- ABCxy[6]
      speed.AB <- sqrt((Bx-Ax)^2 + (By-Ay)^2)
      speed.BC <- sqrt((Cx-Bx)^2 + (Cy-By)^2)
      tan.acc <- speed.BC-speed.AB
      return(tan.acc)
    }
    Radius.Acc <- function(LatLon){
      n <- nrow(LatLon)
      xy.matrix <- cbind(GPS2XY(LatLon[1:(n-2),'lat'], LatLon[1:(n-2),'lon'], LatLon[2:(n-1),'lat'], LatLon[2:(n-1),'lon'])*
                           1000/diff(LatLon[1:(n-1), 'time_stamp']), 
                         0, 
                         0, 
                         GPS2XY(LatLon[3:n,'lat'], LatLon[3:n,'lon'] , LatLon[2:(n-1),'lat'], LatLon[2:(n-1),'lon'])*
                           1000/diff(LatLon[2:n, 'time_stamp']))
      radius.acc <- rbind(NA, t(apply(xy.matrix, 1, function(x) c(XY2radius(x), XY2speedheading(x[5],x[6]), XY2TanAcc(x)))), NA)
      return(radius.acc)
    }
    Heading.Diff <- function(heading){
      heading.diff <- diff(heading)
      heading.unreasonable <- which(abs(heading.diff)>180)
      heading.diff[heading.unreasonable] <- heading.diff[heading.unreasonable]-sign(heading.diff[heading.unreasonable])*360
      return(heading.diff)
    }
    Norm.Acc <- function(angle.speed.r){
      norm.sign <- rep(1, nrow(angle.speed.r))
      norm.sign[which(angle.speed.r$angle.delta < 0 | 
                        (angle.speed.r$angle.delta == 0 & angle.speed.r$angle.cal.delta < 0))] <- -1
      norm.acc <- norm.sign*angle.speed.r$speed.cal^2/angle.speed.r$radius
      return(norm.acc)
    }
    Turn.Position.Times <- function(angle.delta, threshold, time = 1, times = F){
      if (threshold > 0){
        z <- rle(as.numeric(angle.delta > threshold))
      } else if (threshold < 0){
        z <- rle(as.numeric(angle.delta < threshold))
      }
      z$values[z$values==1 & z$lengths>=time] <- -1
      if (times == T){
        return(length(which(z$values == -1)))
      } else {
        turn.position <- which(inverse.rle(z) == -1)
        return(turn.position)
      }
    }
    
    
    Trip.Feature <- function(trip.data){
      require(lubridate)
      names(trip.data) <- c('imei','tid','vid','tripnumber','time_stamp','lat','lon','heading','speed')
      
      if (nrow(trip.data)<=15){
        trip.features <- rep(0, 284)
        return(trip.features)
      }
      trip.data <- trip.data[apply(trip.data,1,function(x) all(!is.na(x))),]
      trip.data <- trip.data[order(trip.data$time_stamp,decreasing = F),]
      trip.data <- unique(trip.data)
	  if (nrow(trip.data)<=15){
        trip.features <- rep(0, 284)
        return(trip.features)
      }
      n <- nrow(trip.data)
      trip.data$GPS_Acc <- c(diff(trip.data$speed)/diff(trip.data$time_stamp), 0)
      trip.data$am <- c(GPSDist(trip.data[2:nrow(trip.data), 'lat'], 
                                trip.data[2:nrow(trip.data), 'lon'], 
                                trip.data[1:(nrow(trip.data)-1), 'lat'], 
                                trip.data[1:(nrow(trip.data)-1), 'lon']), 
                        0)
      trip.data$period <- lubridate:::hour(as.POSIXlt(trip.data$time_stamp, origin='1970-01-01 00:00:00'))
      trip.names <- c(names(trip.data), 'radius', 'speed.cal', 'heading.cal', 'tan.acc')
      trip.data <- cbind(trip.data, Radius.Acc(trip.data[,c('time_stamp', 'lat', 'lon')]))
      names(trip.data) <- trip.names
      trip.data$angle.delta <- c(Heading.Diff(trip.data$heading)/diff(trip.data$time_stamp),0)
      trip.data$angle.cal.delta <- c(Heading.Diff(trip.data$heading.cal)/diff(trip.data$time_stamp),0)
      trip.data$norm.acc <- Norm.Acc(trip.data[,c('angle.delta', 'angle.cal.delta', 'speed.cal', 'radius')])
      trip.data$tan.acc <- trip.data$tan.acc/9.8
      trip.data$norm.acc <- trip.data$norm.acc/9.8
      turn.right.position <- Turn.Position.Times(trip.data$angle.delta, 3, 3)
      turn.left.position <- Turn.Position.Times(trip.data$angle.delta, -3, 3)
      turn.right.times <- Turn.Position.Times(trip.data$angle.delta, 3, 3, times = T)
      turn.left.times <- Turn.Position.Times(trip.data$angle.delta, -3, 3, times = T)
      trip.features <- c(
        trip.data[1, 'imei'], 
        trip.data[1, 'vid'], 
        trip.data[1, 'tripnumber'], 
        trip.data[1, 'time_stamp'], 
        trip.data[n, 'time_stamp'], 
        trip.data[n, 'time_stamp'] - trip.data[1, 'tripnumber'], 
        n, 
        trip.data[1, 'lat'], 
        trip.data[1, 'lon'], 
        trip.data[nrow(trip.data), 'lat'], 
        trip.data[nrow(trip.data), 'lon'], 
        GPSDist(trip.data[1,'lat'], trip.data[1,'lon'], trip.data[n,'lat'], trip.data[n,'lon']), 
        sum(trip.data$am, na.rm=T)/GPSDist(trip.data[1,'lat'], trip.data[1,'lon'], trip.data[n,'lat'], trip.data[n,'lon']), 
        sum(trip.data$am, na.rm=T), 
        unlist(lapply(0:23, function(x) sum(trip.data[which(trip.data$period == x), 'am']))), 
        mean(trip.data$speed, na.rm=T), 
        sd(trip.data$speed, na.rm=T), 
        quantile(trip.data$speed, na.rm = T, (0:10)/10), 
        length(which(trip.data$speed == 0)), 
        mean(trip.data[trip.data$speed > 0, 'speed'], na.rm = T), 
        sd(trip.data[trip.data$speed > 0, 'speed'], na.rm = T), 
        t(IntervalTime(trip.data$speed, name = 'speed', min = 0, max = 200, 
                       gap = 10, inf.plus = T, inf.minus = F)), 
        mean(trip.data$tan.acc, na.rm = T), 
        sd(trip.data$tan.acc, na.rm = T), 
        quantile(trip.data$tan.acc, na.rm = T, (0:10)/10), 
        length(which(trip.data$tan.acc > 0)), 
        length(which(trip.data$tan.acc < 0)), 
        mean(trip.data[trip.data$tan.acc > 0, 'tan.acc'], na.rm = T), 
        mean(trip.data[trip.data$tan.acc < 0, 'tan.acc'], na.rm = T), 
        sd(trip.data[trip.data$tan.acc < 0, 'tan.acc'], na.rm = T), 
        sd(trip.data[trip.data$tan.acc > 0, 'tan.acc'], na.rm = T), 
        quantile(trip.data[trip.data$tan.acc > 0, 'tan.acc'], na.rm = T, (0:10)/10), 
        quantile(trip.data[trip.data$tan.acc < 0, 'tan.acc'], na.rm = T, (0:10)/10), 
        IntervalTimes(trip.data[trip.data$tan.acc > 0, 'tan.acc'], name = 'acc_pos',  min = 0, max = 1, 
                      gap = 0.05, threshold = 1, inf.plus = T, inf.minus = F), 
        rev(IntervalTimes(trip.data[trip.data$tan.acc < 0, 'tan.acc'], name = 'acc_neg',  min = -1, 
                          max = 0, gap = 0.05, threshold = 2, inf.plus = F, inf.minus = T)), 
        length(turn.left.position) + length(turn.right.position), 
        turn.left.times + turn.right.times, 
        mean(trip.data[c(turn.right.position, turn.left.position), 'speed'], na.rm = T), 
        sd(trip.data[c(turn.right.position, turn.left.position), 'speed'], na.rm = T), 
        quantile(trip.data[c(turn.right.position, turn.left.position), 'speed'], na.rm = T, (0:10)/10), 
        length(turn.left.position), 
        turn.left.times, 
        mean(trip.data[turn.left.position, 'speed'], na.rm = T), 
        sd(trip.data[turn.left.position, 'speed'], na.rm = T), 
        quantile(trip.data[turn.left.position, 'speed'], na.rm = T, (0:10)/10), 
        length(turn.right.position), 
        turn.right.times, 
        mean(trip.data[turn.right.position, 'speed'], na.rm = T), 
        sd(trip.data[turn.right.position, 'speed'], na.rm = T), 
        quantile(trip.data[turn.right.position, 'speed'], na.rm = T, (0:10)/10), 
        mean(trip.data$norm.acc, na.rm = T), 
        sd(trip.data$norm.acc, na.rm = T), 
        quantile(trip.data$norm.acc, na.rm = T, (0:10)/10), 
        mean(trip.data[trip.data$norm.acc > 0, 'norm.acc'], na.rm = T), 
        mean(trip.data[trip.data$norm.acc < 0, 'norm.acc'], na.rm = T), 
        sd(trip.data[trip.data$norm.acc > 0, 'norm.acc'], na.rm = T), 
        sd(trip.data[trip.data$norm.acc < 0, 'norm.acc'], na.rm = T), 
        quantile(trip.data[trip.data$norm.acc > 0, 'norm.acc'], na.rm = T, (0:10)/10), 
        quantile(trip.data[trip.data$norm.acc < 0, 'norm.acc'], na.rm = T, (0:10)/10), 
        IntervalTimes(trip.data[trip.data$norm.acc > 0, 'norm.acc'], name = 'turn_right', min = 0, max = 1, 
                      gap = 0.05, threshold = 1, inf.plus = T, inf.minus = F), 
        rev(IntervalTimes(trip.data[trip.data$norm.acc < 0, 'norm.acc'], name = 'turn_left', min = -1, max = 0, 
                          gap = 0.05, threshold = 1, inf.plus = F, inf.minus = T)) 
      )
      return(trip.features)
    }
    
    
    part <- as.data.frame(matrix(unlist(x), ncol = 9,byrow = T), stringsAsFactors = F)
    if (nrow(part) < 60){
      trip.features <- matrix(0, ncol = 284, nrow = 1)
      return(trip.features)
    }
    names(part) <- c('imei','tid','vid','tripnumber','time_stamp','lat','lon','heading','speed')
    part <- apply(part, 2, as.numeric)
    part <- part[apply(part, 1, function(x) all(!is.na(x))),]
    part <- as.data.frame(part)
    part <- part[which(part$lat != 0),]
    
    users.tripnumber <- as.data.frame(unique(part[,c('imei','tripnumber')]))
    trip.features <- matrix(0, ncol = 284, nrow = nrow(users.tripnumber))
    for (j in 1:nrow(users.tripnumber)){
      trip.data <- part[part$imei == users.tripnumber[j,1] & part$tripnumber == users.tripnumber[j,2],]
      if (nrow(trip.data) <= 60){
        next
      }
      trip.features[j,] <- Trip.Feature(trip.data)
    }
    
    
    trip.features <- split(trip.features, 1:nrow(trip.features))
    return(trip.features)
  })
  
  day.features.df <- SparkR:::createDataFrame(day.features)
  names(day.features.df) <- c('imei', 'vid', 'tripnumber', 'start_time', 'end_time','dura', 'dura_valid', 
                              'lat_st', 'lon_st', 'lat_end', 'lon_end', 'fly_dist', 'curv_overall', 'mileage',
                              paste('m',0:23,1:24,sep='_'), 'speed_mean','speed_sd', 'speed_min', 
                              paste('speed', seq(10,90,10), 'pct', sep='_'), 
                              'speed_max', 'speed_0', 'speed_mean_drive', 'speed_sd_drive', 
                              paste('speed', seq(0,200,10), c(seq(10,200,10),Inf), sep='_'),
                              'acc_overall_mean', 'acc_overall_sd', 'acc_overall_min', 
                              paste('acc_overall', seq(10,90,10), 'pct', sep='_'), 'acc_overall_max', 
                              'acc_pos_dura', 'acc_neg_dura', 'acc_pos_mean', 'acc_neg_mean', 'acc_pos_sd', 'acc_neg_sd', 'acc_pos_min', 
                              paste('acc_pos', seq(10,90,10), 'pct',sep='_'), 'acc_pos_max', 'acc_neg_min', 
                              paste('acc_neg', seq(10,90,10), 'pct',sep='_'), 'acc_neg_max', 
                              paste('acc_pos', c(sprintf('%02d', seq(0,100,5))), c(sprintf('%02d', seq(5,100,5)),'inf'),sep='_'),
                              paste('acc_neg', c(sprintf('%02d', seq(0,100,5))), c(sprintf('%02d', seq(5,100,5)),'inf'),sep='_'),
                              paste('turn', c('dura','times',paste('speed',c('mean','sd','min',seq(10,90,10),'max'), sep = '_')), sep = '_'), 
                              paste('turn_left', c('dura','times',paste('speed',c('mean','sd','min',seq(10,90,10),'max'), sep = '_')), sep = '_'), 
                              paste('turn_right', c('dura','times',paste('speed',c('mean','sd','min',seq(10,90,10),'max'), sep = '_')), sep = '_'), 
                              'turn_overall_mean', 'turn_overall_sd', 'turn_overall_min',
                              paste('turn_overall', seq(10,90,10), 'pct', sep='_'),  'turn_overall_max',
                              'turn_right_mean', 'turn_left_mean', 'turn_right_sd', 'turn_left_sd', 'turn_right_min', 
                              paste('turn_right', seq(10,90,10), 'pct', sep='_'), 'turn_right_max','turn_left_min', 
                              paste('turn_left', seq(10,90,10), 'pct', sep='_'),'turn_left_max', 
                              paste('turn_right', c(sprintf('%02d',seq(0,100,5))), c(sprintf('%02d',seq(5,100,5)),'inf'), sep='_'),
                              paste('turn_left', c(sprintf('%02d',seq(0,100,5))), c(sprintf('%02d',seq(5,100,5)),'inf'), sep='_'))
  
  day.features.cl <- SparkR:::coalesce(day.features.df, numPartitions = 200)
  
  
  write.parquet(day.features.cl, 
                path = paste("/user/kettle/obdbi/original/ciitc/all_users_complex_features/stat_date=", days[i], sep = ''), 
                mode="overwrite")
}

sparkR.session.stop()
