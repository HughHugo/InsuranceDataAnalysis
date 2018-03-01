###sparkR initialization in product environment.
library(SparkR)
library(magrittr)
sparkR.session.stop()

sc <- sparkR.session("yarn-client", "ciitc",
                     sparkConfig=list(spark.executor.memory="40g",
                                      spark.executor.cores="12",
                                      spark.network.timeout="3600",
                                      spark.yarn.queue='prod',
                                      spark.sql.autoBroadcastJoinThreshold="104857600"))


all.user.simple.feature <- read.parquet('/user/kettle/obdbi/original/ciitc/all_users_simple_feature')
all.user.simple.feature$day <- from_unixtime(all.user.simple.feature$tripnumber,  "yyyy-MM-dd")

load('/home/heqiuguo/ciitc/data_dist/Validusers_Within2016.Rdata')
valid.users$imei <- as.character(valid.users$imei)
valid.users$valid_start_utc <- as.numeric(as.POSIXct(valid.users$valid_start))
valid.users$valid_end_utc <- as.numeric(as.POSIXct(valid.users$valid_end))
valid.user <- as.DataFrame(valid.users)

valid.user.simple.feature <- SparkR:::join(valid.user, all.user.simple.feature, 
                                           valid.user$imei == all.user.simple.feature$imei & 
                                             valid.user$valid_start_utc <= all.user.simple.feature$tripnumber & 
                                             valid.user$valid_end_utc >= all.user.simple.feature$end_time)

df.names <- names(valid.user.simple.feature)
names(valid.user.simple.feature) <- c(df.names[1:8], 'imei_1', df.names[10:length(df.names)])
eval(parse(text = paste("valid.user.simple.feature$speed_mean <- (", 
                        paste("valid.user.simple.feature$speed_", (0: 19) * 10, "_", (1 : 20) * 10, " * ",(0: 19) * 10 + 5, sep = '', collapse = " + "), 
                        " + valid.user.simple.feature$speed_200_Inf * 205) / valid.user.simple.feature$dura_valid", sep = '')))
valid.user.simple.feature$speed <- valid.user.simple.feature$mileage * 3600 / (valid.user.simple.feature4$end_time - valid.user.simple.feature$start_time)
valid.user.simple.feature <- filter(valid.user.simple.feature, valid.user.simple.feature$speed / valid.user.simple.feature$speed_mean <= 1.5)

simple.feature.by.user <- summarize(groupBy(valid.user.simple.feature, 'imei', 'VIN'), 
                                    valid_start = first(valid.user.simple.feature$valid_start_utc), 
                                    valid_end = first(valid.user.simple.feature$valid_end_utc),
                                    first_time = min(valid.user.simple.feature$tripnumber), 
                                    last_time = max(valid.user.simple.feature$tripnumber), 
                                    valid_days = n_distinct(valid.user.simple.feature$day), 
                                    no_trip = n_distinct(valid.user.simple.feature$tripnumber), 
                                    dura = sum(valid.user.simple.feature$dura), 
                                    dura_valid = sum(valid.user.simple.feature$dura_valid), 
                                    mileage = sum(valid.user.simple.feature$mileage), 
                                    m_0 = sum(valid.user.simple.feature$m_0), 
                                    m_1 = sum(valid.user.simple.feature$m_1), 
                                    m_2 = sum(valid.user.simple.feature$m_2), 
                                    m_3 = sum(valid.user.simple.feature$m_3), 
                                    m_4 = sum(valid.user.simple.feature$m_4), 
                                    m_5 = sum(valid.user.simple.feature$m_5), 
                                    m_6 = sum(valid.user.simple.feature$m_6), 
                                    m_7 = sum(valid.user.simple.feature$m_7), 
                                    m_8 = sum(valid.user.simple.feature$m_8), 
                                    m_9 = sum(valid.user.simple.feature$m_9), 
                                    m_10 = sum(valid.user.simple.feature$m_10), 
                                    m_11 = sum(valid.user.simple.feature$m_11), 
                                    m_12 = sum(valid.user.simple.feature$m_12), 
                                    m_13 = sum(valid.user.simple.feature$m_13), 
                                    m_14 = sum(valid.user.simple.feature$m_14), 
                                    m_15 = sum(valid.user.simple.feature$m_15), 
                                    m_16 = sum(valid.user.simple.feature$m_16), 
                                    m_17 = sum(valid.user.simple.feature$m_17), 
                                    m_18 = sum(valid.user.simple.feature$m_18), 
                                    m_19 = sum(valid.user.simple.feature$m_19), 
                                    m_20 = sum(valid.user.simple.feature$m_20),
                                    m_21 = sum(valid.user.simple.feature$m_21), 
                                    m_22 = sum(valid.user.simple.feature$m_22), 
                                    m_23 = sum(valid.user.simple.feature$m_23), 
                                    speed_0 = sum(valid.user.simple.feature$speed_0), 
                                    speed_0_10 = sum(valid.user.simple.feature$speed_0_10), 
                                    speed_10_20 = sum(valid.user.simple.feature$speed_10_20), 
                                    speed_20_30 = sum(valid.user.simple.feature$speed_20_30), 
                                    speed_30_40 = sum(valid.user.simple.feature$speed_30_40), 
                                    speed_40_50 = sum(valid.user.simple.feature$speed_40_50), 
                                    speed_50_60 = sum(valid.user.simple.feature$speed_50_60), 
                                    speed_60_70 = sum(valid.user.simple.feature$speed_60_70), 
                                    speed_70_80 = sum(valid.user.simple.feature$speed_70_80), 
                                    speed_80_90 = sum(valid.user.simple.feature$speed_80_90), 
                                    speed_90_100 = sum(valid.user.simple.feature$speed_90_100), 
                                    speed_100_110 = sum(valid.user.simple.feature$speed_100_110), 
                                    speed_110_120 = sum(valid.user.simple.feature$speed_110_120), 
                                    speed_120_130 = sum(valid.user.simple.feature$speed_120_130), 
                                    speed_130_140 = sum(valid.user.simple.feature$speed_130_140), 
                                    speed_140_150 = sum(valid.user.simple.feature$speed_140_150), 
                                    speed_150_160 = sum(valid.user.simple.feature$speed_150_160), 
                                    speed_160_170 = sum(valid.user.simple.feature$speed_160_170), 
                                    speed_170_180 = sum(valid.user.simple.feature$speed_170_180), 
                                    speed_180_190 = sum(valid.user.simple.feature$speed_180_190), 
                                    speed_190_200 = sum(valid.user.simple.feature$speed_190_200), 
                                    speed_200_Inf = sum(valid.user.simple.feature$speed_200_Inf))


eval(parse(text = paste("simple.feature.by.user$df_concat <- concat_ws(\',\', ", paste("simple.feature.by.user$", names(simple.feature.by.user), collapse = ", ", sep = ""), ")", sep = "")))
df.concat <- select(simple.feature.by.user, "df_concat")
df.concat.cs <- coalesce(df.concat, numPartitions = 1)
write.text(df.concat.cs, path = "/user/kettle/obdbi/original/ciitc/valid_users_simple_feature_aggregate", mode = 'overwrite')
