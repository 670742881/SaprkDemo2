package com.spark.hbasedemo

;

/**
  * @created by imp ON 2019/2/19
  */

//创建日志类将数据分装到类里面
case class EventLog(
                     rowKey: String,
                     api_v: String,
                     app_id: String,
                     c_time: String,
                     ch_id: String,
                     city: String,
                     province: String,
                     country: String,
                     en: String,
                     ip: String,
                     net_t: String,
                     pl: String,
                     s_time: String,
                     user_id: String,
                     uuid: String,
                     ver: String
                   )
