package com.atguigu.gmall.realtime.bean
import java.text.SimpleDateFormat
import java.util.Date
case class StartupLog(mid: String,
                      uid: String,
                      ar: String,
                      ba: String,
                      ch: String,
                      md: String,
                      os: String,
                      vc: String,
                      ts: Long,
                      var logDate: String = null, // 年月日  2020-07-15
                      var logHour: String = null) { //小时  10
  private val date = new Date(ts)
  logDate = new SimpleDateFormat("yyyy-MM-dd").format(date)
  logHour = new SimpleDateFormat("HH").format(date)
}
/*
{
    "common":{
        "ar":"440000",
        "ba":"iPhone",
        "ch":"Appstore",
        "md":"iPhone X",
        "mid":"mid_26",
        "os":"iOS 13.2.9",
        "uid":"477",
        "vc":"v2.1.134"
    },
    "start":{
        "entry":"icon",
        "loading_time":1925,
        "open_ad_id":6,
        "open_ad_ms":8828,
        "open_ad_skip_ms":1129
    },
    "ts":1597319770000
}
 */
