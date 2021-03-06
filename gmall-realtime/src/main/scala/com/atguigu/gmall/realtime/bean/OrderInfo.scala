package com.atguigu.gmall.realtime.bean

case class OrderInfo(id: Long,
                     province_id: Long,
                     order_status: String,
                     user_id: Long,
                     final_total_amount: Double,
                     benefit_reduce_amount: Double,
                     original_total_amount: Double,
                     feight_fee: Double,
                     expire_time: String,
                     create_time: String,
                     operate_time: String,
                     var create_date: String = null,
                     var create_hour: String = null,
                     var is_first_order: Boolean = false,

                     var province_name: String = null,
                     var province_area_code: String = null,
                     var province_iso_code:String, //国际地区编码

                     var user_age_group: String = null,
                     var user_gender: String = null) {
  create_date = create_time.substring(0, 10)
  create_hour = create_time.substring(11, 13)
}