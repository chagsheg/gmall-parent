package com.atguigu.gmall.gmallpublisher.service;


import java.util.Map;

/**
 * Author lzc
 * Date 2020/8/19 9:06 下午
 */
public interface PublisherService {
    /**
     * 获取指定指定日期的日活
     * @param date  需要计算日活的日期
     * @return  当然日活总数
     */
    Long getDau(String date);

    /**
     * 获取指定日期的小时活跃数
     * @param date  需要计算小时活跃数的日期
     * @return  小时候的活跃数  key: 小时   value: 活跃数
     */
    Map<String, Long> getHourDau(String date);
}

