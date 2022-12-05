package com.fickler.bean

/**
 * @author dell
 * @version 1.0
 *          用来封装省份每一天的统计数据
 */
case class StatisticsDataBean(
                               var dateId: String,
                               var provinceShortName: String,
                               var locationId: Int,
                               var confirmedCount: Int,
                               var currentConfirmedCount: Int,
                               var confirmedIncr: Int,
                               var curedCount: Int,
                               var currentConfirmedIncr: Int,
                               var curedIncr: Int,
                               var suspectedCount: Int,
                               var suspectedCountIncr: Int,
                               var deadCount: Int,
                               var deadIncr: Int

                             )
