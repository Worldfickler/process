package com.fickler.bean

/**
 * @author dell
 * @version 1.0
 */
case class CovidBean(

                      provinceName: String,
                      provinceShortName: String,
                      cityName: String,
                      currentConfirmedCount: Int,
                      confirmedCount: Int,
                      suspectedCount: Int,
                      curedCount: Int,
                      deadCount: Int,
                      locationId: Int,
                      pid: Int,
                      cities: String,
                      statisticsData: String,
                      datetime: String

                    )
