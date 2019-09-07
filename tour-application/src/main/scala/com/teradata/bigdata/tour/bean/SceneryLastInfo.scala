package com.teradata.bigdata.tour.bean

case class SceneryLastInfo(sceneryId: String
                           , sceneryName: String
                           , sceneryLastSum: Int
                           , sceneryAlarmValue: Int
                           , cityWhereSceneryBelong: String
                           , cityList: List[(String, String, Int)]
                          )

