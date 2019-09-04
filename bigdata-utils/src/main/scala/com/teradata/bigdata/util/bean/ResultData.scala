package com.teradata.bigdata.util.bean

import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/8/26/026 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
case class ResultData(
                       localProvince:String,								// 0 所在省
                       localCity:String,                    // 1 所在地市
                       ownerProvince:String,                // 2 归属省
                       ownerCity:String,                    // 3 归属地市
                       roamType:String,                     // 4 用户漫游类型
                       imsi:String,                         // 5 用户IMSI
                       imei:String,                         // 6 终端IMEI
                       phoneNo:String,                      // 7 用户号码
                       procedureType:String,                // 8 流程类型编码
                       procedureStartTimeLong:String,       // 9 业务流程开始时间Long
                       procedureStartTimeSSS:String,        // 10业务流程开始时间SSS
                       procedureStartTime:String,           // 11业务流程开始时间
                       procedureEndTime:String,             // 12业务流程结束时间
                       longitude:String,                    // 13经度
                       latitude:String,                     // 14纬度
                       locationSource:String,               // 15位置信息来源
                       procedureStatus:String,              // 16流程状态
                       userIPV4:String,                     // 17终端用户的IPv4地址
                       userIPV6:String,                     // 18终端用户的IPv6地址
                       lac:String,                          // 19TAC
                       cell:String,                         // 20小区标识
                       otherLac:String,                     // 21对端小区的TAC
                       otherCell:String,                    // 22对端小区的ECI
                       dataType:String,                     // 23数据来源类型
                       phoneNoPartition:String              // 24手机号末尾2位+手机号11位（手机号分区）
                     ){
  val schema: StructType = StructType(
    Seq(
      StructField("localProvince", StringType, nullable = true),              //0 所在省
      StructField("localCity", StringType, nullable = true),                  //1 所在地市
      StructField("ownerProvince", StringType, nullable = true),              //2 归属省
      StructField("ownerCity", StringType, nullable = true),                  //3 归属地市
      StructField("roamType", StringType, nullable = true),                   //4 用户漫游类型
      StructField("imsi", StringType, nullable = true),                       //5 用户IMSI
      StructField("imei", StringType, nullable = true),                       //6 终端IMEI
      StructField("phoneNo", StringType, nullable = true),                    //7 用户号码
      StructField("procedureType", StringType, nullable = true),              //8 流程类型编码
      StructField("procedureStartTimeLong", StringType, nullable = true),     //9 业务流程开始时间Long
      StructField("procedureStartTimeSSS", StringType, nullable = true),      //10业务流程开始时间SSS
      StructField("procedureStartTime", StringType, nullable = true),         //11业务流程开始时间
      StructField("procedureEndTime", StringType, nullable = true),           //12业务流程结束时间
      StructField("longitude", StringType, nullable = true),                  //13经度
      StructField("latitude", StringType, nullable = true),                   //14纬度
      StructField("locationSource", StringType, nullable = true),             //15位置信息来源
      StructField("procedureStatus", StringType, nullable = true),            //16流程状态
      StructField("userIPV4", StringType, nullable = true),                   //17终端用户的IPv4地址
      StructField("userIPV6", StringType, nullable = true),                   //18终端用户的IPv6地址
      StructField("tac", StringType, nullable = true),                        //19TAC
      StructField("eci", StringType, nullable = true),                        //20小区标识
      StructField("otherTac", StringType, nullable = true),                   //21对端小区的TAC
      StructField("otherEci", StringType, nullable = true),                   //22对端小区的ECI
      StructField("dataType", StringType, nullable = true),                   //23数据来源类型
      StructField("phoneNoPartition", StringType, nullable = true)            //24手机号末尾2位+手机号11位（手机号分区）
    )
  )
}



