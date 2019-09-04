package com.teradata.bigdata.duration.bean

/**
  * @Project:
  * @Description:
  * @param ownerProvince    用户归属省份
  * @param ownerCity        用户归属地市
  * @param localProvince    用户所在省份
  * @param localCity        用户所在地市
  * @param imei             imei
  * @param tableFlag        数据来源
  * @param imsi             imsi
  * @param phoneNo          手机号
  * @param startTime        业务开始时间
  * @param lac              lac
  * @param cell             cell
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/4/004 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
case class UserInfo(
                   ownerProvince: String
                 , ownerCity: String
                 , localProvince: String
                 , localCity: String
                 , imei: String
                 , tableFlag: String
                 , imsi: String
                 , phoneNo: String
                 , startTime: String
                 , lac: String
                 , cell: String
               )
