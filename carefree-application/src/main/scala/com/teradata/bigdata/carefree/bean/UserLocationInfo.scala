package com.teradata.bigdata.carefree.bean

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/9/3/003 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
case class UserLocationInfo(imsi:String                     //用户IMSI
                            ,imei:String                    //终端IMEI
                            ,msisdn:String                  //用户号码
                            ,local_city:String              //所在地市
                            ,location_source:String         //位置信息来源
                            ,procedure_start_time:String    //业务流程开始时间
                            ,TAC:String                     //TAC
                            ,ECI:String)                    //小区标识