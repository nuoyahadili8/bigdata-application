package com.teradata.bigdata.intellectsms.users

/**
  * @Project:
  * @Description:
  * @param localCity      活动要求所在的城市
  * @param lacCellSet     活动要求所在的基站
  * @param roamTypeSet    活动要求的漫游类型
  * @param stayDuration   活动要求的驻留时长
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/8/28/028 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
case class YunmasActInfo(localCitySet: Set[String]
                         , lacCellSet: Set[String]
                         , roamTypeSet: Set[String]
                         , stayDuration: Long)
