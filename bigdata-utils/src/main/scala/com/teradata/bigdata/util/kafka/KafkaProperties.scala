package com.teradata.bigdata.util.kafka

/**
  * @Project:
  * @Description:
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/8/26/026 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
class KafkaProperties {

  val kafkaBrokers:Set[String] =
    Set("ss-b02-m12-c05-r5300-4:6667"
    ,"ss-b02-m12-c05-r5300-1:6667"
    ,"ss-b02-m12-c05-r5300-5:6667"
    ,"ss-b02-m12-c05-r5300-2:6667"
    ,"ss-b02-m12-c05-r5300-6:6667"
    ,"ss-b02-m12-c05-r5300-9:6667"
    ,"ss-b02-m12-c04-r5300-10:6667"
    ,"ss-b02-m12-c05-r5300-3:6667"
    ,"ss-b02-m12-c05-r5300-7:6667"
    ,"ss-b02-m12-c05-r5300-8:6667"
    ,"ss-b02-m12-c05-r5300-10:6667"
    ,"ss-b02-m12-c06-r5300-1:6667"
    ,"ss-b02-m12-c06-r5300-2:6667"
    ,"ss-b02-m12-c06-r5300-3:6667"
    ,"ss-b02-m12-c06-r5300-5:6667"
    ,"ss-b02-m12-c06-r5300-4:6667"
    ,"ss-b02-m12-c06-r5300-6:6667"
    ,"ss-b02-m12-c06-r5300-7:6667"
    ,"ss-b02-m12-c06-r5300-9:6667"
    ,"ss-b02-m12-c06-r5300-8:6667"
    ,"ss-b02-m12-c06-r5300-10:6667"
    ,"ss-b02-m12-c07-r5300-1:6667"
    ,"ss-b02-m12-c07-r5300-2:6667"
    ,"ss-b02-m12-c07-r5300-3:6667"
    ,"ss-b02-m12-c07-r5300-4:6667"
    ,"ss-b02-m12-c07-r5300-7:6667"
    ,"ss-b02-m12-c07-r5300-6:6667"
    ,"ss-b02-m12-c07-r5300-9:6667"
    ,"ss-b02-m12-c07-r5300-8:6667"
    ,"ss-b02-m12-c07-r5300-5:6667")

  val zookeeperServers = "ss-b02-m12-a01-r5300-1:2181,ss-b02-m12-a01-r5300-3:2181,ss-b02-m12-a01-r5300-4:2181,ss-b02-m12-a01-r5300-5:2181,ss-b02-m12-a01-r5300-6:2181/kafka"

  val sourceTopic:Set[String]=Set("O_DPI_LTE_MME","O_DPI_MC_LOCATIONUPDATE_2G","O_DPI_MC_LOCATIONUPDATE_3G")

  val integrationTopic:String = "YZ_APP_TD_234G_DPI_DATA"

}
