package com.teradata.bigdata.intellectsms

import java.util.{Calendar, Properties}

import com.teradata.bigdata.intellectsms.area._
import com.teradata.bigdata.intellectsms.users.DangjianUsers
import com.teradata.bigdata.util.kafka.{KafkaProperties, KafkaSink}
import com.teradata.bigdata.util.spark.{BroadcastWrapper, SparkConfig}
import com.teradata.bigdata.util.tools.TimeFuncs
import com.xiaoleilu.hutool.util.StrUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * @Project:
  * @Description: 智能短信 符合条件的记录放入kafka topic YZ_TD_YUNMAS_ALL
  * @Version 1.0.0
  * @Throws SystemException:
  * @Author: <li>2019/8/27/027 Administrator Create 1.0
  * @Copyright ©2018-2019 al.github
  * @Modified By:
  */
object IntelligentSMSApplication extends TimeFuncs with Serializable {

  var lastTime = Calendar.getInstance().getTime
  var publicSecurityLastTime = Calendar.getInstance().getTime
  val timeFreq: Long = 300000L
  val publicSecurityTimeFreq: Long = timeFreq
  val classNameStr = "IntelligentSMSApplication"

  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConfig
    val kafkaProperties = new KafkaProperties()

    val conf = sparkConfig.getConf.setAppName(classNameStr)
    val ssc = new StreamingContext(conf, Seconds(20))

    val brokers = kafkaProperties.kafkaBrokers.mkString(",")
    val groupId = classNameStr
    // 整合之后的topic
    val topics = Array(kafkaProperties.integrationTopic)

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    // 获取公安的用户列表（监控人群，全是手机号）
    def getPublicSecurityCustFromHdfs: Set[String] = {
      val publicSecurityPath = "hdfs://nmsq/user/bdoc/70/hive/b_yz_app_bonc_hive/pdata_floatingnet_sc_phonelist/*"
      ssc.sparkContext.textFile(publicSecurityPath).collect().toSet
    }

    val publicSecurityCustSetBro: BroadcastWrapper[Set[String]] = BroadcastWrapper[Set[String]](ssc, getPublicSecurityCustFromHdfs)

    def updateGonganBroadcast() {
      //每隔5分钟更新广播变量
      val currTime = toDate
      val diffTime = currTime.getTime - publicSecurityLastTime.getTime
      if (diffTime > publicSecurityTimeFreq) {
        publicSecurityCustSetBro.update(getPublicSecurityCustFromHdfs, blocking = true)
        publicSecurityLastTime = toDate
      }
    }

    val kafkaStreams: InputDStream[ConsumerRecord[String, String]]
    = KafkaUtils.createDirectStream[String, String](
      ssc
      , LocationStrategies.PreferConsistent
      , ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    // 广播生产者对象
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", brokers)
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    kafkaStreams.map(m =>{
      m.value().split(",", -1)
    }).filter((f: Array[String]) => {
      if (f.length >= 25 && f(7).length >= 11) {
        true
      } else{
        false
      }
    }).map(m => {
      //     (业务流程开始时间)   ,手机号 ,所在地市 ,用户漫游类型 ,归属省 ,归属地市 ,lac   ,cell  ,流程类型编码
      (m(7),((m(11),m(9).toLong),m(7)  ,m(1)    ,m(4)        ,m(2)  ,m(3)    ,m(19) ,m(20) ,m(8)))
    }).foreachRDD(rdd =>{
      updateGonganBroadcast

      // hash手动分区  保证数据分布均匀
      rdd.partitionBy(new HashPartitioner(partitions = 200)).foreachPartition(partition => {
        // 从广播变量中获取公安的用户列表（监控人群，内容全是手机号）
        val publicSecurityCustSet: Set[String] = publicSecurityCustSetBro.value
        // 目标topic
        val targetTopic = "YZ_TD_YUNMAS_ALL"

        val area = new AreaList
        val xinganmengArea = new XinganmengAreaList
        val fengzhenArea = new FengzhenAreaList
        val xinganmengWuchakouAreaList = new XinganmengWuchakouAreaList
        val ganqimaoduAreaList = new GanqimaoduAreaList
        val xinghexianAreaList = new XinghexianAreaList
        val chenbaerhuqiAreaList = new ChenbaerhuqiAreaList
        val eerduosiAreaList = new EerduosiAreaList
        val elunchunAreaList = new ElunchunAreaList
        val erlianhaotexuanchuanbuAreaList = new ErlianhaotexuanchuanbuAreaList
        val yijinhuoluoqiAreaList = new YijinhuoluoqiAreaList
        val genheAlongshanAreaList = new GenheAlongshanAreaList
        val tuoketuoxianAreaList = new TuoketuoxianAreaList
        val genhelinyejuAreaList = new GenhelinyejuAreaList
        val chuoyuanlinyejuAreaList = new ChuoyuanlinyejuAreaList
        val bailanglinyejuAreaList = new BailanglinyejuAreaList
        val dalateqiAreaList = new DalateqiAreaList
        val mianduheAreaList = new MianduheAreaList
        val wuerqiAreaList = new WuerqiAreaList
        val baotouAreaList = new BaotouAreaList
        val bamengbianjingzhiduAreaList = new BamengbianjingzhiduAreaList
        val tumujibaohuquAreaList = new TumujibaohuquAreaList
        val wulagaiGuanliquAreaList = new WulagaiGuanliquAreaList
        val chuoerlinyejuAreaList = new ChuoerlinyejuAreaList
        val budui32107AreaList = new Budui32107AreaList
        val jinhesenlingongyeAreaList = new JinhesenlingongyeAreaList
        val daxinganlinglinguanjuAreaList = new DaxinganlinglinguanjuAreaList
        val shangduxuanchuanbuAreaList = new ShangduxuanchuanbuAreaList
        val budui66113AreaList = new Budui66113AreaList
        val yituliheAreaList = new YituliheAreaList
        val eqianqiAreaList = new EqianqiAreaList
        val ganhelinyeAreaList = new GanhelinyeAreaList
        val moerdaogaAreaList = new MoerdaogaAreaList
        val chenqigonganjuAreaList = new ChenqigonganjuAreaList
        val tongliaojiaotouAreraList = new TongliaojiaotouAreraList
        val keyouzhongqiAreaList = new KeyouzhongqiAreaList
        val genhewenlvguangdianAreaList = new GenhewenlvguangdianAreaList
        val helingeerlvyouAreaList = new HelingeerlvyouAreaList
        val manzhouliAreaList = new ManzhouliAreaList
        val manguilinyejuAreaList = new ManguilinyejuAreaList
        val shangduqiAreaList = new ShangduqiAreaList
        val minhangjichangAreaList = new MinhangjichangAreaList
        val hulunbeiergonganjuAreaList = new HulunbeiergonganjuAreaList
        val chifenghongshanAreaList = new ChifenghongshanAreaList
        val xinganmenggonganjuAreaList = new XinganmenggonganjuAreaList
        val chifengaohanqizhengfaweiAreaList = new ChifengaohanqizhengfaweiAreaList
        val eergunaAreaList = new EergunaAreaList
        val wulatezhongqizhengfaweiAreaList = new WulatezhongqizhengfaweiAreaList
        val hangjinqiAreaList = new HangjinqiAreaList
        val hulunbeierwenlvguangdianAreaList = new HulunbeierwenlvguangdianAreaList
        val genhegonganjuAreaList = new GenhegonganjuAreaList
        val alashanjingjiquAreaList = new AlashanjingjiquAreaList
        val chifengyuanbaoshanAreaList = new ChifengyuanbaoshanAreaList
        val wulanhaoteAreaList = new WulanhaoteAreaList
        val tuquanxianweizhengfaweiAreaList = new TuquanxianweizhengfaweiAreaList
        val zhungeerqiAreaList = new ZhungeerqiAreaList
        val wushenqiAreaList = new WushenqiAreaList
        val etuokeqiAreaList = new EtuokeqiAreaList
        val kangbashenAreaList = new KangbashenAreaList
        /*val huhehaoteAreaList1 = new HuhehaoteAreaList1
        val huhehaoteAreaList2 = new HuhehaoteAreaList2
        val huhehaoteAreaList3 = new HuhehaoteAreaList3
        val huhehaoteAreaList4 = new HuhehaoteAreaList4
        val huhehaoteAreaList5 = new HuhehaoteAreaList5
        val huhehaoteAreaList6 = new HuhehaoteAreaList6
        val huhehaoteAreaList7 = new HuhehaoteAreaList7
        val huhehaoteAreaList8 = new HuhehaoteAreaList8*/
        val wulateqianqiAreaList = new WulateqianqiAreaList
        val xianghuangqiAreaList = new XianghuangqiAreaList
        val baotoukunquAreaList = new BaotoukunquAreaList
        val xiwuzhumuqinqiAreaList = new XiwuzhumuqinqiAreaList
        val taiqiAreaList = new TaiqiAreaList
        val tuyouqiAreaList = new TuyouqiAreaList
        val dangjianUsers = new DangjianUsers
        val zhalaiteAreaList = new ZhalaiteAreaList
        val siziwangqigonganListArea = new SiziwangqigonganListArea
        val siziwangqilvyouAreaList = new SiziwangqilvyouAreaList
        val bayannaoerAreaList = new BayannaoerAreaList
        val baoTouExtAreaList = new BaoTouExtAreaList


        val aershanLacCiList = area.aershanLacCiList
        val xinBaerhuzuoqi = area.xinBaerhuzuoqi
        val erlianhaote = area.erlianhaote
        val alashan = area.alashan
        val elunchun = area.elunchun
        val ejinaqi = area.ejinaqi
        val wulanchabu = area.wulanchabu
        val baotouyidong = area.baotouyidong
        val manzhouli = area.manzhouli
        val siziwangqi = area.siziwangqi
        val keerqinyouyizhongqi = area.keerqinyouyizhongqi
        val xinganmengnongmuyeju1 = xinganmengArea.xinganmengnongmuyeju1
        val xinganmengnongmuyeju2 = xinganmengArea.xinganmengnongmuyeju2
        val xinganmengnongmuyeju3 = xinganmengArea.xinganmengnongmuyeju3
        //        val dengkousanshenggong = area.dengkousanshenggong
        val fengzheng = fengzhenArea.fengzheng
        val tuoketuoxian = tuoketuoxianAreaList.tuoketuoxian
        val xinganmengWuchakou = xinganmengWuchakouAreaList.xinganmengWuchakouSet
        val ganqimaodu = ganqimaoduAreaList.ganqimaoduAreaSet
        val wulatehouqi = area.wulatehouqi
        val xinghexian = xinghexianAreaList.xinghexian
        val chenbaerhuqi = chenbaerhuqiAreaList.chenbaerhuqi
        val eerduosi = eerduosiAreaList.eerduosi
        val eerduosiLeaders = eerduosiAreaList.eerduosiLeaders
        val elunchunJijianjiancha = elunchunAreaList.elunchunJijianjiancha
        val elunchungonganju = elunchunAreaList.elunchungonganju
        val erlianhaotexuanchuanbu = erlianhaotexuanchuanbuAreaList.erlianhaotexuanchuanbu
        val yijinhuoluoqi = yijinhuoluoqiAreaList.yijinhuoluoqi
        val elunchunqiweixuanchuanbu = elunchunAreaList.elunchunqiweixuanchuanbu
        val genheAlongshan = genheAlongshanAreaList.genheAlongshan
        val bailangzhen = bailanglinyejuAreaList.bailangzhen
        val chuoyuanlinyeju = chuoyuanlinyejuAreaList.chuoyuanlinyeju
        val genhelinyeju = genhelinyejuAreaList.genhelinyeju
        val dalateqi = dalateqiAreaList.dalateqi
        val mianduhe = mianduheAreaList.mianduhe
        val wuerqi = wuerqiAreaList.wuerqi
        val baotounongshanghang = baoTouExtAreaList.baotounongshanghang
        val bamengbianjing = bamengbianjingzhiduAreaList.Bamengbianjing
        val tumujibaohuqu = tumujibaohuquAreaList.tumujibaohuqu
        val wulagaiGuanliqu = wulagaiGuanliquAreaList.wulagaiguanliqu
        val wulagaiguanliqulvyou = wulagaiGuanliquAreaList.wulagaiguanliqulvyou
        val chuoerlinyeju = chuoerlinyejuAreaList.chuoerlinyeju
        val budui32107Youqi = budui32107AreaList.budui32107Youqi
        val budui32107Zuoqi = budui32107AreaList.budui32107Zuoqi
        val jinhesenlingongye = jinhesenlingongyeAreaList.jinhesenlingongye
        val daxinganlinglinguanju = daxinganlinglinguanjuAreaList.daxinganlinglinguanju
        val shangduxuanchuanbu = shangduxuanchuanbuAreaList.shangduxuanchuanbu
        val budui66113 = budui66113AreaList.budui66113
        val yitulihe = yituliheAreaList.yitulihe
        val eqianqiChaao = eqianqiAreaList.eqianqiChaao
        val eqianqiAoang = eqianqiAreaList.eqianqiAoang
        val eqianqiAoyin = eqianqiAreaList.eqianqiAoyin
        val eqianqiAodong = eqianqiAreaList.eqianqiAodong
        val eqianqiSanyan = eqianqiAreaList.eqianqiSanyan
        val eqianqiS216 = eqianqiAreaList.eqianqiS216
        val eqianqiTongshi = eqianqiAreaList.eqianqiTongshi
        val eqianqiX632 = eqianqiAreaList.eqianqiX632
        val ganhelinye = ganhelinyeAreaList.ganhelinye
        val moerdaoga = moerdaogaAreaList.moerdaoga
        val chenqigonganju = chenqigonganjuAreaList.chenqigonganju
        val tongliaojiaotou = tongliaojiaotouAreraList.tongliaojiaotou
        val keyouzhongqizhengfawei = keyouzhongqiAreaList.keyouzhongqizhengfawei
        val genhewenlvguangdian = genhewenlvguangdianAreaList.genhewenlvguangdian
        val helingeernanshan = helingeerlvyouAreaList.helingeernanshan
        val manzhouliwenlvguangdian = manzhouliAreaList.manzhouliwenlvguangdian
        val manguilinyeju = manguilinyejuAreaList.manguilinyeju
        val helingeerxian = helingeerlvyouAreaList.helingeerxian
        val shangduqi = shangduqiAreaList.shangduqi
        val jichang = minhangjichangAreaList.jichang
        val hulunbeiergonganju = hulunbeiergonganjuAreaList.hulunbeiergonganju
        val chifenghongshan = chifenghongshanAreaList.chifenghongshan
        val eqianqiGonganju1 = eqianqiAreaList.eqianqiGonganju1
        val eqianqiGonganju2 = eqianqiAreaList.eqianqiGonganju2
        val wulanhaotejichang = xinganmenggonganjuAreaList.wulanhaotejichang
        val chifengaohanqizhengfawei = chifengaohanqizhengfaweiAreaList.chifengaohanqizhengfawei
        val eergunagonganju = eergunaAreaList.eergunagonganju
        val wulatezhongqizhengfawei = wulatezhongqizhengfaweiAreaList.wulatezhongqizhengfawei
        val hangjinqi = hangjinqiAreaList.hangjinqi
        val hulunbeierwenlvguangdian = hulunbeierwenlvguangdianAreaList.hulunbeierwenlvguangdian
        val genhegonganju = genhegonganjuAreaList.genhegonganju
        val alashanjingjiqu = alashanjingjiquAreaList.alashanjingjiqu
        val chifengyuanbaoshan = chifengyuanbaoshanAreaList.chifengyuanbaoshan
        val wulanhaote = wulanhaoteAreaList.wulanhaote
        val tuquanxianweizhengfawei = tuquanxianweizhengfaweiAreaList.tuquanxianweizhengfawei
        val hulunhu = manzhouliAreaList.hulunhu
        val zhalainuoershoufeizhan = zhalaiteAreaList.zhalainuoershoufeizhan
        val dalateqigonganjujiaotongdui1 = dalateqiAreaList.dalateqigonganjujiaotongdui1
        val dalateqigonganjujiaotongdui2 = dalateqiAreaList.dalateqigonganjujiaotongdui2
        val zhungeerqi = zhungeerqiAreaList.zhungeerqi
        val wushenqi = wushenqiAreaList.wushenqi
        val etuokeqi = etuokeqiAreaList.etuokeqi
        val eqianqi = eqianqiAreaList.eqianqi
        val eergunawenlvtiyu = eergunaAreaList.eergunawenlvtiyu
        val hangjinqizhengfawei = hangjinqiAreaList.hangjinqizhengfawei
        val kangbashen = kangbashenAreaList.kangbashen
        val budui32107Humeng = budui32107AreaList.budui32107Humeng
        val wulilengwenlvguangdian = elunchunAreaList.wulilengwenlvguangdian
        val eergunamengyuanlvyou = eergunaAreaList.eergunamengyuanlvyou
        /*val huhehaoteSiqu1 = huhehaoteAreaList1.huhehaoteSiqu1
        val huhehaoteSiqu2 = huhehaoteAreaList2.huhehaoteSiqu2
        val huhehaoteSiqu3 = huhehaoteAreaList3.huhehaoteSiqu3
        val huhehaoteSiqu4 = huhehaoteAreaList4.huhehaoteSiqu4
        val huhehaoteSiqu5 = huhehaoteAreaList5.huhehaoteSiqu5
        val huhehaoteSiqu6 = huhehaoteAreaList6.huhehaoteSiqu6
        val huhehaoteSiqu7 = huhehaoteAreaList7.huhehaoteSiqu7
        val huhehaoteSiqu8 = huhehaoteAreaList8.huhehaoteSiqu8*/
        val wulateqianqi = wulateqianqiAreaList.wulateqianqi
        val elunchunWenhualvyou = elunchunAreaList.elunchunWenhualvyou
        val baotoubeiteruierkouqiang = baotouAreaList.baotoubeiteruierkouqiang
        val baotouzhongtishengaochuangguan = baotouAreaList.baotouzhongtishengaochuangguan
        val keyouzhongqifayuan = keyouzhongqiAreaList.keyouzhongqifayuan
        val xianghuangqilvyoufuwuzhongxin = xianghuangqiAreaList.xianghuangqilvyoufuwuzhongxin
        val baotoudongheyingyebu = baotouAreaList.baotoudongheyingyebu
        val baotoujiuyuanyingyebu = baotouAreaList.baotoujiuyuanyingyebu
        val baotoukunquyingyebu = baotoukunquAreaList.baotoukunquyingyebu
        val baotouqingshanyingyebu = baotouAreaList.baotouqingshanyingyebu
        val xiwuzhumuqinqigonganjiaojing = xiwuzhumuqinqiAreaList.xiwuzhumuqinqigonganjiaojing
        val taiqisijinchukou = taiqiAreaList.taiqisijinchukou
        val tuyouqiwaiquan = tuyouqiAreaList.tuyouqiwaiquan
        val tuyouqiPhoneNo = tuyouqiAreaList.tuyouqiPhoneNo
        val hangjinqiqixinghu = hangjinqiAreaList.hangjinqiqixinghu
        val zhalaitenongchanyeyuan = zhalaiteAreaList.zhalaitenongchanyeyuan
        val siziwangqigongan = siziwangqigonganListArea.siziwangqigongan
        val siziwangqilvyou = siziwangqilvyouAreaList.siziwangqilvyouAreaList
        val dalateqiweiyuanhui = dalateqiAreaList.dalateqiweiyuanhui
        val bayannaoerlvye = bayannaoerAreaList.bayannaoerlvye
        val alashanqixiangju = alashanjingjiquAreaList.alashanqixiangju
        val wulateqianqilvyou = wulateqianqiAreaList.wulateqianqilvyou
        val eergunasengzelvyou = eergunaAreaList.eergunasengzelvyou

        partition
          .toList
          //按时间排序
          .sortBy(_._2._1._2)
          .foreach(kline => {

            val line = kline._2

            val phone_no = line._2
            val local_city = line._3
            val roam_type = line._4
            val owner_province = line._5
            val owner_city = line._6
            val lac = line._7
            val ci = line._8
            val lac_ci = lac + "-" + ci
            val startTime: String = line._1._1
            val startTimeLong: Long = line._1._2
            val procedure_type = line._9


            val stringLine = startTime + "|" +
              phone_no + "|" +
              local_city + "|" +
              roam_type + "|" +
              owner_province + "|" +
              owner_city + "|" +
              lac + "|" +
              ci

            // eventType  需求ID  自定义
            def send(eventType: Int): Unit = {
              kafkaProducer.value.send(targetTopic, eventType.toString + "|" + stringLine)
            }

            /**
              * 所有类型（按漫游类型）的用户
              */
            //            党建人群1
            if (dangjianUsers.dangjian.contains(phone_no)) send(1)
            //            公安人群32
            if (publicSecurityCustSet.contains(phone_no)) send(32)
            //              兴安盟农牧业局21
            //            if (roam_type.equals("4") && (xinganmengnongmuyeju1.contains(lac_ci)
            //              || xinganmengnongmuyeju2.contains(lac_ci)
            //              || xinganmengnongmuyeju3.contains(lac_ci)
            //              )
            //            ) send(21)
            //   乌拉特后旗30
            //            if (local_city.equals("0478") && wulatehouqi.contains(lac_ci)) send(30)

            //            阿尔山5
            if (local_city.equals("0482")) send(5)
            //              鄂伦春旗委宣传部11
            if (elunchunqiweixuanchuanbu.contains(lac_ci)) send(11)
            //              包头移动17
            if (baotouyidong.contains(lac_ci)) send(17)
            //                兴安盟五岔沟28
            if (xinganmengWuchakou.contains(lac_ci)) send(28)
            //            满洲里18
            //            if (manzhouli.contains(lac_ci)) send(18)
            //              甘其毛都29
            if (ganqimaodu.contains(lac_ci)) send(29)
            //              鄂伦春纪检监察34
            if (elunchunJijianjiancha.contains(lac_ci)) send(34)
            //              鄂伦春公安局35
            if (elunchungonganju.contains(lac_ci)) send(35)
            //            二连浩特宣传部36
            if (erlianhaotexuanchuanbu.contains(lac_ci)) send(36)
            //            根河阿龙山39
            if (genheAlongshan.contains(lac_ci)) send(39)
            //            呼伦贝尔，绰源林业局40
            if (chuoyuanlinyeju.contains(lac_ci)) send(40)
            //            呼伦贝尔，根河林业局41
            if (genhelinyeju.contains(lac_ci)) send(41)
            //            兴安盟，白狼林业局42
            if (bailangzhen.contains(lac_ci)) send(42)
            //            呼伦贝尔，牙克石，免渡河44
            if (mianduhe.contains(lac_ci)) send(44)
            //            呼伦贝尔，牙克石，乌尔旗45
            if (wuerqi.contains(lac_ci)) send(45)
            //            巴彦淖尔边境管理支队48
            if (bamengbianjing.contains(lac_ci)) send(48)
            //            兴安盟，扎赉特旗，图牧吉保护区49
            if (tumujibaohuqu.contains(lac_ci)) send(49)
            //            锡林郭勒盟，乌拉盖54
            if (wulagaiGuanliqu.contains(lac_ci)) send(54)
            //  民航机场，开关机用户55(20190716先去掉，等hbase恢复) procedure_type流程类型编码（1：绑定   6：解绑）***特殊  有开关机的情况
            if (Set("1", "6").contains(procedure_type) && jichang.contains(lac_ci)) send(55)
            //            if (jichang.contains(lac_ci)) send(55)
            //            呼伦贝尔，绰尔林业局56
            if (chuoerlinyeju.contains(lac_ci)) send(56)
            //            32107部队57
            if (budui32107Youqi.contains(lac_ci)) send(57)
            //            金河森林工业58
            if (jinhesenlingongye.contains(lac_ci)) send(58)
            //            大兴安岭林管局59
            if (daxinganlinglinguanju.contains(lac_ci)) send(59)
            //            66113部队61
            if (budui66113.contains(lac_ci)) send(61)
            //            伊图里河62
            if (yitulihe.contains(lac_ci)) send(62)

            //            鄂前旗察敖63
            if (eqianqiChaao.contains(lac_ci)) send(63)
            //            鄂前旗敖昂64
            if (eqianqiAoang.contains(lac_ci)) send(64)
            //            鄂前旗敖银65
            if (eqianqiAoyin.contains(lac_ci)) send(65)
            //            鄂前旗敖东66
            if (eqianqiAodong.contains(lac_ci)) send(66)
            //            鄂前旗三盐67
            if (eqianqiSanyan.contains(lac_ci)) send(67)
            //            鄂前旗S216 68
            if (eqianqiS216.contains(lac_ci)) send(68)
            //            鄂前旗通史 69
            if (eqianqiTongshi.contains(lac_ci)) send(69)
            //            鄂前旗X632 70
            if (eqianqiX632.contains(lac_ci)) send(70)
            //            甘河林业局71
            if (ganhelinye.contains(lac_ci)) send(71)
            //            陈巴尔虎公安局72
            if (chenqigonganju.contains(lac_ci)) send(72)
            //            莫尔道嘎73
            if (moerdaoga.contains(lac_ci)) send(73)
            //            通辽交投74
            if (tongliaojiaotou.contains(lac_ci)) send(74)
            //            兴安盟,科右中旗政法委75
            if (keyouzhongqizhengfawei.contains(lac_ci)) send(75)
            //            根河文旅广电76
            if (genhewenlvguangdian.contains(lac_ci)) send(76)
            //            和林格尔南山 77
            if (helingeernanshan.contains(lac_ci)) send(77)
            //            根河,满归林业局79
            if (manguilinyeju.contains(lac_ci)) send(79)
            //            乌兰察布，商都81
            if (shangduqi.contains(lac_ci)) send(81)
            //            呼伦贝尔公安局83
            if (hulunbeiergonganju.contains(lac_ci)) send(83)
            //            鄂前旗公安局1 85
            if (eqianqiGonganju1.contains(lac_ci)) send(85)
            //            鄂前旗公安局2 86
            if (eqianqiGonganju2.contains(lac_ci)) send(86)
            //              呼伦贝尔，额尔古纳公安局89
            if (eergunagonganju.contains(lac_ci)) send(89)
            //            兴安盟公安局，乌兰浩特机场
            if (wulanhaotejichang.contains(lac_ci)) send(91)
            //            鄂尔多斯，杭锦旗92
            if (hangjinqi.contains(lac_ci)) send(92)
            //            根河公安局94
            if (genhegonganju.contains(lac_ci)) send(94)
            //            阿拉善经济开发区行政审批服务局96
            if (alashanjingjiqu.contains(lac_ci)) send(96)
            if (alashanjingjiqu.contains(lac_ci)) send(97)

            //            白狼镇人民政府101
            if (bailangzhen.contains(lac_ci)) send(101)
            //            兴安盟法院102
            if (wulanhaote.contains(lac_ci)) send(102)
            //            突泉县政法103
            if (tuquanxianweizhengfawei.contains(lac_ci)) send(103)
            //            32107部队左旗104
            if (budui32107Zuoqi.contains(lac_ci)) send(104)
            //            满洲里呼伦湖105
            if (hulunhu.contains(lac_ci)) send(105)
            //            满洲里扎赉诺尔106
            if (zhalainuoershoufeizhan.contains(lac_ci)) send(106)
            //              鄂尔多斯市公安局交通管理支队达拉特旗大队108\109
            if (dalateqigonganjujiaotongdui1.contains(lac_ci)) send(108)
            if (dalateqigonganjujiaotongdui2.contains(lac_ci)) send(109)
            //            中共准格尔旗委员会宣传部110
            if (zhungeerqi.contains(lac_ci)) send(110)
            //            鄂尔多斯金融工作室-达拉特旗111
            if (dalateqi.contains(lac_ci)) send(111)
            //            鄂尔多斯金融工作室-准格尔旗112
            if (zhungeerqi.contains(lac_ci)) send(112)
            //            鄂尔多斯金融工作室-杭锦旗113
            if (hangjinqi.contains(lac_ci)) send(113)
            //            鄂尔多斯金融工作室-乌审旗114
            if (wushenqi.contains(lac_ci)) send(114)
            //            鄂尔多斯金融工作室-鄂托克旗115
            if (etuokeqi.contains(lac_ci)) send(115)
            //            鄂尔多斯金融工作室-鄂前旗116
            if (eqianqi.contains(lac_ci)) send(116)
            //            额尔古纳文旅体局117
            if (eergunawenlvtiyu.contains(lac_ci)) send(117)
            //            康巴什文旅局119
            if (kangbashen.contains(lac_ci)) send(119)
            //            30107部队（呼伦贝尔）120
            if (budui32107Humeng.contains(lac_ci)) send(120)
            //            乌拉特后旗政法委122
            if (wulatehouqi.contains(lac_ci)) send(122)
            //            额尔古纳蒙源旅游123
            if (eergunamengyuanlvyou.contains(lac_ci)) send(123)
            //            乌拉特前旗委员会政法委124
            if (wulateqianqi.contains(lac_ci)) send(124)
            //            鄂伦春文化旅游局127
            if (elunchunWenhualvyou.contains(lac_ci)) send(127)
            //            准格尔旗文化和旅游131
            if (zhungeerqi.contains(lac_ci)) send(131)
            //            镶黄旗旅游服务中心132
            if (xianghuangqilvyoufuwuzhongxin.contains(lac_ci)) send(132)
            //            锡盟太旗文体旅游局138
            if (taiqisijinchukou.contains(lac_ci)) send(138)
            //            土默特右旗文化旅游广电局139
            if (tuyouqiwaiquan.contains(lac_ci) && tuyouqiPhoneNo.contains(phone_no)) send(139)
            //              杭锦旗气象局140
            if (hangjinqiqixinghu.contains(lac_ci)) send(140)
            //            乌兰察布市四子王旗公安局信息平台 144
            if(siziwangqigongan.contains(lac_ci)) send(144)
            // 中国共产党达拉特旗委员会宣传部  146
            if(dalateqiweiyuanhui.contains(lac_ci)) send(146)
            // 阿拉善盟气象局 147
            if(alashanjingjiqu.contains(lac_ci)) send(147)
            // 乌拉特前旗文化旅游广电局 148
            if (wulateqianqilvyou.contains(lac_ci)) send(148)
            // 鄂尔多斯，达拉特旗43（内蒙古圣景文化旅游发展有限责任公司）  20190826 【变更需求】Fw:内蒙古圣景文化旅游发展有限责任公司要求针对达拉特旗基站下所有用户发送短信的需求
            if (dalateqi.contains(lac_ci)) send(43)
            // 乌拉盖管理区文体旅游广电局 201
            if (local_city.equals("0479") && wulagaiguanliqulvyou.contains(lac_ci)) send(201)

            //            呼和浩特房屋安全鉴定中心125
            /*if (huhehaoteSiqu1.contains(lac_ci)
              || huhehaoteSiqu2.contains(lac_ci)
              || huhehaoteSiqu3.contains(lac_ci)
              || huhehaoteSiqu4.contains(lac_ci)
              || huhehaoteSiqu5.contains(lac_ci)
              || huhehaoteSiqu6.contains(lac_ci)
              || huhehaoteSiqu7.contains(lac_ci)
              || huhehaoteSiqu8.contains(lac_ci)
            ) send(125)*/
            //              四子王旗19
            //            if (siziwangqi.contains(lac_ci)) send(19)

            //            else if (local_city.equals("0482") && aershanLacCiList.contains(lac_ci)) {
            //            用户漫游类型：
            //            1：国际漫游_
            //            2：省际漫游_
            //            3：省内漫游_
            //            4：本地

            //              漫入人群
            if (!roam_type.equals("4") && !roam_type.equals("")) {
              //              赤峰旅游局2
              if (local_city.equals("0476")) send(2)
              //            呼伦贝尔，新巴尔虎左旗6
              if (local_city.equals("0470") && xinBaerhuzuoqi.contains(lac_ci)) send(6)
              //                鄂尔多斯7
              if (local_city.equals("0477") && eerduosi.contains(lac_ci)) send(7)
              //              乌兰察布10
              if (local_city.equals("0474") && wulanchabu.contains(lac_ci)) send(10)
              //            漫入赤峰人群(所在地市，漫游类型)id:12
              if (local_city.equals("0476")) send(12)
              //              阿拉善额济纳旗13
              if (local_city.equals("0483") && ejinaqi.contains(lac_ci)) send(13)
              //                乌兰察布，丰镇26
              if (local_city.equals("0474") && fengzheng.contains(lac_ci)) send(26)
              //                呼和浩特27
              if (local_city.equals("0471") && !owner_city.equals("0471")) send(27)
              //                翁牛特旗22
              //              if (local_city.equals("0476") && wengniute.contains(lac_ci)) send(22)
              //            呼伦贝尔，陈巴尔虎旗31
              if (local_city.equals("0470") && chenbaerhuqi.contains(lac_ci)) send(31)
              //                鄂尔多斯，伊金霍洛旗37
              if (local_city.equals("0477") && yijinhuoluoqi.contains(lac_ci)) send(37)

              //                兴安盟，旅游局50
              if (local_city.equals("0482")) send(50)
              //              内蒙古文旅厅51
              if (roam_type.equals("2")) send(51)
              //              巴彦淖尔，经济和信息化委员会52
              if (local_city.equals("0478")) send(52)
              //              鄂尔多斯，达拉特旗53（鄂尔多斯市恩格贝沙漠生态旅游文化有限责任公司）
              if (local_city.equals("0477") && dalateqi.contains(lac_ci)) send(53)
              //                呼伦贝尔，满洲里文旅广电78
              if (local_city.equals("0470") && manzhouliwenlvguangdian.contains(lac_ci)) send(78)
              //                呼和浩特，和林县80
              if (local_city.equals("0471") && helingeerxian.contains(lac_ci)) send(80)
              //              赤峰市，红山84
              if (local_city.equals("0476") && chifenghongshan.contains(lac_ci)) send(84)
              //              乌拉特中旗政法委87
              if (local_city.equals("0478") && wulatezhongqizhengfawei.contains(lac_ci)) send(87)
              //              赤峰，敖汉旗政法委90
              if (local_city.equals("0476") && chifengaohanqizhengfawei.contains(lac_ci)) send(90)
              if (local_city.equals("0476") && chifengaohanqizhengfawei.contains(lac_ci)) send(107)
              //              呼伦贝尔，文旅广电局93
              if (local_city.equals("0470") && hulunbeierwenlvguangdian.contains(lac_ci)) send(93)
              //              阿拉善公安局95
              if (local_city.equals("0483")) send(95)
              //            鄂尔多斯，杭锦旗政法委98
              if (local_city.equals("0477") && hangjinqizhengfawei.contains(lac_ci)) send(98)
              //              赤峰，元宝山区100
              if (local_city.equals("0476") && chifengyuanbaoshan.contains(lac_ci)) send(100)
              //            杭锦旗人民法院118
              if (roam_type.equals("2") && hangjinqi.contains(lac_ci)) send(118)
              //              鄂伦春乌力楞121
              if (roam_type.equals("2") && wulilengwenlvguangdian.contains(lac_ci)) send(121)
              //              乌海文体旅游广电局126
              //              if (local_city.equals("0473")) send(126)
              //              科尔沁右翼中旗人民法院130
              if (roam_type.equals("3") && keyouzhongqifayuan.contains(lac_ci)) send(130)
              //              乌审旗旅游事业服务中心133
              if (wushenqi.contains(lac_ci)) send(133)
              //              包头市农牧局135
              if (baotoudongheyingyebu.contains(lac_ci)
                || baotoujiuyuanyingyebu.contains(lac_ci)
                || baotoukunquyingyebu.contains(lac_ci)
                || baotouqingshanyingyebu.contains(lac_ci)
              ) send(135)
              //              西乌珠穆沁旗公安交通警察大队136
              if (xiwuzhumuqinqigonganjiaojing.contains(lac_ci)) send(136)
              //              西乌珠穆沁旗锡林河酒业有限公司137
              if (xiwuzhumuqinqigonganjiaojing.contains(lac_ci)) send(137)
              //              扎赉特旗现代农业示范园区管理委员会142
              if (zhalaitenongchanyeyuan.contains(lac_ci)) send(142)

              //                磴口三盛公景区24
              //              if (local_city.equals("0478") && dengkousanshenggong.contains(lac_ci)) send(24)
              //            二连浩特3
              //              else if (local_city.equals("0479") && erlianhaote.contains(lac_ci)) send(3)
              //            锡林郭勒乌拉盖4
              //              else if (local_city.equals("0479") && wulagai.contains(lac_ci)) send(4)
              //              阿拉善右旗8
              //              else if (local_city.equals("0483") && alashan.contains(lac_ci)) send(8)
              //                  科尔沁右翼中旗20
              //              else if (local_city.equals("0482") && keerqinyouyizhongqi.contains(lac_ci)) send(20)
              //                  呼和浩特，托县25
              //              else if (local_city.equals("0471") && tuoxian.contains(lac_ci)) send(25)

              // 乌兰察布分公司四子王旗旅游局 145
              if (local_city.equals("0474") && siziwangqilvyou.contains(lac_ci)) send(145)
              // 内蒙古巴彦绿业实业有限公司143
              if (local_city.equals("0478") && bayannaoerlvye.contains(lac_ci)) send(143)
              // 额尔古纳市森泽旅游文化有限公司149
              if (local_city.equals("0470") && eergunasengzelvyou.contains(lac_ci)) send(149)
            }
            //            非漫入人群，本地用户
            else {
              if (!roam_type.equals("")) {
                //            呼和浩特，托克托县25
                if (local_city.equals("0471")
                  && tuoketuoxian.contains(lac_ci)
                  && owner_city.equals("0471")
                ) send(25)
                // 包头，农商行46
                if (baotounongshanghang.contains(lac_ci)) send(46)
                // 乌兰察布，商都宣传部60
                if (shangduxuanchuanbu.contains(lac_ci)) send(60)
                // 中国共产党达拉特旗委员会宣传部99
                if (local_city.equals("0477")
                  && owner_city.equals("0477")
                  && dalateqi.contains(lac_ci)) send(99)
                // 包头市贝特瑞尔口腔128
                if (baotoubeiteruierkouqiang.contains(lac_ci)) send(128)
                // 包头市中体盛奥场馆管理有限公司129
                if (baotouzhongtishengaochuangguan.contains(lac_ci)) send(129)
                // 科尔沁右翼中旗人民法院130
                if (keyouzhongqifayuan.contains(lac_ci)) send(130)
                // 扎赉特旗现代农业示范园区管理委员会141
                if (zhalaitenongchanyeyuan.contains(lac_ci)) send(141)
              }

            }
          })
      })
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}
