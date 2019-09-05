###### 配置文件
```
a1.sources = source
a1.channels = channel
a1.sinks = sink

a1.sources.source.type = avro
a1.sources.source.bind = localhost
a1.sources.source.port = 44444
a1.sources.source.channels = channel

a1.sinks.sink.type = com.teradata.bigdata.flume.FileSink
a1.sinks.sink.file.path = /root/flume/data/%{dayStr}
a1.sinks.sink.file.filePrefix = dangjian_
a1.sinks.sink.file.extension = .AVL
a1.sinks.sink.file.rollInterval = 30
a1.sinks.sink.file.txnEventMax = 10000
a1.sinks.sink.file.maxOpenFiles = 5
a1.sinks.sink.channel = channel

a1.channels.channel.type = memory
a1.channels.channel.capacity = 100000
a1.channels.channel.transactionCapacity = 100000
a1.channels.channel.keep-alive = 60
```
###### 执行命令
```
flume-ng agent --conf conf --conf-file ./f3.conf --name a1 -Dflume.root.logger=INFO,console
```

###### 生成配置
```
dhx-select-phone-all.sources = kafkaSource
dhx-select-phone-all.channels = memoryChannel
dhx-select-phone-all.sinks = fileSink

dhx-select-phone-all.sources.kafkaSource.batchSize = 500000
dhx-select-phone-all.sources.kafkaSource.batchDurationMillis = 500000

dhx-select-phone-all.sources.kafkaSource.channels = memoryChannel
dhx-select-phone-all.sinks.fileSink.channel = memoryChannel

dhx-select-phone-all.sources.kafkaSource.type = org.apache.flume.source.kafka.KafkaSource
dhx-select-phone-all.sources.kafkaSource.kafka.bootstrap.servers = ss-b02-m12-c05-r5300-4:6667,ss-b02-m12-c05-r5300-1:6667,ss-b02-m12-c05-r5300-5:6667,ss-b02-m12-c05-r5300-2:6667,ss-b02-m12-c05-r5300-6:6667,ss-b02-m12-c05-r5300-9:6667,ss-b02-m12-c04-r5300-10:6667,ss-b02-m12-c05-r5300-3:6667,ss-b02-m12-c05-r5300-7:6667,ss-b02-m12-c05-r5300-8:6667,ss-b02-m12-c05-r5300-10:6667,ss-b02-m12-c06-r5300-1:6667,ss-b02-m12-c06-r5300-2:6667,ss-b02-m12-c06-r5300-3:6667,ss-b02-m12-c06-r5300-5:6667,ss-b02-m12-c06-r5300-4:6667,ss-b02-m12-c06-r5300-6:6667,ss-b02-m12-c06-r5300-7:6667,ss-b02-m12-c06-r5300-9:6667,ss-b02-m12-c06-r5300-8:6667,ss-b02-m12-c06-r5300-10:6667,ss-b02-m12-c07-r5300-1:6667,ss-b02-m12-c07-r5300-2:6667,ss-b02-m12-c07-r5300-3:6667,ss-b02-m12-c07-r5300-4:6667,ss-b02-m12-c07-r5300-7:6667,ss-b02-m12-c07-r5300-6:6667,ss-b02-m12-c07-r5300-9:6667,ss-b02-m12-c07-r5300-8:6667,ss-b02-m12-c07-r5300-5:6667
dhx-select-phone-all.sources.kafkaSource.topic=YZ_TD_YUNMAS_ALL
dhx-select-phone-all.sources.kafkaSource.groupId =dhx-select-phone-all-flume
dhx-select-phone-all.sources.kafkaSource.kafka.consumer.timeout.ms = 100

dhx-select-phone-all.channels.memoryChannel.type = memory
dhx-select-phone-all.channels.memoryChannel.keep-alive=60
dhx-select-phone-all.channels.memoryChannel.capacity=5000000
dhx-select-phone-all.channels.memoryChannel.transactionCapacity=5000000
dhx-select-phone-all.channels.memoryChannel.byteCapacityBufferPercentage = 20
dhx-select-phone-all.channels.memoryChannel.byteCapacity = 536870912

dhx-select-phone-all.sinks.fileSink.type = com.teradata.bigdata.flume.FileSink
dhx-select-phone-all.sinks.fileSink.file.path = /data/iop/projects/data/receive/
dhx-select-phone-all.sinks.fileSink.file.filePrefix = dangjian_
dhx-select-phone-all.sinks.fileSink.file.extension = .AVL
dhx-select-phone-all.sinks.fileSink.file.rollInterval = 20
dhx-select-phone-all.sinks.fileSink.file.txnEventMax = 100000
dhx-select-phone-all.sinks.fileSink.file.maxOpenFiles = 20
```

###### 启动脚本
```
sh /data/iop/app/apache-flume-1.9.0-bin/bin/flume-ng agent \
--conf /data/iop/app/apache-flume-1.9.0-bin/conf/ \
--conf-file /data/iop/projects/conf/dhx-kafka-file-select-phone.conf \
--name dhx-select-phone-all \
-Xmx40000m -Xms4096m -Xmn256m -Xss256k -XX:+UseParallelGC -XX:ParallelGCThreads=20 \
-Dflume.root.logger=INFO,LOGFILE \
-Dflume.monitoring.type=http \
-Dflume.monitoring.port=60012 &


sh /data/iop/app/apache-flume-1.9.0-bin/bin/flume-ng agent \
--conf /data/iop/app/apache-flume-1.9.0-bin/conf/ \
--conf-file /data/iop/projects/conf/dhx-kafka-file-select-phone.conf \
--name dhx-select-phone-all \
-Xmx40000m -Xms4096m -Xmn256m -Xss256k -XX:+UseParallelGC -XX:ParallelGCThreads=20 \
-Dflume.root.logger=INFO,LOGFILE \
-Dflume.monitoring.type=http \
-Dflume.monitoring.port=60013 &


sh /data/iop/app/apache-flume-1.9.0-bin/bin/flume-ng agent \
--conf /data/iop/app/apache-flume-1.9.0-bin/conf/ \
--conf-file /data/iop/projects/conf/dhx-kafka-file-select-phone.conf \
--name dhx-select-phone-all \
-Xmx40000m -Xms4096m -Xmn256m -Xss256k -XX:+UseParallelGC -XX:ParallelGCThreads=20 \
-Dflume.root.logger=INFO,LOGFILE \
-Dflume.monitoring.type=http \
-Dflume.monitoring.port=60014 &


sh /data/iop/app/apache-flume-1.9.0-bin/bin/flume-ng agent \
--conf /data/iop/app/apache-flume-1.9.0-bin/conf/ \
--conf-file /data/iop/projects/conf/dhx-kafka-file-select-phone.conf \
--name dhx-select-phone-all \
-Xmx40000m -Xms4096m -Xmn256m -Xss256k -XX:+UseParallelGC -XX:ParallelGCThreads=20 \
-Dflume.root.logger=INFO,LOGFILE \
-Dflume.monitoring.type=http \
-Dflume.monitoring.port=60015 &
```