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
