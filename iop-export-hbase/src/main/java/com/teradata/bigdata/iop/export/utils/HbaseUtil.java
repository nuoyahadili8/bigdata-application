package com.teradata.bigdata.iop.export.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import java.io.Serializable;

/**
 * @Project:
 * @Description:
 * @Version 1.0.0
 * @Throws SystemException:
 * @Author: <li>2019/9/23/023 Administrator Create 1.0
 * @Copyright ©2018-2019 al.github
 * @Modified By:
 */
public class HbaseUtil implements Serializable {

    public Configuration getConfiguration(){
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://nmsq/apps/hbase/data");
        conf.set("hbase.zookeeper.quorum", "ss-b02-m12-a01-r5300-1,ss-b02-m12-a01-r5300-3,ss-b02-m12-a01-r5300-4,ss-b02-m12-a01-r5300-5,ss-b02-m12-a01-r5300-6");
        conf.set("hadoop.security.bdoc.access.id", "5754f80986433f4cb8de");
        conf.set("hadoop.security.bdoc.access.key", "d76256523eb684f3f814");
        conf.set("hbase.client.start.log.errors.counter", "1");
        conf.set("hbase.client.retries.number", "1");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.client.pause", "1000");
        conf.set("hbase.rpc.timeout", "12000");
        conf.set("hbase.client.operation.timeout", "60000");
        conf.set("hbase.client.scanner.timeout.period", "10000");
        conf.set("hbase.client.write.buffer", "6291456");
        conf.set("hbase.zookeeper.property.maxClientCnxns", "1000");
        conf.set("hbase.regionserver.handler.count", "30000");
        return conf;
    }
}
