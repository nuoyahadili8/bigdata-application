package com.teradata.bigdata.tour.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.io.Serializable;

/**
 * @Project:
 * @Description:
 * @Version 1.0.0
 * @Throws SystemException:
 * @Author: <li>2019/9/10/010 Administrator Create 1.0
 * @Copyright ©2018-2019 al.github
 * @Modified By:
 */
public class HbaseUtil implements Serializable {

    volatile static HbaseUtil hbaseUtil;
    private Configuration config;

    private HbaseUtil(){
        init();
    }

    public static HbaseUtil getInstance(){
        if (null == hbaseUtil){
            synchronized (HbaseUtil.class){
                if (null == hbaseUtil){
                    hbaseUtil = new HbaseUtil();
                }
            }
        }
        return hbaseUtil;
    }

    private void init(){
        config = HBaseConfiguration.create();
        config.set("hbase.rootdir", "hdfs://nmsq/apps/hbase/data");
        config.set("hbase.zookeeper.quorum", "ss-b02-m12-a01-r5300-1,ss-b02-m12-a01-r5300-3,ss-b02-m12-a01-r5300-4,ss-b02-m12-a01-r5300-5,ss-b02-m12-a01-r5300-6");
        config.set("hadoop.security.bdoc.access.id", "5754f80986433f4cb8de");
        config.set("hadoop.security.bdoc.access.key", "d76256523eb684f3f814");
        config.set("hbase.client.start.log.errors.counter", "1");
        config.set("hbase.client.retries.number", "1");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.client.pause", "1000");
        config.set("hbase.rpc.timeout", "12000");
        config.set("hbase.client.operation.timeout", "60000");
        config.set("hbase.client.scanner.timeout.period", "10000");
        config.set("hbase.client.write.buffer", "6291456");
        config.set("hbase.zookeeper.property.maxClientCnxns", "1000");
        config.set("hbase.regionserver.handler.count", "30000");
    }

    public Configuration getConfig(){
        return config;
    }

    public Connection createHbaseConnection() throws IOException {
        return ConnectionFactory.createConnection(config);
    }


}
