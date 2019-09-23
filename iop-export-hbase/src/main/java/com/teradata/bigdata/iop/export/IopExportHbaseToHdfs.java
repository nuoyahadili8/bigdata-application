package com.teradata.bigdata.iop.export;

import com.teradata.bigdata.iop.export.utils.DateUtils;
import com.teradata.bigdata.iop.export.utils.HbaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.List;

/**
 * @Project:
 * @Description:
 * @Version 1.0.0
 * @Throws SystemException:
 * @Author: <li>2019/9/23/023 Administrator Create 1.0
 * @Copyright ©2018-2019 al.github
 * @Modified By:
 */
public class IopExportHbaseToHdfs {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < 3) {
            System.err.println("Usage: <HBase tableName> <Local dir path> <current date>");
            System.exit(1);
        }

        HbaseUtil hbaseUtil = new HbaseUtil();
        Configuration conf = hbaseUtil.getConfiguration();

        conf.set("fs.defaultFS", "hdfs://nmsq");
        conf.set("dfs.nameservices", "nmsq");
        conf.set("dfs.ha.namenodes.nmsq", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.nmsq.nn1", "ss-b02-m12-a01-rh2288hv3-9:8070");
        conf.set("dfs.namenode.rpc-address.nmsq.nn2", "ss-b02-m12-a01-rh2288hv3-10:8070");
        conf.set("mapreduce.job.queuename", "root.bdoc.b_yz_app_td_yarn");
        conf.set("dfs.client.failover.proxy.provider.nmsq", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        String currentDate = DateUtils.toLongYMD(otherArgs[2]);
        System.out.println(currentDate);
        Long currentDate0Time = Long.parseLong(DateUtils.getUnixTimeStamp(currentDate + " 00:00:00"));
        System.out.println(currentDate0Time);
        Long currentDate20Time = Long.parseLong(DateUtils.getUnixTimeStamp(currentDate + " 20:00:00"));
        System.out.println(currentDate20Time);

        LongWritable minTimestamp = new LongWritable(currentDate0Time);
        LongWritable maxTimestamp = new LongWritable(currentDate20Time);

        DefaultStringifier.store(conf, minTimestamp, "minTimestampFilter");
        DefaultStringifier.store(conf, maxTimestamp, "maxTimestampFilter");


        Job job = Job.getInstance(conf, IopExportHbaseToHdfs.class.getName());
        job.setJarByClass(IopExportHbaseToHdfs.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Writable.class);

        job.setNumReduceTasks(0);

        Scan scan = new Scan();
        // 设定caching
        scan.setCaching(1000);
        // 对于mr程序来说必须设定为false
        scan.setCacheBlocks(false);


        TableMapReduceUtil.initTableMapperJob(
                otherArgs[0]
                , scan
                , IopExportHbaseToLocalMapper.class
                , Text.class
                , Text.class
                , job);
        TableMapReduceUtil.addDependencyJars(job);


        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }

    public static class IopExportHbaseToLocalMapper extends TableMapper<Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws InterruptedException, IOException {
            Configuration conf = context.getConfiguration();
            LongWritable minTimestampFilter = DefaultStringifier.load(conf, "minTimestampFilter", LongWritable.class);
            LongWritable maxTimestampFilter = DefaultStringifier.load(conf, "maxTimestampFilter", LongWritable.class);


            Long minTimestamp = Long.parseLong(minTimestampFilter.toString());
            Long maxTimestamp = Long.parseLong(maxTimestampFilter.toString());

            List<Cell> cellList = value.listCells();

            for (Cell cell : cellList) {
                Long recordTime = cell.getTimestamp() / 1000;
                if (recordTime > minTimestamp && recordTime < maxTimestamp) {
                    String phoneNo = Bytes.toString(cell.getRow());
                    String qualifier = Bytes.toString(cell.getQualifier());
                    String saledTime = DateUtils.gerUnixTime2String(recordTime.toString(), "yyyy-MM-dd HH:mm:ss");
                    outKey.set(key.get());
                    outValue.set(phoneNo + "|" + qualifier + "|" + saledTime);
                    context.write(outKey, outValue);
                }
            }
        }

    }
}
