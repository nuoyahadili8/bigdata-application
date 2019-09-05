package com.teradata.bigdata.flume;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.*;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @Project:
 * @Description:
 * @Version 1.0.0
 * @Throws SystemException:
 * @Author: <li>2019/9/5/005 Administrator Create 1.0
 * @Copyright ©2018-2019 al.github
 * @Modified By:
 */
public class FileSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(FileSink .class );

    private String path ;
    private static final String defaultFileName = "FlumeData";
    private static final int defaultMaxOpenFiles = 50;

    /**
     * Default length of time we wait for blocking BucketWriter calls before
     * timing out the operation. Intended to prevent server hangs.
     */

    private long txnEventMax ;

    private FileWriterLinkedHashMap sfWriters ;

    private String serializerType ;
    private Context serializerContext ;

    private boolean needRounding = false;
    private int roundUnit = Calendar.SECOND;
    private int roundValue = 1;
    private SinkCounter sinkCounter ;

    private int maxOpenFiles ;
    private String extension = ".AVL";
    private String dateFormatStr = "yyyyMMddHHmmss";

    private ScheduledExecutorService timedRollerPool ;

    private long rollInterval ;

    @Override
    public void configure(Context context) {

        String directory = Preconditions. checkNotNull(
                context.getString( "file.path"), "file.path is required");
        String fileName = context.getString( "file.filePrefix", defaultFileName);
        extension = context.getString("file.extension",extension);
        this.path = directory + "/" + fileName;

        maxOpenFiles = context.getInteger("file.maxOpenFiles" , defaultMaxOpenFiles);
        dateFormatStr = context.getString("file.dateFormat" , dateFormatStr);

        serializerType = context.getString("sink.serializer" , "TEXT" );
        serializerContext = new Context(context.getSubProperties(EventSerializer. CTX_PREFIX));
        txnEventMax = context.getLong("file.txnEventMax" , 1L);
        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }

        rollInterval = context.getLong("file.rollInterval" , 30L);
        String rollerName = "hdfs-" + getName() + "-roll-timer-%d" ;
        timedRollerPool = Executors.newScheduledThreadPool( maxOpenFiles,
                new ThreadFactoryBuilder().setNameFormat(rollerName).build());
    }

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        List<BucketFileWriter> writers = Lists. newArrayList();
        transaction.begin();
        try {
            Event event = null;
            int txnEventCount = 0;
            for (txnEventCount = 0; txnEventCount < txnEventMax; txnEventCount++) {
                event = channel.take();
                if (event == null) {
                    break;
                }

                // reconstruct the path name by substituting place holders
                String realPath = BucketPath.escapeString(path, event.getHeaders(), needRounding,
                        roundUnit, roundValue );
                BucketFileWriter bucketFileWriter = sfWriters.get(realPath);

                // we haven't seen this file yet, so open it and cache the
                // handle
                if (bucketFileWriter == null) {
                    bucketFileWriter = new BucketFileWriter(dateFormatStr);
                    bucketFileWriter.open(realPath, serializerType,
                            serializerContext, rollInterval , timedRollerPool,
                            sfWriters,extension);
                    sfWriters.put(realPath, bucketFileWriter);
                }

                // track the buckets getting written in this transaction
                if (!writers.contains(bucketFileWriter)) {
                    writers.add(bucketFileWriter);
                }

                // Write the data to File
                bucketFileWriter.append(event);
            }

            if (txnEventCount == 0) {
                sinkCounter.incrementBatchEmptyCount();
            } else if (txnEventCount == txnEventMax) {
                sinkCounter.incrementBatchCompleteCount();
            } else {
                sinkCounter.incrementBatchUnderflowCount();
            }

            // flush all pending buckets before committing the transaction
            for (BucketFileWriter bucketFileWriter : writers) {
                if (!bucketFileWriter.isBatchComplete()) {
                    flush(bucketFileWriter);
                }
            }
            transaction.commit();
            if (txnEventCount > 0) {
                sinkCounter.addToEventDrainSuccessCount(txnEventCount);
            }

            if (event == null) {
                return Status.BACKOFF ;
            }
            return Status.READY ;
        } catch (IOException eIO) {
            transaction.rollback();
            logger.warn("File IO error" , eIO);
            return Status.BACKOFF ;
        } catch (Throwable th) {
            transaction.rollback();
            logger.error("process failed" , th);
            if (th instanceof Error) {
                throw (Error) th;
            } else {
                throw new EventDeliveryException(th);
            }
        } finally {
            transaction.close();
        }
    }

    private void flush(BucketFileWriter bucketFileWriter) throws IOException {
        bucketFileWriter.flush();
    }

    @Override
    public synchronized void start() {
        super.start();
        this.sfWriters = new FileWriterLinkedHashMap(maxOpenFiles);
        sinkCounter.start();

    }

}
