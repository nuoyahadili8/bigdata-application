package com.teradata.bigdata.flume;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Project:
 * @Description:
 * @Version 1.0.0
 * @Throws SystemException:
 * @Author: <li>2019/9/5/005 Administrator Create 1.0
 * @Copyright ©2018-2019 al.github
 * @Modified By:
 */
public class BucketFileWriter {

    private static final Logger logger = LoggerFactory.getLogger(BucketFileWriter.class);
    private static final String IN_USE_EXT = ".tmp";
    /**
     * 这个锁确保一次只有一个线程可以打开一个文件。
     */
    private AtomicReference<String> fileExtensionDateStr;

    private OutputStream outputStream;

    private EventSerializer serializer;

    private String filePath;

    private String extension;

    /**
     * 关闭文件句柄并将临时文件重命名为永久文件名。可以安全地多次呼叫。记录hdfswriter.close（）异常。
     * @throws IOException
     * On failure to rename if temp file exists.
     */
    public BucketFileWriter() {
        fileExtensionDateStr =new AtomicReference(new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis()));
    }

    public BucketFileWriter(String dateFormat){
        fileExtensionDateStr =new AtomicReference(new SimpleDateFormat(dateFormat).format(System.currentTimeMillis()));
    }

    public void open(final String filePath, String serializerType,
                     Context serializerContext, final long rollInterval,
                     final ScheduledExecutorService timedRollerPool,
                     final FileWriterLinkedHashMap sfWriters,
                     final String extension) throws IOException {
        this.filePath = filePath;
        this.extension = extension;
        File file = new File(filePath + fileExtensionDateStr + extension + IN_USE_EXT);
        file.getParentFile().mkdirs();
        outputStream = new BufferedOutputStream(new FileOutputStream(file));
        logger.info("filename = " + file.getAbsolutePath());
        serializer = EventSerializerFactory.getInstance(serializerType,
                serializerContext, outputStream);
        serializer.afterCreate();
        if (rollInterval > 0) {
            Callable<Void> action = () -> {
                logger.debug(
                        "Rolling file ({}): Roll scheduled after {} sec elapsed.",
                        filePath + fileExtensionDateStr + IN_USE_EXT,
                        rollInterval);
                if (sfWriters.containsKey(filePath)) {
                    sfWriters.remove(filePath);
                }
                close();
                return null;
            };
            timedRollerPool.schedule(action, rollInterval, TimeUnit.SECONDS);
        }
    }

    public void append(Event event) throws IOException {
        serializer.write(event);
    }

    public boolean isBatchComplete() {
        return true;
    }

    public void flush() throws IOException {
        serializer.flush();
        outputStream.flush();

    }

    /**
     * 将bucketpath文件从.tmp重命名为永久位置。
     */
    private void renameBucket() {
        File srcPath = new File(filePath + fileExtensionDateStr + extension + IN_USE_EXT);
        File dstPath = new File(filePath + fileExtensionDateStr + extension);
        if (srcPath.exists()) {
            srcPath.renameTo(dstPath);
            logger.info("Renaming " + srcPath + " to " + dstPath);
        }
    }

    public synchronized void close() throws IOException, InterruptedException {
        if (outputStream != null) {
            outputStream.flush();
            outputStream.close();
        }
        renameBucket();
    }
}
