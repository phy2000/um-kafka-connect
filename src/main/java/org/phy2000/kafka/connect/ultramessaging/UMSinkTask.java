/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.phy2000.kafka.connect.ultramessaging;

import com.latencybusters.lbm.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;

/**
 * UMSinkTask writes records to stdout or a file.
 */
public class UMSinkTask extends SinkTask {

    private static final Logger logger = LoggerFactory.getLogger(UMSinkTask.class);

    private String config;
    private String license;
    private String um_topic_prefix;
    private String filename;
    private PrintStream outputStream;

    private LBMSourceAttributes sattr = null;
    private LBMContextAttributes cattr = null;
    private LBMContext ctx = null;
    private SrcCB srccb = null;
    private Map<Object, LBMSource> sources = null;
    //private LBMObjectRecycler objRec = new LBMObjectRecycler();

    boolean block = true;
    ByteBuffer message = null;

    public UMSinkTask() {
    }

    // for testing
    public UMSinkTask(PrintStream outputStream) {
        filename = null;
        this.outputStream = outputStream;
    }

    @Override
    public String version() {
        return new UMSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        config = props.get(UMSinkConnector.UM_CONFIG_FILE);
        license = props.get(UMSinkConnector.UM_LICENSE_FILE);
        um_topic_prefix = props.get(UMSinkConnector.UM_TOPIC_PREFIX);
        filename = props.get(UMSinkConnector.FILE_CONFIG);
        logger.info("start() - config[" + config + "] license[" + license + "] um_topic_prefix[" + um_topic_prefix + "] filename[" + filename + "]");
        if (filename == null) {
            outputStream = System.out;
        } else {
            try {
                outputStream = new PrintStream(
                    Files.newOutputStream(Paths.get(filename), StandardOpenOption.CREATE, StandardOpenOption.APPEND),
                    false, StandardCharsets.UTF_8.name());
            } catch (IOException e) {
                throw new ConnectException("Couldn't find or create file '" + filename + "' for UMSinkTask", e);
            }
        }

        /* create LBM context & source */
        LBM lbm = null;
        try {
            LBM.setLicenseFile(license);
            lbm = new LBM();
        } catch (LBMException ex) {
            logger.error("Error initializing LBM for source: " + ex.toString());
            System.exit(1);
        }
        org.apache.log4j.BasicConfigurator.configure();
        log4jLogger lbmlogger = new log4jLogger(org.apache.log4j.Logger.getLogger(this.getClass()));
        lbm.setLogger(lbmlogger);
        logger.info("        - setLogger");

        try {
            sattr = new LBMSourceAttributes();
            //sattr.setObjectRecycler(objRec, null);
            cattr = new LBMContextAttributes();
            //cattr.setObjectRecycler(objRec, null);
        } catch (LBMException ex) {
            logger.warn("Error creating source/context attributes: " + ex.toString());
            System.exit(1);
        }
        try {
            ctx = new LBMContext(cattr);
        } catch (LBMException ex) {
            System.err.println("Error creating context: " + ex.toString());
            System.exit(1);
        }
        System.out.println("UMSink::start() created context");
        srccb = new SrcCB();
        sources = new HashMap<Object, LBMSource>();

        try {
            Thread.sleep(999);
        } catch (InterruptedException e) {
            System.err.println("lbmsrc: error--" + e);
        }
        logger.info("        - UMSinkConnector open for business");
    }

    private void confirmByteBuffer(int msglen) {
        if ((message != null) && (message.capacity() >= msglen))
            return;
        logger.info("UMSink::confirmByteBuffer - allocateDirect(" + msglen + ")");
        message = ByteBuffer.allocateDirect(msglen);
    }

    private LBMSource createSrc(Object topicKey) {
        String srcTopicString = um_topic_prefix + topicKey.toString();
        LBMTopic lbmTopic = null;
        LBMSource lbmSrc = null;
        try {
            lbmTopic =  ctx.allocTopic(srcTopicString, sattr);
        } catch (LBMException ex) {
            System.err.println("Error allocating topic: " + ex.toString());
            System.exit(1);
        }
        try {
            lbmSrc = ctx.createSource(lbmTopic, srccb, (Object)srcTopicString, null);
            //lbmSrc.addSourceCallback(srccb, (Object)srcTopicString);
            sources.put(topicKey, lbmSrc);
        } catch (LBMException ex) {
            System.err.println("Error creating source: " + ex.toString());
            System.exit(1);
        }
        logger.info("createSrc() - created source on topic[{}]", srcTopicString);
        return lbmSrc;
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        LBMSource lbmSrc;
        for (SinkRecord record : sinkRecords) {
            logger.info("put() - ************ record topic[{}] key[{}] kafka offset[{}] partition[{}]", record.topic(), record.key(), record.kafkaOffset(), record.kafkaPartition());
            if (record.value() instanceof byte[]) {
                byte[] byteArray = (byte[]) record.value();
                ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
                logger.info("      - writing sink record message [{}] from topic [{}] to log file [{}] ", StandardCharsets.UTF_8.decode(byteBuffer).toString(), record.topic(), logFilename());
                byteBuffer.position(0);
                outputStream.println(StandardCharsets.UTF_8.decode(byteBuffer).toString());
                byteBuffer.position(0);
                confirmByteBuffer(byteBuffer.limit());
                message.position(0);
                message.put(byteBuffer);

                Object topicKey = record.key();
                if (sources.containsKey(topicKey)) {
                    logger.info("                     found key[{}]... extracting existing source from sources map of size[{}]", record.key(), sources.size());
                    lbmSrc = sources.get(topicKey);
                } else {
                    logger.info("                     new key[{}]... adding it to sources map of size[{}]", record.key(), sources.size());
                    lbmSrc = createSrc(topicKey);
                    sources.put(record.key().toString(), lbmSrc);
                }
                try {
                    lbmSrc.send(message, 0, message.limit(), block ? 0 : LBM.SRC_NONBLOCK);
                } catch (LBMException ex) {
                    ex.printStackTrace();
                }
                message.rewind();
                logger.info("      - sent message [{}] to topic [{}]", StandardCharsets.UTF_8.decode(message).toString(), um_topic_prefix + topicKey.toString());
            }  else {
                logger.warn("      - record.value() is an instance of [{}]", record.value().getClass().toString());
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        logger.info("flush() - flushing {} output stream", logFilename());
        outputStream.flush();
    }

    @Override
    public void stop() {
        if (outputStream != null && outputStream != System.out) {
            logger.info("stop() - closing output stream");
            outputStream.close();
        }
        //objRec.close();
        /* try {
            src.close();
        } catch (LBMException ex) {
            System.err.println("Error closing source: " + ex.toString());
        } */
        ctx.close();

    }

    private String logFilename() {
        return filename == null ? "stdout" : filename;
    }
}

class SrcCB implements LBMSourceEventCallback {
    public boolean blocked = false;
    private static final Logger logger = LoggerFactory.getLogger(SrcCB.class);

    public int onSourceEvent(Object arg, LBMSourceEvent sourceEvent) {
        String clientname;
        String srcTopicString = (String)sourceEvent.clientObject();

        switch (sourceEvent.type()) {
            case LBM.SRC_EVENT_CONNECT:
                clientname = sourceEvent.dataString();
                logger.info("Receiver client [{}] connect on topic [{}]", clientname, srcTopicString);
                break;
            case LBM.SRC_EVENT_DISCONNECT:
                clientname = sourceEvent.dataString();
                logger.info("Receiver client [{}] disconnect on topic [{}]", clientname, srcTopicString);
                break;
            case LBM.SRC_EVENT_WAKEUP:
                blocked = false;
                break;
            case LBM.SRC_EVENT_TIMESTAMP:
                LBMSourceEventTimestampInfo tsInfo = sourceEvent.timestampInfo();
                logger.info("HR@{}.{}[SQN {}]", tsInfo.hrTimestamp().tv_sec(),
                            tsInfo.hrTimestamp().tv_nsec(), tsInfo.sequenceNumber());
                break;
            default:
                break;
        }
        sourceEvent.dispose();
        System.out.flush();
        return 0;
    }
}
