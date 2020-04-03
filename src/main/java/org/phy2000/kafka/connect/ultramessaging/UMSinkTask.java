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
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.data.Schema;
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

/**
 * UMSinkTask writes records to stdout or a file.
 */
public class UMSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(UMSinkTask.class);

    private String filename;
    private PrintStream outputStream;
    private String topicname = "um-out";
    private LBMSource src = null;
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
        filename = props.get(UMSinkConnector.FILE_CONFIG);
        logger.info("start() - filename[" + filename + "]");
        if (filename == null) {
            outputStream = System.out;
        } else {
            try {
                outputStream = new PrintStream(
                    Files.newOutputStream(Paths.get(filename), StandardOpenOption.CREATE, StandardOpenOption.APPEND),
                    false,
                    StandardCharsets.UTF_8.name());
            } catch (IOException e) {
                throw new ConnectException("Couldn't find or create file '" + filename + "' for UMSinkTask", e);
            }
        }

        /* create LBM context & source */
        LBM lbm = null;
        try
        {
            lbm = new LBM();
        }
        catch (LBMException ex)
        {
            logger.warn("Error initializing LBM for source: " + ex.toString());
            System.exit(1);
        }
        org.apache.log4j.BasicConfigurator.configure();
        log4jLogger lbmlogger = new log4jLogger(org.apache.log4j.Logger.getLogger(this.getClass()));
        lbm.setLogger(lbmlogger);
        System.out.println("UMSink::start() setLogger");

        LBMSourceAttributes sattr = null;
        LBMContextAttributes cattr = null;
        try
        {
            sattr = new LBMSourceAttributes();
            cattr = new LBMContextAttributes();
        }
        catch (LBMException ex)
        {
            logger.warn("Error creating source/context attributes: " + ex.toString());
            System.exit(1);
        }
        LBMContext ctx = null;
        try
        {
            ctx = new LBMContext(cattr);
        }
        catch (LBMException ex)
        {
            System.err.println("Error creating context: " + ex.toString());
            System.exit(1);
        }
        System.out.println("UMSink::start() created context");
        LBMTopic topic = null;
        try
        {
            topic =  ctx.allocTopic(topicname, sattr);
        }
        catch (LBMException ex)
        {
            System.err.println("Error allocating topic: " + ex.toString());
            System.exit(1);
        }
        SrcCB srccb = new SrcCB();
        try
        {
            src = ctx.createSource(topic, srccb, null, null);
        }
        catch (LBMException ex)
        {
            System.err.println("Error creating source: " + ex.toString());
            System.exit(1);
        }
        System.out.println("UMSink::start() created source");
        try
        {
            Thread.sleep(999);
        }
        catch (InterruptedException e)
        {
            System.err.println("lbmsrc: error--" + e);
        }
        logger.info("Sending to topic[" + topicname + "]");
    }

    private void confirmByteBuffer(int msglen) {
        if ((message != null) && (message.capacity() >= msglen))
            return;
        logger.info("UMSink::confirmByteBuffer - allocateDirect(" + msglen + ")");
        message = ByteBuffer.allocateDirect(msglen);
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord record : sinkRecords) {

            logger.info("Writing to[{}] value[{}] valueSchema[{}] Timestamp[{}] Topic[{}]", logFilename(), record.value(), record.valueSchema(), record.timestamp(), record.topic());
            if (record.value() instanceof byte[]) {
                byte[] byteArray = (byte[]) record.value();
                ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
                outputStream.println(StandardCharsets.UTF_8.decode(byteBuffer).toString());
                byteBuffer.position(0);
                confirmByteBuffer(byteBuffer.limit());
                message.position(0);
                message.put(byteBuffer);
                try {
                    src.send(message, 0, message.limit(), block ? 0 : LBM.SRC_NONBLOCK);
                } catch (LBMException ex) {
                    ex.printStackTrace();
                }
                message.rewind();
                logger.info("sent message [{}]", StandardCharsets.UTF_8.decode(message).toString());
            }  else {
                logger.warn("record.value() is an instance of [{}]", record.value().getClass().toString());
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        logger.info("Flushing output stream for {}", logFilename());
        outputStream.flush();
    }

    @Override
    public void stop() {
        if (outputStream != null && outputStream != System.out)
            outputStream.close();
    }

    private String logFilename() {
        return filename == null ? "stdout" : filename;
    }
}

class SrcCB implements LBMSourceEventCallback
{
    public boolean blocked = false;
    private static final Logger logger = LoggerFactory.getLogger(SrcCB.class);

    public int onSourceEvent(Object arg, LBMSourceEvent sourceEvent)
    {
        String clientname;

        switch (sourceEvent.type())
        {
            case LBM.SRC_EVENT_CONNECT:
                clientname = sourceEvent.dataString();
                logger.info("Receiver connect " + clientname);
                break;
            case LBM.SRC_EVENT_DISCONNECT:
                clientname = sourceEvent.dataString();
                logger.info("Receiver disconnect " + clientname);
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
