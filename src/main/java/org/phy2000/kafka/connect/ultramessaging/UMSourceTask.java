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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import com.latencybusters.lbm.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.connect.data.Schema.BYTES_SCHEMA;

/**
 * UMSourceTask reads from stdin or a file.
 */
public class UMSourceTask extends SourceTask {
    private static final Logger logger = LoggerFactory.getLogger(UMSourceTask.class);
    private InputStream stream;
    private BufferedReader reader = null;
    private char[] buffer = new char[1024];
    private int offset = 0;

    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
    public static final String FILENAME_FIELD = "filename";
    public static final String POSITION_FIELD = "position";

    private String um_config_filename;
    private String um_topic = null;
    private String kafka_topic = null;
    private int batchSize = UMSourceConnector.DEFAULT_TASK_BATCH_SIZE;
    private Long streamOffset;
    private LBM lbm;

    private static int while_loop_count = 0;

    //BlockingQueue<LBMMessage> msgQ = new LinkedBlockingDeque<>(1000);
    BlockingQueue<LBMMessage> msgQ = new LinkedBlockingDeque<>(1000);

    LBMObjectRecycler objRec = new LBMObjectRecycler();

    @Override
    public String version() {
        return new UMSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        batchSize = Integer.parseInt(props.get(UMSourceConnector.TASK_BATCH_SIZE_CONFIG));
        System.out.println("UMSourceTask::start() batchSize: " + batchSize);
        // - Get UM Config file
        um_config_filename = props.get(UMSourceConnector.UM_CONFIG_FILE);
        System.out.println("UMSourceTask::start() um_config_filename: " + um_config_filename);
        // - Get Topic Names
        // TODO - handle multiple topics and/or partitioning
        um_topic = props.get(UMSourceConnector.UM_TOPIC);
        System.out.println("UMSourceTask::start() um_topic: " +  um_topic);
        kafka_topic = props.get(UMSourceConnector.KAFKA_TOPIC);
        System.out.println("UMSourceTask::start() kafka_topic: " +  kafka_topic);

        try {
            // - Set UM License file or License string
            System.out.println("UMSourceTask::start() UM_LICENSE_STRING: " + (props.get(UMSourceConnector.UM_LICENSE_STRING)));
            System.out.println("UMSourceTask::start() UM_LICENSE_FILE: " + (props.get(UMSourceConnector.UM_LICENSE_FILE)));
            if (props.get(UMSourceConnector.UM_LICENSE_STRING) != null) {
                System.out.println("UMSourceTask::start() Using UM license string");
                LBM.setLicenseString(props.get(UMSourceConnector.UM_LICENSE_STRING));
            } else {
                System.out.println("UMSourceTask::start() Using UM license file");
                LBM.setLicenseFile(props.get(UMSourceConnector.UM_LICENSE_FILE));
            }
            // Init LBM
            lbm = new LBM();
        } catch (LBMException ex) {
            String errStr = "Error initializing LBM: " + ex.toString();
            logger.error(errStr, ex);
            throw new ConnectException(errStr, ex);
        }
        org.apache.log4j.BasicConfigurator.configure();
        log4jLogger lbmlogger = new log4jLogger(org.apache.log4j.Logger.getLogger(this.getClass()));
        lbm.setLogger(lbmlogger);
        System.out.println("UMSourceTask::start() setLogger");

        /*try {
            LBM.setConfiguration(um_config_filename);
        } catch (LBMException ex) {
            String errStr = String.format("Error LBM.setConfiguration(%s)", um_config_filename);
            logger.error(errStr, ex);
            throw new ConnectException(errStr, ex);
        } */

        LBMContextAttributes ctx_attr = null;
        try {
            ctx_attr = new LBMContextAttributes();
            ctx_attr.setObjectRecycler(objRec, null);
        } catch (LBMException ex) {
            String errStr = "Error creating context attributes: " + ex.toString();
            logger.error(errStr, ex);
            throw new ConnectException(errStr, ex);
        }
        // Create Callback object
        UmRcvReceiver rcv = new UmRcvReceiver(um_topic, kafka_topic, msgQ);
        ctx_attr.setImmediateMessageCallback(rcv);
        System.out.println("UMSourceTask::start() set callback");

        // Create Context object
        LBMContext ctx = null;
        try {
            /* ctx_attr.setValue("request_tcp_interface", "192.168.254.0/24");
            ctx_attr.setValue("default_interface", "192.168.254.0/24");
            ctx_attr.setValue("request_tcp_port_low", "31000");
            ctx_attr.setValue("request_tcp_port_high", "31100");
            ctx_attr.setValue("resolver_multicast_interface", "192.168.254.0/24");
            ctx_attr.setValue("resolver_multicast_address", "225.11.15.85");
            ctx_attr.setValue("resolver_multicast_port", "13965");
             */
            ctx = new LBMContext(ctx_attr);
        } catch (LBMException ex) {
            String errStr = ("Error creating context: " + ex.toString());
            logger.error(errStr, ex);
            throw new ConnectException(errStr, ex);
        }
        System.out.println("UMSourceTask::start() created context");

        // - Create Topic object
        LBMTopic topic = null;
        try {
            LBMReceiverAttributes rcv_attr = new LBMReceiverAttributes();
            rcv_attr.setObjectRecycler(objRec, null);
            topic = new LBMTopic(ctx, um_topic, rcv_attr);
        }
        catch (LBMException ex)
        {
            String errStr = String.format("Error creating LBMTopic(%s): %s", um_topic, ex.toString());
            logger.error(errStr, ex);
            throw new ConnectException(errStr, ex);
        }
        System.out.println("UMSourceTask::start() created LBMTopic on [" + um_topic + "]");

        LBMReceiver lbmrcv = null;
        // - Create Receiver object
        try {
            lbmrcv = new LBMReceiver(ctx, topic, rcv, null);
        } catch (LBMException ex) {
            String errStr = String.format("Error creating LBMReceiver(%s): %s", um_topic, ex.toString());
            logger.error(errStr, ex);
            throw new ConnectException(errStr, ex);
        }
        System.out.println("UMSourceTask::start() created receiver");
        System.out.println("Sleeping 2 seconds ");
        for (int i = 0; i < 2; ++i) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("Thread.sleep failed!");
            }
            System.out.println(".");
        }
        System.out.println("done!");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        ArrayList<SourceRecord> records = null;
        records = new ArrayList<>();

        if (while_loop_count++ >= 10000000) {
            logger.info("poll() - entered while loop 10,000,000 times...");
            while_loop_count = 0;
        }

        LBMMessage msg = null;
        while ((msg = msgQ.poll()) != null) {
            logger.info("poll() - received record topic[" + msg.topicName() + "] seqnum[" + msg.sequenceNumber() + "] for kafka topic[" + kafka_topic + "] msg.dataLength[" + msg.dataLength() + "] msg.dataString()[" + msg.dataString() + "]");
            logger.info("         msg.data().length[" + msg.data().length + "] Arrays.toString(msg.data()[" + Arrays.toString(msg.data()) + "]");
            SourceRecord record = new SourceRecord(offsetKey(msg.topicName()), offsetValue(msg.sequenceNumber()),
                    kafka_topic, null, BYTES_SCHEMA, msg.data());
            records.add(record);
            if (records.size() >= batchSize) {
                return records;
            }
        }
        return records;
    }

    @Override
    public void stop() {
        logger.trace("Stopping");
        synchronized (this) {
            try {
                if (stream != null && stream != System.in) {
                    stream.close();
                    logger.trace("Closed input stream");
                }
            } catch (IOException e) {
                logger.error("Failed to close UMSourceTask stream: ", e);
            }
            this.notify();
        }
    }

    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap(FILENAME_FIELD, filename);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }

    private String logFilename() {
        return um_config_filename == null ? "stdin" : um_config_filename;
    }
}

class UmRcvReceiver implements LBMReceiverCallback, LBMImmediateMessageCallback
{
    public long imsg_count = 0;
    public long msg_count = 0;
    public long total_msg_count = 0;
    public long subtotal_msg_count = 0;
    public long byte_count = 0;
    public long unrec_count = 0;
    public long total_unrec_count = 0;
    public long burst_loss = 0;
    public long rx_msgs = 0;
    public long otr_msgs = 0;

    public long data_start_time = 0;
    public long data_end_time = 0;

    public int stotal_msg_count = 0;
    public long total_byte_count = 0;
    String umTopic = "";
    String kafkaTopic = "";

    BlockingQueue<LBMMessage> msgQ;

    UmRcvReceiver(String um_topic, String kafka_topic, BlockingQueue<LBMMessage> msg_queue) {
        System.out.println("UmRcvReceiver::UmRcvReceiver()");
        umTopic = um_topic;
        kafkaTopic = kafka_topic;
        msgQ = msg_queue;
    }
    private static final Logger logger = LoggerFactory.getLogger(UmRcvReceiver.class);

    // This immediate-mode receiver is *only* used for topicless
    // immediate-mode sends.  Immediate sends that use a topic
    // are received with normal receiver objects.
    public int onReceiveImmediate(Object cbArg, LBMMessage msg)
    {
        imsg_count++;
        logger.info("onReceiveImmediate() Calling onReceive()...");
        return onReceive(cbArg, msg);
    }

    Boolean handleMsgData(Object cbArg, LBMMessage msg) {
        if (stotal_msg_count == 0)
            data_start_time = System.currentTimeMillis();
        else
            data_end_time = System.currentTimeMillis();
        msg_count++;
        total_msg_count++;
        stotal_msg_count++;
        subtotal_msg_count++;
        /* When using Zero Object Delivery, be sure to use the
         * LBMMessage.dataLength() method to obtain message length,
         * rather than using LBMMessage.data().length.  Calling
         * LBMMessage.data() will cause the creation of a new
         * byte[] array object, which is unnecessary if all you need
         * is the message length. */
        byte_count += msg.dataLength();
        total_byte_count += msg.dataLength();

        byte[] m = msg.data();
        //ByteBuffer ms = msg.dataBuffer();

        if ((msg.flags() & LBM.MSG_FLAG_RETRANSMIT) != 0) {
            logger.warn("handleMsgData() Retransmit request");
            rx_msgs++;
        }
        if ((msg.flags() & LBM.MSG_FLAG_OTR) != 0) {
            logger.warn("handleMsgData() Off transport recovery");
            otr_msgs++;
        }
        // Will be picked up by "poll" thread
        while (true) {
            if (!msgQ.offer(msg)) {
                logger.warn("handleMsgData() Queue is full for seqnum[" + msg.sequenceNumber() + "] - waiting 1 second for retry");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException intEx) {
                    logger.warn("handleMsgData() Retry interrupted for seqnum[" + msg.sequenceNumber() + "] interrupt exception:" + intEx);
                    return false;
                }
            } else {
                logger.info("handleMsgData() Received seqnum [" + msg.sequenceNumber() + "] msg.dataString()[" + msg.dataString() + "]");
                return true;
            }
        }
    }
    public int onReceive(Object cbArg, LBMMessage msg)
    {
        boolean doDispose = true;
        switch (msg.type())
        {
            case LBM.MSG_DATA:
                if (handleMsgData(cbArg, msg)) {
                	doDispose = false;
                } else {
                    logger.warn("onReceive() - TODO: what should we do about failed queuing?");
                }
                break;
            case LBM.MSG_BOS:
                logger.info("onReceive() [" + msg.topicName() + "][" + msg.source() + "], Beginning of Transport Session");
                break;
            case LBM.MSG_EOS:
                logger.info("onReceive() [" + msg.topicName() + "][" + msg.source() + "], End of Transport Session");
                subtotal_msg_count = 0;
                break;
            case LBM.MSG_UNRECOVERABLE_LOSS:
                unrec_count++;
                total_unrec_count++;
                logger.warn("onReceive() [" + msg.topicName() + "][" + msg.source() + "], Unrecoverable Loss!");
                break;
            case LBM.MSG_UNRECOVERABLE_LOSS_BURST:
                burst_loss++;
                logger.info("onReceive() [" + msg.topicName() + "][" + msg.source() + "], Unrecoverable Burst Loss!");
                break;
            case LBM.MSG_REQUEST:
            	if (handleMsgData(cbArg, msg)) {
                	doDispose = false;
                }
                break;
            case LBM.MSG_HF_RESET:
                long sqn = msg.sequenceNumber();
                if ((msg.flags() & (LBM.MSG_FLAG_HF_32 | LBM.MSG_FLAG_HF_64)) != 0) {
                    sqn = msg.hfSequenceNumber();
                }
                logger.info(String.format("[%s][%s][%s]%s%s%s%s-RESET\n", msg.topicName(), msg.source(), sqn >= 0 ? sqn : msg.hfSequenceNumberBigInt(),
                        ((msg.flags() & LBM.MSG_FLAG_RETRANSMIT) != 0 ? "-RX" : ""),
                        ((msg.flags() & LBM.MSG_FLAG_OTR) != 0 ? "-OTR" : ""),
                        ((msg.flags() & LBM.MSG_FLAG_HF_64) != 0 ? "-HF64" : ""),
                        ((msg.flags() & LBM.MSG_FLAG_HF_32) != 0 ? "-HF32" : "")));
                break;
            default:
                logger.warn("onReceive() - Unknown lbm_msg_t type " + msg.type() + " [" + msg.topicName() + "][" + msg.source() + "]");
                break;
        }
        if (doDispose) {
            msg.dispose();
        }
        return 0;
    }
}
