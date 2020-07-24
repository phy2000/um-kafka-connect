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
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import static org.apache.kafka.connect.data.Schema.BYTES_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

/**
 * UMSourceTask.
 */
public class UMSourceTask extends SourceTask {
    private static final Logger logger = LoggerFactory.getLogger(UMSourceTask.class);
    public static final String FILENAME_FIELD = "filename";
    public static final String POSITION_FIELD = "position";
    public static int um_verbose;
    private static int while_loop_count = 0;

    private String kafka_topic = null;
    private int batch_size;

    final private BlockingQueue<LBMMessage> msgQ = new LinkedBlockingDeque<>(1000);
    final private LBMObjectRecycler objRec = new LBMObjectRecycler();

    @Override
    public String version() {
        return new UMSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        um_verbose = Integer.parseInt(props.get(UMSourceConnector.UM_VERBOSE));
        System.out.println("UMSourceTask::start() um_verbose: " + um_verbose);
        int um_persist = Integer.parseInt(props.get(UMSourceConnector.UM_PERSIST));
        System.out.println("UMSourceTask::start() um_persist: " + um_persist);
        int um_wildcard = Integer.parseInt(props.get(UMSourceConnector.UM_WILDCARD));
        System.out.println("UMSourceTask::start() um_wildcard: " + um_wildcard);
        String um_config_filename = props.get(UMSourceConnector.UM_CONFIG_FILE);
        System.out.println("UMSourceTask::start() um_config_filename: " + um_config_filename);
        String um_license_filename = props.get(UMSourceConnector.UM_LICENSE_FILE);
        System.out.println("UMSourceTask::start() um_license_file: " + (props.get(UMSourceConnector.UM_LICENSE_FILE)));
        String um_topic = props.get(UMSourceConnector.UM_TOPIC);
        System.out.println("UMSourceTask::start() um_topic: " + um_topic);
        kafka_topic = props.get(UMSourceConnector.KAFKA_TOPIC);
        System.out.println("UMSourceTask::start() kafka_topic: " +  kafka_topic);
        batch_size = Integer.parseInt(props.get(UMSourceConnector.BATCH_SIZE));
        System.out.println("UMSourceTask::start() batch_size: " + batch_size);

        LBM lbm;
        try {
            LBM.setLicenseFile(um_license_filename);
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

        try {
            LBM.setConfiguration(um_config_filename);
        } catch (LBMException ex) {
            String errStr = String.format("Error LBM.setConfiguration(%s)", um_config_filename);
            logger.error(errStr, ex);
            throw new ConnectException(errStr, ex);
        }

        LBMContextAttributes ctx_attr;
        try {
            ctx_attr = new LBMContextAttributes();
            ctx_attr.setObjectRecycler(objRec, null);
            ctx_attr.enableSourceNotification();
        } catch (LBMException ex) {
            String errStr = "Error creating context attributes: " + ex.toString();
            logger.error(errStr, ex);
            throw new ConnectException(errStr, ex);
        }
        LBMWRcvSourceNotify srcNotify = new LBMWRcvSourceNotify();
        LBMContext ctx;
        try {
            ctx = new LBMContext(ctx_attr);
        } catch (LBMException ex) {
            String errStr = ("Error creating context: " + ex.toString());
            logger.error(errStr, ex);
            throw new ConnectException(errStr, ex);
        }
        System.out.println("UMSourceTask::start() created context");
        try {
            ctx.addSourceNotifyCallback(srcNotify);
        } catch (LBMException ex) {
            String errStr = ("Error adding source notification callback: " + ex.toString());
            logger.error(errStr, ex);
            throw new ConnectException(errStr, ex);
        }
        LBMReceiverAttributes rcv_attr;
        try {
            rcv_attr = new LBMReceiverAttributes();
            rcv_attr.setValue("use_late_join", "1");
            rcv_attr.setValue("use_otr", "2");
            if (um_persist != 0) {
                rcv_attr.setValue("ume_explicit_ack_only", "1");
                rcv_attr.setValue("ume_activity_timeout", "2000");
                rcv_attr.setValue("ume_state_lifetime", "3000");
            }
        } catch (LBMException ex) {
            String errStr = ("Error creating receiver attributes: " + ex.toString());
            logger.error(errStr, ex);
            throw new ConnectException(errStr, ex);
        }
        if (um_wildcard == 0) {
            UmRcvReceiver rcv = new UmRcvReceiver(msgQ);
            ctx_attr.setImmediateMessageCallback(rcv);
            System.out.println("UMSourceTask::start() set callback");

            LBMTopic topic;
            try {
                topic = new LBMTopic(ctx, um_topic, rcv_attr);
            }
            catch (LBMException ex)
            {
                String errStr = String.format("Error creating LBMTopic(%s): %s", um_topic, ex.toString());
                logger.error(errStr, ex);
                throw new ConnectException(errStr, ex);
            }
            System.out.println("UMSourceTask::start() created LBMTopic on [" + um_topic + "]");

            try {
                new LBMReceiver(ctx, topic, rcv, null);
            } catch (LBMException ex) {
                String errStr = String.format("Error creating LBMReceiver(%s): %s", um_topic, ex.toString());
                logger.error(errStr, ex);
                throw new ConnectException(errStr, ex);
            }
            System.out.println("UMSourceTask::start() created receiver");
        } else {
            LBMWildcardReceiverAttributes wrcv_attr;
            try {
                wrcv_attr = new LBMWildcardReceiverAttributes();
            } catch (LBMException ex) {
                String errStr = ("Error creating wildcard attributes: " + ex.toString());
                logger.error(errStr, ex);
                throw new ConnectException(errStr, ex);
            }
            LBMWRcvReceiver wrcv = new LBMWRcvReceiver(msgQ);
            LBMWildcardReceiver lbmwrcv = null;
            try {
                lbmwrcv = new LBMWildcardReceiver(ctx,
                        um_topic,
                        rcv_attr,
                        wrcv_attr,
                        wrcv,
                        null);
                ctx.enableImmediateMessageReceiver();
            } catch (LBMException ex) {
                System.err.println("Error creating wildcard receiver: " + ex.toString());
                System.exit(1);
            }
            wrcv.setLBMWildcardReceiver(lbmwrcv);
            System.out.println("UMSourceTask::start() created wildcard receiver");
        }
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
    public List<SourceRecord> poll() {
        ArrayList<SourceRecord> records;
        records = new ArrayList<>();

        if (while_loop_count++ >= 10000001) {
            if (um_verbose > 0)
                logger.info("poll() - entered while loop 10,000,000 times...");
            while_loop_count = 0;
        }

        LBMMessage msg;
        while ((msg = msgQ.poll()) != null) {
            if (um_verbose > 0)
                logger.info("poll() - received record topic[" + msg.topicName() + "] seqnum[" + msg.sequenceNumber() + "] for kafka topic[" + kafka_topic + "] msg.dataLength[" + msg.dataLength() + "] msg.dataString()[" + msg.dataString() + "]");
            if (um_verbose > 1)
                logger.info("         msg.data().length[" + msg.data().length + "] Arrays.toString(msg.data()[" + Arrays.toString(msg.data()) + "]");
            SourceRecord record = new SourceRecord(offsetKey(msg.topicName()), offsetValue(msg.sequenceNumber()),
                    kafka_topic, STRING_SCHEMA, msg.topicName(), BYTES_SCHEMA, msg.data());
            UMTopic.logLastSentSQN(msg.topicName(), msg.source(), msg.sequenceNumber());
            records.add(record);
            if (records.size() >= batch_size) {
                return records;
            }
        }
        return records;
    }

    @Override
    public void stop() {
        logger.info("Stopping");
        synchronized (this) {
            this.notify();
        }
    }

    private Map<String, String> offsetKey(String filename) { return Collections.singletonMap(FILENAME_FIELD, filename); }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }

    public void commit() {
        for (Map.Entry<String, UMTopic> entry : UMTopic.topicMap.entrySet()) {
            String topic = entry.getValue().topicString;
            Map<String, Object> offset;
            offset = context.offsetStorageReader().offset(offsetKey(topic));
            if (offset != null) {   // offset is null until the first commit after 1st write to a partition completes
                Object lastRecordedOffset = offset.get(POSITION_FIELD);
                if (lastRecordedOffset != null && !(lastRecordedOffset instanceof Long)) {
                    throw new ConnectException("commit() - last recorded offset position is the incorrect type");
                }
                if (lastRecordedOffset != null) {
                    if (um_verbose > 1)
                        logger.info("commit() - last recorded offset[{}] for topic[{}] from source[{}] with last sent sqn[{}]", lastRecordedOffset, topic, entry.getValue().sourceString, entry.getValue().lastSentSQN);
                    LinkedList<DeferredAck> list = entry.getValue().deferredAckList;
                    while (list.size() > 0) {
                        long sqn = list.getFirst().get_sqn();
                        if (sqn <= (Long)lastRecordedOffset) {
                            DeferredAck ackSQN = list.removeFirst();
                            UMEMessageAck ack = ackSQN.get_ack();
                            if (ack != null) {
                                try {
                                    ack.dispose();
                                } catch (LBMException ex) {
                                    ex.printStackTrace();
                                    throw new ConnectException("commit() - ack dispose failed");
                                }
                            }
                            if (um_verbose > 1)
                                logger.info("           freed [{}]", sqn);
                        } else {
                            break;
                        }
                    }
                }
            } else {
                logger.info("commit() - offset was null for topic[{}]; offset is assumed to be 0", topic);
            }
        }
    }
}

class LBMWRcvSourceNotify implements LBMSourceNotification {
    private final Logger logger = LoggerFactory.getLogger(LBMWRcvSourceNotify.class);

    public int sourceNotification(String topic, String source, Object cbArg) {
        logger.info("new topic [" + topic + "], source [" + source + "]");
        return 0;
    }
}

class DeferredAck {
    final private UMEMessageAck _ack;
    final private long _sqn;
    public DeferredAck(UMEMessageAck ack, long sqn) { _ack = ack; _sqn = sqn; }
    public UMEMessageAck get_ack() { return _ack; }
    public long get_sqn() { return _sqn; }
}

class UMTopic {
    private static final Logger logger = LoggerFactory.getLogger(UMTopic.class);
    final public static HashMap<String, UMTopic> topicMap = new HashMap<>(); // set of known topics by source
    final public String topicString;                       // duh
    final public String sourceString;                      // topic source string
    final public String sourceTopicString;                 // topic map key
    final public LinkedList<DeferredAck> deferredAckList;  // list of deferred acks for this source/topic pairing
    final public Boolean committed;                        // true if any messages are committed
    public long lastSentSQN = -1;                          // the last sent sequence number sent to kafka

    public UMTopic(String topicStr, String sourceStr) {
        topicString = topicStr;
        sourceString = sourceStr;
        sourceTopicString = topicStr + sourceStr;
        deferredAckList = new LinkedList<>();
        committed = false;
        topicMap.put(sourceTopicString, this);
        logger.info("UMTopics - created topic [{}] from source[{}]; current number of known topics[{}]", topicString, sourceString, topicMap.size());
    }

    public static void logLastSentSQN(String topicName, String source, long sequenceNumber) {
        String sourceTopicString = topicName + source;
        if (UMTopic.topicMap.containsKey(sourceTopicString)) {
            if (UMSourceTask.um_verbose > 2)
                logger.info("logLastSentSQN() - logged sqn[{}] on topic[{}] from source[{}]", sequenceNumber, topicName, source);
            UMTopic entry = UMTopic.topicMap.get(sourceTopicString);
            entry.lastSentSQN = sequenceNumber;
        } else {
            logger.warn("logLastSentSQN() - failed to log sqn[{}] on topic[{}] from source[{}]", sequenceNumber, topicName, source);
        }
    }

    public static void removeTopic(String topicStr, String sourceStr) {
        String sourceTopicString = topicStr + sourceStr;
        if (UMTopic.topicMap.containsKey(sourceTopicString)) {
            UMTopic.topicMap.remove(sourceTopicString);
            logger.info("removeTopic() - removed topic[{}] from source[{}] from UMTopic.map", topicStr, sourceStr);
        } else {
            logger.warn("removeTopic() - failed to remove topic[{}] from source[{}] from UMTopic.map", topicStr, sourceStr);
        }
    }
}

class UmRcvReceiver implements LBMReceiverCallback, LBMImmediateMessageCallback {
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

    final public BlockingQueue<LBMMessage> msgQ;

    private static final Logger logger = LoggerFactory.getLogger(UmRcvReceiver.class);

    UmRcvReceiver(BlockingQueue<LBMMessage> msg_queue) {
        msgQ = msg_queue;
    }

    public int onReceiveImmediate(Object cbArg, LBMMessage msg) {
        imsg_count++;
        if (UMSourceTask.um_verbose > 1)
            logger.info("onReceiveImmediate() - calling onReceive()...");
        return onReceive(cbArg, msg);
    }

    Boolean handleMsgData(LBMMessage msg) {
        if (stotal_msg_count == 0)
            data_start_time = System.currentTimeMillis();
        else
            data_end_time = System.currentTimeMillis();
        msg_count++;
        total_msg_count++;
        stotal_msg_count++;
        subtotal_msg_count++;
        byte_count += msg.dataLength();
        total_byte_count += msg.dataLength();

        if ((msg.flags() & LBM.MSG_FLAG_RETRANSMIT) != 0) {
            logger.warn("                - retransmit request");
            rx_msgs++;
        }
        if ((msg.flags() & LBM.MSG_FLAG_OTR) != 0) {
            logger.warn("                - off transport recovery");
            otr_msgs++;
        }
        // Will be picked up by "poll" thread
        while (true) {
            if (!msgQ.offer(msg)) {
                logger.warn("                - queue is full for seqnum[" + msg.sequenceNumber() + "] - waiting 1 second for retry");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException intEx) {
                    logger.warn("                - retry interrupted for seqnum[" + msg.sequenceNumber() + "] interrupt exception:" + intEx);
                    return false;
                }
            } else {
                if (UMSourceTask.um_verbose > 0)
                    logger.info("                - queued sqn [" + msg.sequenceNumber() + "]");
                return true;
            }
        }
    }

    public int onReceive(Object cbArg, LBMMessage msg) {
        boolean doDispose = true;
        switch (msg.type())
        {
            case LBM.MSG_DATA:
                if (handleMsgData(msg)) {
                    doDispose = false;
                } else {
                    logger.warn("onReceive() - TODO: what should we do about failed queuing?");
                }
                break;
            case LBM.MSG_BOS:
                logger.info("LBM.MSG_BOS");
                logger.info("onReceive() [" + msg.topicName() + "][" + msg.source() + "], Beginning of Transport Session");
                break;
            case LBM.MSG_EOS:
                logger.info("LBM.MSG_EOS");
                logger.info("onReceive() [" + msg.topicName() + "][" + msg.source() + "], End of Transport Session");
                subtotal_msg_count = 0;
                break;
            case LBM.MSG_UNRECOVERABLE_LOSS:
                logger.info("LBM.MSG_UNRECOVERABLE_LOSS");
                unrec_count++;
                total_unrec_count++;
                logger.warn("onReceive() [" + msg.topicName() + "][" + msg.source() + "], Unrecoverable Loss!");
                break;
            case LBM.MSG_UNRECOVERABLE_LOSS_BURST:
                burst_loss++;
                logger.info("onReceive() [" + msg.topicName() + "][" + msg.source() + "], Unrecoverable Burst Loss!");
                break;
            case LBM.MSG_REQUEST:
                logger.info("LBM.MSG_REQUEST");
                if (handleMsgData(msg)) {
                    doDispose = false;
                }
                break;
            case LBM.MSG_NO_SOURCE_NOTIFICATION:
                logger.info("LBM.MSG_NO_SOURCE_NOTIFICATION");
                logger.info("No source notification for topic [{}]: ", msg.topicName());
                break;
            default:
                logger.warn("onReceive() - Unknown lbm_msg_t type[{}][{}][{}]", msg.type(), msg.topicName(), msg.source());
                break;
        }
        System.out.flush();
        if (doDispose) {
            msg.dispose();
        }
        return 0;
    }

    private void end() {
        logger.info("Quitting.... received [{}] messages", total_msg_count);
        System.exit(0);
    }
}

class LBMWRcvReceiver implements LBMReceiverCallback, LBMImmediateMessageCallback {
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
    public LBMWildcardReceiver _wrcv;

    final boolean _end_on_eos = false;

    public long data_start_time = 0;
    public long data_end_time = 0;

    public int stotal_msg_count = 0;
    public long total_byte_count = 0;

    final public BlockingQueue<LBMMessage> msgQ;

    private static final Logger logger = LoggerFactory.getLogger(LBMWRcvReceiver.class);

    public LBMWRcvReceiver(BlockingQueue<LBMMessage> msg_queue) {
        msgQ = msg_queue;
    }

    public void setLBMWildcardReceiver(LBMWildcardReceiver lbmrcv) {
        _wrcv = lbmrcv;
    }

    public int onReceiveImmediate(Object cbArg, LBMMessage msg) {
        imsg_count++;
        if (UMSourceTask.um_verbose > 1)
            logger.info("onReceiveImmediate() - calling onReceive()...");
        return onReceive(cbArg, msg);
    }

    Boolean handleMsgData(LBMMessage msg) {
        String sourceTopicString = msg.topicName() + msg.source();
        if (!UMTopic.topicMap.containsKey(sourceTopicString)) {
            if (UMSourceTask.um_verbose > 1)
                logger.info("handleMsgData() - discovered a new topic[{}] from source[{}]", msg.topicName(), msg.source());
            new UMTopic(msg.topicName(), msg.source());
        }
        UMTopic entry = UMTopic.topicMap.get(sourceTopicString);
        try {
            entry.deferredAckList.add(new DeferredAck(msg.extractUMEAck(), msg.sequenceNumber()));
        } catch (LBMException e) {
            e.printStackTrace();
        }

        if (UMSourceTask.um_verbose > 1)
            logger.info("handleMsgData() - received msg topic[" + msg.topicName() + "] seqn[" + msg.sequenceNumber() + "] data[" + msg.dataString() + "]");
        if (stotal_msg_count == 0)
            data_start_time = System.currentTimeMillis();
        else
            data_end_time = System.currentTimeMillis();
        msg_count++;
        total_msg_count++;
        stotal_msg_count++;
        subtotal_msg_count++;
        byte_count += msg.dataLength();
        total_byte_count += msg.dataLength();

        if ((msg.flags() & LBM.MSG_FLAG_RETRANSMIT) != 0) {
            logger.warn("                - retransmit request");
            rx_msgs++;
        }
        if ((msg.flags() & LBM.MSG_FLAG_OTR) != 0) {
            logger.warn("                - off transport recovery");
            otr_msgs++;
        }
        // Will be picked up by "poll" thread
        while (true) {
            if (!msgQ.offer(msg)) {
                logger.warn("                - queue is full for seqnum[" + msg.sequenceNumber() + "] - waiting 1 second for retry");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException intEx) {
                    logger.warn("                - retry interrupted for seqnum[" + msg.sequenceNumber() + "] interrupt exception:" + intEx);
                    return false;
                }
            } else {
                if (UMSinkTask.um_verbose > 0)
                    logger.info("                - queued sqn [" + msg.sequenceNumber() + "]");
                return true;
            }
        }
    }

    public int onReceive(Object cbArg, LBMMessage msg) {
        boolean doDispose = true;
        switch (msg.type())
        {
            case LBM.MSG_DATA:
                if (handleMsgData(msg)) {
                    doDispose = false;
                } else {
                    logger.warn("onReceive() - TODO: what should we do about failed queuing?");
                }
                if (UMSourceTask.um_verbose > 0) {
                    long sqn = msg.sequenceNumber();
                    if ((msg.flags() & (LBM.MSG_FLAG_HF_32 | LBM.MSG_FLAG_HF_64)) != 0) {
                        sqn = msg.hfSequenceNumber();
                    }
                    System.out.format("@%d.%06d[%s%s][%s][%s]%s%s%s%s%s%s%s, %s bytes\n",
                            msg.timestampSeconds(), msg.timestampMicroseconds(), msg.topicName(),
                            ((msg.channelInfo() != null) ? ":" + msg.channelInfo().channelNumber() : ""),
                            msg.source(), sqn >= 0 ? sqn : msg.hfSequenceNumberBigInt(),
                            ((msg.flags() & LBM.MSG_FLAG_RETRANSMIT) != 0 ? "-RX" : ""),
                            ((msg.flags() & LBM.MSG_FLAG_OTR) != 0 ? "-OTR" : ""),
                            ((msg.flags() & LBM.MSG_FLAG_HF_64) != 0 ? "-HF64" : ""),
                            ((msg.flags() & LBM.MSG_FLAG_HF_32) != 0 ? "-HF32" : ""),
                            ((msg.flags() & LBM.MSG_FLAG_HF_DUPLICATE) != 0 ? "-HFDUP" : ""),
                            ((msg.flags() & LBM.MSG_FLAG_HF_PASS_THROUGH) != 0 ? "-PASS" : ""),
                            ((msg.flags() & LBM.MSG_FLAG_HF_OPTIONAL) != 0 ? "-HFOPT" : ""),
                            msg.dataLength());
                }
                break;
            case LBM.MSG_BOS:
                logger.info("LBM.MSG_BOS");
                logger.info("onReceive() [" + msg.topicName() + "][" + msg.source() + "], Beginning of Transport Session");
                break;
            case LBM.MSG_EOS:
                logger.info("LBM.MSG_EOS");
                logger.info("onReceive() [" + msg.topicName() + "][" + msg.source() + "], End of Transport Session");
                try {
                    _wrcv.deregister();
                } catch (LBMException e) {
                    e.printStackTrace();
                }
                UMTopic.removeTopic(msg.topicName(), msg.source());
                if (_end_on_eos) {
                    end();
                }
                break;
            case LBM.MSG_UNRECOVERABLE_LOSS:
                logger.info("LBM.MSG_UNRECOVERABLE_LOSS");
                unrec_count++;
                total_unrec_count++;
                if (UMSourceTask.um_verbose > 0) {
                    long sqn = msg.sequenceNumber();
                    if ((msg.flags() & (LBM.MSG_FLAG_HF_32 | LBM.MSG_FLAG_HF_64)) != 0) {
                        sqn = msg.hfSequenceNumber();
                    }
                    logger.info("[{}][{}][{}]{}{}-RESET", msg.topicName(), msg.source(),
                            ((sqn) >= 0 ? sqn : msg.hfSequenceNumberBigInt()),
                            ((msg.flags() & LBM.MSG_FLAG_HF_64) != 0 ? "-HF64" : ""),
                            ((msg.flags() & LBM.MSG_FLAG_HF_32) != 0 ? "-HF32" : ""));
                }
                break;
            case LBM.MSG_UNRECOVERABLE_LOSS_BURST:
                logger.info("LBM.MSG_UNRECOVERABLE_LOSS_BURST");
                burst_loss++;
                if (UMSourceTask.um_verbose > 0) {
                    logger.info("[{}][{}], LOST BURST", msg.topicName(), msg.source());
                }
                break;
            case LBM.MSG_REQUEST:
                logger.info("LBM.MSG_REQUEST");
                if (handleMsgData(msg)) {
                    doDispose = false;
                }
                if (UMSourceTask.um_verbose > 0) {
                    logger.info("Request [{}][{}], [{}] bytes [{}]",
                        msg.topicName(), msg.source(), msg.sequenceNumber(), msg.data().length);
                }
                break;
            case LBM.MSG_NO_SOURCE_NOTIFICATION:
                logger.info("LBM.MSG_NO_SOURCE_NOTIFICATION");
                logger.info("No source notification for topic [{}]: ", msg.topicName());
                break;
            case LBM.MSG_UME_REGISTRATION_ERROR:
                logger.info("LBM.MSG_UME_REGISTRATION_ERROR");
                logger.info("[{}][{}] UME registration error: [{}]", msg.topicName(), msg.source(), msg.data());
                break;
            case LBM.MSG_UME_REGISTRATION_SUCCESS:
                logger.info("LBM.MSG_UME_REGISTRATION_SUCCESS");
                logger.info("[{}][{}] UME registration successful. Src RegID [{}] Rcv RegID [{}]",
                        msg.topicName(), msg.source(), msg.sourceRegistrationId(), msg.receiverRegistrationId());
                break;
            case LBM.MSG_UME_REGISTRATION_CHANGE:
                logger.info("LBM.MSG_UME_REGISTRATION_CHANGE");
                logger.info("[{}][{}] UME registration change: [{}]", msg.topicName(), msg.source(), msg.dataString());
                break;
            case LBM.MSG_UME_REGISTRATION_SUCCESS_EX:
                logger.info("LBM.MSG_UME_REGISTRATION_SUCCESS_EX");
                UMERegistrationSuccessInfo reg = msg.registrationSuccessInfo();
                System.out.print("[" + msg.topicName() + "][" + msg.source()
                        + "] store " + reg.storeIndex() + ": "
                        + reg.store() + " UME registration successful. SrcRegID "
                        + reg.sourceRegistrationId() + " RcvRegID " + reg.receiverRegistrationId()
                        + ". Flags " + reg.flags() + " ");
                if ((reg.flags() & LBM.MSG_UME_REGISTRATION_SUCCESS_EX_FLAG_OLD) != 0)
                    System.out.print("OLD[SQN " + reg.sequenceNumber() + "] ");
                if ((reg.flags() & LBM.MSG_UME_REGISTRATION_SUCCESS_EX_FLAG_NOCACHE) != 0)
                    System.out.print("NOCACHE ");
                if ((reg.flags() & LBM.MSG_UME_REGISTRATION_SUCCESS_EX_FLAG_SRC_SID) != 0) {
                    System.out.print("Src Session ID 0x" + Long.toHexString(reg.sourceSessionId()) + " ");
                }
                System.out.println();
                break;
            case LBM.MSG_UME_REGISTRATION_COMPLETE_EX:
                logger.info("LBM.MSG_UME_REGISTRATION_COMPLETE_EX");
                UMERegistrationCompleteInfo regcomplete = msg.registrationCompleteInfo();
                System.out.print("[" + msg.topicName() + "][" + msg.source()
                        + "] UME registration complete. SQN " + regcomplete.sequenceNumber()
                        + ". Flags " + regcomplete.flags() + " ");
                if ((regcomplete.flags() & LBM.MSG_UME_REGISTRATION_COMPLETE_EX_FLAG_QUORUM) != 0) {
                    System.out.print("QUORUM ");
                }
                if ((regcomplete.flags() & LBM.MSG_UME_REGISTRATION_COMPLETE_EX_FLAG_RXREQMAX) != 0) {
                    System.out.print("RXREQMAX ");
                }
                if ((regcomplete.flags() & LBM.MSG_UME_REGISTRATION_COMPLETE_EX_FLAG_SRC_SID) != 0) {
                    System.out.print("Src Session ID 0x" + Long.toHexString(regcomplete.sourceSessionId()) + " ");
                }
                System.out.println();
                break;
            case LBM.MSG_UME_DEREGISTRATION_SUCCESS_EX:
                logger.info("LBM.MSG_UME_DEREGISTRATION_SUCCESS_EX");
                UMEDeregistrationSuccessInfo dereg = msg.deregistrationSuccessInfo();
                System.out.print("[" + msg.topicName() + "][" + msg.source()
                        + "] store " + dereg.storeIndex() + ": "
                        + dereg.store() + " UME deregistration successful. SrcRegID "
                        + dereg.sourceRegistrationId() + " RcvRegID " + dereg.receiverRegistrationId()
                        + ". Flags " + dereg.flags() + " ");
                System.out.println();
                break;
            case LBM.MSG_UME_DEREGISTRATION_COMPLETE_EX:
                logger.info("LBM.MSG_UME_DEREGISTRATION_COMPLETE_EX");
                logger.info("[{}][{}] UME deregistration complete ex: ", msg.topicName(), msg.source());
                break;
            default:
                logger.warn("onReceive() - Unknown lbm_msg_t type[{}][{}][{}]", msg.type(), msg.topicName(), msg.source());
                break;
        }
        System.out.flush();
        if (doDispose) {
            msg.dispose();
        }
        return 0;
    }

    private void end() {
        logger.info("Quitting.... received " + total_msg_count + " messages");
        System.exit(0);
    }
}
