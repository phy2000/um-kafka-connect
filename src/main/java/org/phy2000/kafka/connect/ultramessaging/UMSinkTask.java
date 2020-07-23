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
import java.net.InetSocketAddress;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * UMSinkTask writes records to a file and to UM streaming or persistence.
 */
public class UMSinkTask extends SinkTask {

    static UMSinkTask umSinkTask = null;
    static Collection<TopicPartition> taskPartitions = null;
    static Set<String> taskTopics = null;

    public static int um_verbose;
    public static boolean um_persist;
    public static boolean um_wildcard;
    public static List<String> kafkaTopics;

    public static int stablerecv = 0;
    public static long sleep_before_sending = 0;

    private static final Logger logger = LoggerFactory.getLogger(UMSinkTask.class);
    private String um_topic;
    private String filename;
    private PrintStream outputStream;

    private LBMSourceAttributes sattr = null;
    private LBMContextAttributes cattr = null;
    private LBMContext ctx = null;
    private Map<Object, LBMSource> sources = null;
    final private BlockingQueue<String> resumeQ = new LinkedBlockingDeque<>(100);

    LBMSource lbmSrc;

    final boolean block = true;
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

    /************************************************************************************************************
     persist wildcard   description
        0       0       create LBM sources for each member of kafkaTopics (aka topics)
        0       1       create LBM sources dynamically using record.key() as UM topic prepended w/um_topic prefix
        1       0       create UME source for each member of kafkaTopics (aka topics)
        1       1       defer UME source creation using record.key() as UM topic
     ************************************************************************************************************/
    @Override
    public void start(Map<String, String> props) {
        umSinkTask = this;
        kafkaTopics = Arrays.asList(props.get(UMSinkConnector.KAFKA_TOPICS).split("\\s*,\\s*"));
        String config = props.get(UMSinkConnector.UM_CONFIG_FILE);
        String license = props.get(UMSinkConnector.UM_LICENSE_FILE);
        um_verbose = Integer.parseInt(props.get(UMSinkConnector.UM_VERBOSE));
        um_persist = Integer.parseInt(props.get(UMSinkConnector.UM_PERSIST)) > 0;
        um_wildcard = Integer.parseInt(props.get(UMSinkConnector.UM_WILDCARD)) > 0;
        um_topic = props.get(UMSinkConnector.UM_TOPIC);
        filename = props.get(UMSinkConnector.FILE_CONFIG);

        if (um_verbose > 0) {
            logger.info("start() - config[{}] license[{}] um_topic[{}] filename[{}] um_persist[{}] um_wildcard[{}] um_verbose[{}]", config, license, um_topic, filename, um_persist, um_wildcard, um_verbose);
            if (um_verbose >= 2) {
                kafkaTopics.forEach((kafkaTopic) -> logger.info("          kafka topic [{}]", kafkaTopic));
            }
        }

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
        if (um_verbose > 1)
            logger.info("        - setLogger");

        try {
            sattr = new LBMSourceAttributes();
            cattr = new LBMContextAttributes();
            cattr.setFromXml(cattr.getValue("context_name"));
        } catch (LBMException ex) {
            logger.warn("Error creating source/context attributes: " + ex.toString());
            System.exit(1);
        }

        if (um_persist) {
            if (check_ume_store_config(sattr) == -1) {
                logger.warn("no UME stores have been specified, exiting program...");
                System.exit(1);
            }
        }

        try {
            ctx = new LBMContext(cattr);
        } catch (LBMException ex) {
            System.err.println("Error creating context: " + ex.toString());
            System.exit(1);
        }
        System.out.println("UMSink::start() created context");
        sources = new HashMap<>();

        if (!um_wildcard) {
            kafkaTopics.forEach((topic) -> {
                lbmSrc = createSrc((Object)topic);
            });
        }

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
        String srcTopicString;
        if (um_wildcard) {
            srcTopicString = um_topic + topicKey.toString();
        } else {
            srcTopicString = topicKey.toString();
        }
        LBMTopic lbmTopic = null;
        LBMSource lbmSrc = null;
        try {
            lbmTopic =  ctx.allocTopic(srcTopicString, sattr);
        } catch (LBMException ex) {
            System.err.println("Error allocating topic: " + ex.toString());
            System.exit(1);
        }
        try {
            if (um_persist) {
                UMESrcCB umeSrcCB =  new UMESrcCB(um_verbose, srcTopicString, resumeQ);
                lbmSrc = ctx.createSource(lbmTopic, umeSrcCB, srcTopicString, null);
            } else {
                SrcCB srcCB = new SrcCB();
                lbmSrc = ctx.createSource(lbmTopic, srcCB, srcTopicString, null);
                //lbmSrc.addSourceCallback(srcCB, (Object)srcTopicString);
            }
            sources.put(topicKey, lbmSrc);
        } catch (LBMException ex) {
            System.err.println("Error creating source: " + ex.toString());
            System.exit(1);
        }
        logger.info("createSrc() - created source on topic[{}]", srcTopicString);
        return lbmSrc;
    }

    private static int check_ume_store_config(LBMSourceAttributes sattr) {
        // flag whether a store name is present
        String store_name = null;
        try {
            store_name = sattr.getValue("ume_store_name");
        } catch (LBMException ex) {
            logger.warn("Error getting source attribute: " + ex.toString());
        }
        boolean hasStoreName = store_name != null && store_name.length() != 0;

        UMEStoreEntry[] stores = sattr.getStores();
        UMEStoreGroupEntry[] groups = sattr.getStoreGroups();
        InetSocketAddress addr = null;

        if (stores.length < 1 && !hasStoreName) {
            logger.error("No UME stores specified. To send without a store, please disable um.persist");
            return -1; /* exit program */
        }
        if (um_verbose > 0) {
            try {
                int store_behaviour = LBM.SRC_TOPIC_ATTR_UME_STORE_BEHAVIOR_QC;

                String storeBehavior = sattr.getValue("ume_store_behavior");
                if (groups.length > 0) {
                    for (int j = 0; j < groups.length; j++) {
                        logger.info("Group [{}]: Size [{}]", j, groups[j].groupSize());
                        for (int i = 0; i < stores.length; i++) {
                            if (stores[i].groupIndex() == groups[j].index()) {
                                addr = stores[i].address();
                                if (stores[i].isNamed()) {
                                    /* If the IP is 0.0.0.0, this store is specified by name. */
                                    logger.info("Store {}: {} DomainID  {}", i, stores[i].name(), stores[i].domainId());
                                } else {
                                    logger.info("Store {}: {} DomainID  {}", i, addr.toString(), stores[i].domainId());
                                }
                                if (stores[i].registrationId() != 0) {
                                    logger.info("Store {}: RegID {}", i, stores[i].registrationId());
                                }
                            }
                        }
                    }
                } else {
                    logger.info("No Store Groups - Number of Stores: {}", stores.length);
                    for (int i = 0; i < stores.length; i++) {
                        addr = stores[i].address();
                        if (stores[i].isNamed()) {
                            /* If the IP is 0.0.0.0, this store is specified by name. */
                            logger.info("Store {}: {} DomainID  {}", i, stores[i].name(), stores[i].domainId());
                        } else {
                            logger.info("Store {}: {} DomainID  {}", i, addr.toString(), stores[i].domainId());
                        }
                        if (stores[i].registrationId() != 0) {
                            logger.info("Store {}: RegID {}", i, stores[i].registrationId());
                        }
                    }
                }
            } catch (LBMException ex) {
                logger.warn("Error getting source attributes: [{}]", ex.toString());
            }
        }
        return 0;
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        logger.info("put() *******************************");
        for (SinkRecord record : sinkRecords) {
            if (um_verbose > 0)
                logger.info("      - record topic[{}] key[{}] kafka offset[{}] partition[{}]", record.topic(), record.key(), record.kafkaOffset(), record.kafkaPartition());
            if (record.value() instanceof byte[]) {
                byte[] byteArray = (byte[]) record.value();
                ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
                if (um_verbose > 1)
                    logger.info("      - writing sink record message [{}] from topic [{}] to log file [{}] ", StandardCharsets.UTF_8.decode(byteBuffer).toString(), record.topic(), logFilename());
                ((Buffer)byteBuffer).position(0);   // cast essential for Java 1.8
                outputStream.println(StandardCharsets.UTF_8.decode(byteBuffer).toString()); // writing to file.out
                ((Buffer)byteBuffer).position(0);
                confirmByteBuffer(byteBuffer.limit());
                ((Buffer)message).position(0);
                message.put(byteBuffer);

                if (um_wildcard) {
                    Object topicKey = record.key();
                    if (sources.containsKey(topicKey)) {
                        if (um_verbose >= 2)
                            logger.info("      - found wildcard key[{}]... extracting existing source from sources map of size [{}]", record.key(), sources.size());
                        lbmSrc = sources.get(topicKey);
                    } else {
                        if (um_verbose >= 1)
                            logger.info("      - new key[{}]... adding it to sources map of size [{}]", record.key(), sources.size());
                        lbmSrc = createSrc(topicKey);
                        sources.put(record.key().toString(), lbmSrc);
                    }
                } else {
                    Object topic = record.topic();
                    if (sources.containsKey(topic)) {
                        if (um_verbose >= 2) {
                            logger.info("      - found source for topic [{}] from sources map of size [{}]", topic, sources.size());
                        }
                        lbmSrc = sources.get(topic);
                    } else {
                        logger.warn("      - could not find source for topic [{}]", topic);
                        return;
                    }
                }
                try {
                    lbmSrc.send(message, 0, message.limit(), block ? 0 : LBM.SRC_NONBLOCK);
                    ((Buffer)message).rewind(); // cast essential for Java 1.8
                    if (um_verbose > 1)
                        logger.info("      - sent message [{}] to topic [{}]", StandardCharsets.UTF_8.decode(message).toString(), record.topic());
                } catch (UMENoRegException ex) {
                    ((Buffer)message).rewind(); // cast essential for Java 1.8
                    logger.warn("      - not currently registered with enough UME stores... could NOT send [{}] to topic[{}]", StandardCharsets.UTF_8.decode(message).toString(), record.topic());
                } catch (LBMException ex) {
                    ((Buffer)message).rewind(); // cast essential for Java 1.8
                    logger.warn("      - source send failed... could NOT send [{}] to topic[{}]", StandardCharsets.UTF_8.decode(message).toString(), record.topic());
                    ex.printStackTrace();
                }
            }  else {
                logger.warn("      - record.value() not instance of byte[]; found [{}]", record.value().getClass().toString());
            }
        }
        resumePartitions();
    }

    private void resumePartitions() {
        String resumePartitionTopic;
        while ((resumePartitionTopic = resumeQ.poll()) != null) {
            if (um_verbose >=1)
                logger.info("resumePartitions() - resuming partition associated with topic [{}]", resumePartitionTopic);
            for (TopicPartition partition : taskPartitions) {
                if ((partition.topic()).compareTo(resumePartitionTopic) == 0) {
                    try {
                        this.context.resume(partition);
                    } catch (Throwable t) {
                        t.printStackTrace();
                        logger.warn("Throwable [{}]", t.toString());
                    }
                    if (um_verbose >= 1) {
                        logger.info("resumePartitions() - resumed partition [{}] on topic [{}]", partition, resumePartitionTopic);
                    }
                }
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (um_verbose > 0)
            logger.info("flush() - flushing {} output stream", logFilename());
        outputStream.flush();
        if (um_verbose > 1)
            offsets.forEach((k, v) -> logger.info("        - topic[{}] partition[{}] offset[{}]", k.topic(), k.partition(), v.offset()));
    }

    @Override
    public void stop() {
        sources.forEach((topicKey, lbmSource) -> {
            try {
                logger.info("stop() - closing source on topic[{}]", topicKey.toString());
                lbmSource.close();
            } catch (LBMException e) {
                e.printStackTrace();
            }
        });
        if (outputStream != null && outputStream != System.out) {
            logger.info("stop() - closing output stream");
            outputStream.close();
        }
        ctx.close();
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        taskPartitions = partitions;
        taskTopics = new HashSet<String>();
        partitions.forEach(((partition) -> taskTopics.add(partition.topic())));
        if (um_verbose >= 2) {
            taskPartitions.forEach((partition) -> logger.info("open() - partition [{}] number [{}] on topic [{}]", partition.toString(), partition.partition(), partition.topic()));
            taskTopics.forEach((topic) -> logger.info("       - unique topic [{}]", topic));
        }
        if (um_persist) {
            taskPartitions.forEach((partition) -> {
                this.context.pause(partition);
                if (um_verbose >= 2) {
                    logger.info("open() - paused partition [{}]", partition);
                }
            });
        }
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        if (um_verbose >= 2) {
            partitions.forEach((partition) -> logger.info("close() - partition [{}] number [{}] on topic [{}]", partition.toString(), partition.partition(), partition.topic()));
        }
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


class LongObject
{
    public long value = 0;

    public void done() {
    }
}

class UMESrcCB implements LBMSourceEventCallback//, LBMMessageReclamationCallback
{
    public boolean blocked = false;
    private int _verbose;
    private String _srcTopicString;
    private BlockingQueue<String> _resumeQ;
    private int force_reclaim_total = 0;
    private StringBuilder builder = new StringBuilder();

    private static final Logger logger = LoggerFactory.getLogger(UMESrcCB.class);

    public UMESrcCB(int verbose, String srcTopicString, BlockingQueue<String> resumeQ)
    {
        _verbose = verbose;
        _srcTopicString = srcTopicString;
        _resumeQ = resumeQ;
    }

    private void resumePartition(String srcTopicString) {
        if (!_resumeQ.offer(srcTopicString)) {
            logger.info("Warning UMESrcCB::resumePartition() - resume queue is full; could not resume topic [{}]", srcTopicString);
        } else {
            logger.info("UMESrcCB::resumePartition() - added topic [{}] to resume queue", srcTopicString);
        }
    }

    public int onSourceEvent(Object arg, LBMSourceEvent sourceEvent)
    {
        builder.setLength(0);
        switch (sourceEvent.type())
        {
            case LBM.SRC_EVENT_CONNECT:
                logger.info("Receiver connect [{}", sourceEvent.dataString());
                break;
            case LBM.SRC_EVENT_DISCONNECT:
                logger.info("Receiver disconnect [{}]", sourceEvent.dataString());
                break;
            case LBM.SRC_EVENT_WAKEUP:
                blocked = false;
                break;
            case LBM.SRC_EVENT_UME_REGISTRATION_ERROR:
                logger.info("Error registering source with UME store: [{}]", sourceEvent.dataString());
                break;
            case LBM.SRC_EVENT_UME_REGISTRATION_SUCCESS:
                logger.info("UME store registration success. RegID [{}]", sourceEvent.registrationId());
                break;
            case LBM.SRC_EVENT_UME_DEREGISTRATION_SUCCESS_EX:
                logger.info("UME_DEREGISTRATION_SUCCESS_EX ");
                break;
            case LBM.SRC_EVENT_UME_DEREGISTRATION_COMPLETE_EX:
                logger.info("UME_DEREGISTRATION_COMPLETE_EX ");
                break;

            case LBM.SRC_EVENT_UME_REGISTRATION_SUCCESS_EX:
                UMESourceEventRegistrationSuccessInfo reg = sourceEvent.registrationSuccessInfo();
                builder.append(String.format("UME store %d: %s registration success. RegID %d. Flags %d ",
                        reg.storeIndex(), reg.store(), reg.registrationId(), reg.flags()));
                if (((reg.flags() & LBM.SRC_EVENT_UME_REGISTRATION_SUCCESS_EX_FLAG_OLD)) != 0) {
                    builder.append("OLD[SQN " + reg.sequenceNumber() + "] ");
                }
                if (((reg.flags() & LBM.SRC_EVENT_UME_REGISTRATION_SUCCESS_EX_FLAG_NOACKS)) != 0) {
                    builder.append("NOACKS ");
                }
                logger.info("{}\n              UMESourceEventRegistrationSuccessInfo[{}] on topic [{}]",
                        builder.toString(), reg.toString(), _srcTopicString);
            break;
            case LBM.SRC_EVENT_UME_REGISTRATION_COMPLETE_EX:
                UMESourceEventRegistrationCompleteInfo regcomp = sourceEvent.registrationCompleteInfo();
                builder.append("UME registration complete. SQN %d. Flags %d " + regcomp.sequenceNumber() + regcomp.flags());
                if ((regcomp.flags() & LBM.SRC_EVENT_UME_REGISTRATION_COMPLETE_EX_FLAG_QUORUM) != 0) {
                    builder.append("QUORUM ");
                }
                logger.info("{}\n              UMESourceEventRegistrationCompleteInfo[{}] on topic [{}]",
                        builder.toString(), regcomp.toString(), _srcTopicString);
                resumePartition(_srcTopicString);
                break;
            case LBM.SRC_EVENT_UME_MESSAGE_STABLE:
                if (_verbose >= 2)
                    logger.info("UME message stable - sequence number {} (cd {})",
                            Long.toHexString(sourceEvent.sequenceNumber()),
                            Long.toHexString((Long) sourceEvent.clientObject()) );
                UMSinkTask.stablerecv++;
                break;
            case LBM.SRC_EVENT_UME_MESSAGE_NOT_STABLE:
                UMESourceEventAckInfo nstaInfo = sourceEvent.ackInfo();
                if (_verbose >= 2) {
                    builder.append("UME store " + nstaInfo.storeIndex() + ": "
                            + nstaInfo.store()
                            + " message NOT stable!! SQN "
                            + nstaInfo.sequenceNumber()
                            + " (cd " + nstaInfo.clientObject()
                            + "). Flags "
                            + nstaInfo.flags()
                            + " ");
                    if ((nstaInfo.flags() & LBM.SRC_EVENT_UME_MESSAGE_NOT_STABLE_FLAG_LOSS) != 0) {
                        builder.append("LOSS");
                    }
                    else if ((nstaInfo.flags() & LBM.SRC_EVENT_UME_MESSAGE_NOT_STABLE_FLAG_TIMEOUT) != 0) {
                        builder.append("TIMEOUT");
                    }
                    logger.info(builder.toString());
                }
                break;
            case LBM.SRC_EVENT_UME_MESSAGE_STABLE_EX:
                UMESourceEventAckInfo staInfo = sourceEvent.ackInfo();
                if (_verbose >= 2) {
                    builder.append("UME store " + staInfo.storeIndex() + ": "
                            + staInfo.store() + " message stable. SQN " + staInfo.sequenceNumber()
                            + " (cd " + staInfo.clientObject() + "). Flags " + staInfo.flags() + " ");
                    if ((staInfo.flags() & LBM.SRC_EVENT_UME_MESSAGE_STABLE_EX_FLAG_INTRAGROUP_STABLE) != 0) {
                        builder.append("IA ");
                    }
                    if ((staInfo.flags() & LBM.SRC_EVENT_UME_MESSAGE_STABLE_EX_FLAG_INTERGROUP_STABLE) != 0) {
                        builder.append("IR ");
                    }
                    if ((staInfo.flags() & LBM.SRC_EVENT_UME_MESSAGE_STABLE_EX_FLAG_STABLE) != 0) {
                        builder.append("STABLE ");
                    }
                    if ((staInfo.flags() & LBM.SRC_EVENT_UME_MESSAGE_STABLE_EX_FLAG_STORE) != 0) {
                        builder.append("STORE ");
                    }
                    logger.info(builder.toString());
                }

                if((staInfo.flags() & LBM.SRC_EVENT_UME_MESSAGE_STABLE_EX_FLAG_STABLE) == LBM.SRC_EVENT_UME_MESSAGE_STABLE_EX_FLAG_STABLE) {
                    UMSinkTask.stablerecv++;
                }
                break;
            case LBM.SRC_EVENT_UME_DELIVERY_CONFIRMATION:
                if (_verbose > 0)
                    logger.info("UME delivery confirmation - sequence number {} Rcv RegID {} (cd {})",
                            Long.toHexString(sourceEvent.sequenceNumber()),
                            sourceEvent.registrationId(),
                            Long.toHexString((Long) sourceEvent.clientObject()) );
                break;
            case LBM.SRC_EVENT_UME_DELIVERY_CONFIRMATION_EX:
                UMESourceEventAckInfo cdelvinfo = sourceEvent.ackInfo();
                if (_verbose > 0) {
                    builder.append("UME delivery confirmation. SQN " + cdelvinfo.sequenceNumber()
                            + ", RcvRegID "
                            + cdelvinfo.receiverRegistrationId()
                            + " (cd "
                            + cdelvinfo.clientObject()
                            + "). Flags "
                            + cdelvinfo.flags()
                            + " ");
                    if ((cdelvinfo.flags() & LBM.SRC_EVENT_UME_DELIVERY_CONFIRMATION_EX_FLAG_UNIQUEACKS) != 0) {
                        builder.append("UNIQUEACKS ");
                    }
                    if ((cdelvinfo.flags() & LBM.SRC_EVENT_UME_DELIVERY_CONFIRMATION_EX_FLAG_UREGID) != 0) {
                        builder.append("UREGID ");
                    }
                    if ((cdelvinfo.flags() & LBM.SRC_EVENT_UME_DELIVERY_CONFIRMATION_EX_FLAG_OOD) != 0) {
                        builder.append("OOD ");
                    }
                    if ((cdelvinfo.flags() & LBM.SRC_EVENT_UME_DELIVERY_CONFIRMATION_EX_FLAG_EXACK) != 0) {
                        builder.append("EXACK ");
                    }
                    logger.info("{}\n", builder.toString());
                }
                break;
            case LBM.SRC_EVENT_UME_MESSAGE_RECLAIMED:
                if (_verbose > 0)
                    logger.info("UME message reclaimed - sequence number {} (cd {})",
                            Long.toHexString(sourceEvent.sequenceNumber()),
                            Long.toHexString((Long) sourceEvent.clientObject()) );
                break;
            case LBM.SRC_EVENT_UME_MESSAGE_RECLAIMED_EX:
                UMESourceEventAckInfo reclaiminfo = sourceEvent.ackInfo();

                if (_verbose > 0) {
                    if (reclaiminfo.clientObject() != null) {
                        builder.append("UME message reclaimed (ex) - sequence number "
                                + Long.toHexString(reclaiminfo.sequenceNumber())
                                + " (cd "
                                + Long.toHexString((Long) reclaiminfo.clientObject())
                                + "). Flags 0x"
                                + reclaiminfo.flags());
                    } else {
                        builder.append("UME message reclaimed (ex) - sequence number "
                                + Long.toHexString(reclaiminfo.sequenceNumber())
                                + " Flags 0x"
                                + reclaiminfo.flags());
                    }
                    if ((reclaiminfo.flags() & LBM.SRC_EVENT_UME_MESSAGE_RECLAIMED_EX_FLAG_FORCED) != 0) {
                        builder.append(" FORCED");
                    }
                    logger.info(builder.toString());
                }
                break;
            case LBM.SRC_EVENT_UME_STORE_UNRESPONSIVE:
                logger.info("UME store: [{}]", sourceEvent.dataString());
                break;
            case LBM.SRC_EVENT_SEQUENCE_NUMBER_INFO:
                LBMSourceEventSequenceNumberInfo info = sourceEvent.sequenceNumberInfo();
                if (info.firstSequenceNumber() != info.lastSequenceNumber()) {
                    logger.info("SQN [{},{}] (cd {})", info.firstSequenceNumber(), info.lastSequenceNumber(), info.clientObject());
                } else {
                    logger.info("SQN {} (cd {})", info.lastSequenceNumber(), info.clientObject());
                }
                break;
            case LBM.SRC_EVENT_FLIGHT_SIZE_NOTIFICATION:
                if (_verbose > 0) {
                    LBMSourceEventFlightSizeNotification note = sourceEvent.flightSizeNotification();
                    builder.append("Flight Size Notification. Type ");
                    switch (note.type()) {
                        case LBM.SRC_EVENT_FLIGHT_SIZE_NOTIFICATION_TYPE_UME:
                            builder.append("UME");
                            break;
                        case LBM.SRC_EVENT_FLIGHT_SIZE_NOTIFICATION_TYPE_ULB:
                            builder.append("ULB");
                            break;
                        case LBM.SRC_EVENT_FLIGHT_SIZE_NOTIFICATION_TYPE_UMQ:
                            builder.append("UMQ");
                            break;
                        default:
                            builder.append("unknown");
                            break;
                    }
                    builder.append(". Inflight is {} specified flight size"
                            + (note.state() == LBM.SRC_EVENT_FLIGHT_SIZE_NOTIFICATION_STATE_OVER ? "OVER" : "UNDER"));
                    logger.info(builder.toString());
                }
                break;
            default:
                logger.info("Unknown source event [{}]", sourceEvent.type());
                break;
        }
        sourceEvent.dispose();
        System.out.flush();
        return 0;
    }

    public void onMessageReclaim(Object clientd, String topic, long sqn)
    {
        LongObject t = (LongObject)clientd;
        if (t == null) {
            logger.info("WARNING: source for topic [{}] forced reclaim 0x{}", topic, Long.toString(sqn, 16));
        } else {
            long endt = System.currentTimeMillis();
            endt -= t.value;
            force_reclaim_total++;
            if (endt > 5000) {
                logger.info("WARNING: source for topic [{}] forced reclaim. Total {}", topic, force_reclaim_total);
                t.value = System.currentTimeMillis();
            }
        }
    }
}
