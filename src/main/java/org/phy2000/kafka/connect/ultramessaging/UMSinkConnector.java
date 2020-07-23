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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Very simple connector that works with the console. This connector supports both source and
 * sink modes via its 'mode' setting.
 */
public class UMSinkConnector extends SinkConnector {
    public static final String KAFKA_TOPICS = "topics";
    public static final String UM_VERBOSE = "um.verbose";
    public static final String UM_PERSIST = "um.persist";
    public static final String UM_WILDCARD = "um.wildcard";
    public static final String UM_CONFIG_FILE = "um.config.file";
    public static final String UM_LICENSE_FILE = "um.license.file";
    public static final String UM_TOPIC = "um.topic";
    public static final String FILE_CONFIG = "file";

    public static final String DEFAULT_UM_VERBOSE = "0";
    public static final String DEFAULT_UM_PERSIST = "0";
    public static final String DEFAULT_UM_WILDCARD = "0";
    public static final String DEFAULT_UM_CONFIG_FILE = "";
    public static final String DEFAULT_UM_LICENSE_FILE = "";
    public static final String DEFAULT_UM_TOPIC = "";
    public static final String DEFAULT_FILE_DOT_OUT = "file.out";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(KAFKA_TOPICS, Type.LIST, Importance.HIGH,"List of Kafka topics")
        .define(UM_VERBOSE, Type.INT, DEFAULT_UM_VERBOSE, Importance.HIGH,"Log file verbosity")
        .define(UM_PERSIST, Type.INT, DEFAULT_UM_PERSIST, Importance.HIGH,"Persistent receiver")
        .define(UM_WILDCARD, Type.INT, DEFAULT_UM_WILDCARD, Importance.HIGH,"Wildcard receiver")
        .define(UM_CONFIG_FILE, Type.STRING, DEFAULT_UM_CONFIG_FILE, Importance.HIGH,"Full path to the UM configuration file")
        .define(UM_LICENSE_FILE, Type.STRING, DEFAULT_UM_LICENSE_FILE, Importance.HIGH,"Full path to the UM License file")
        .define(UM_TOPIC, Type.STRING, DEFAULT_UM_TOPIC, Importance.HIGH,"UM source Topic prefix")
        .define(FILE_CONFIG, Type.STRING, DEFAULT_FILE_DOT_OUT, Importance.HIGH, "Destination filename. If not specified, the standard output will be used");

    private final UMSinkConnector.um_kafka_config um_config = new um_kafka_config();

    static class um_kafka_config {
        int um_verbose;
        int um_persist;
        int um_wildcard;
        String um_config_file;
        String um_license_file;
        String um_topic;
        String filename;
        List<String> kafka_topics;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
        um_config.kafka_topics = parsedConfig.getList(KAFKA_TOPICS);
        um_config.um_verbose = parsedConfig.getInt(UM_VERBOSE);
        um_config.um_persist = parsedConfig.getInt(UM_PERSIST);
        um_config.um_wildcard = parsedConfig.getInt(UM_WILDCARD);
        um_config.um_config_file = parsedConfig.getString(UM_CONFIG_FILE);
        um_config.um_license_file = parsedConfig.getString(UM_LICENSE_FILE);
        um_config.um_topic = parsedConfig.getString(UM_TOPIC);
        um_config.filename = parsedConfig.getString(FILE_CONFIG);
        if (um_config.um_verbose > 1) {
            um_config.kafka_topics.forEach((t) -> System.out.println("UMSinkConnector::start() - configured Kafka topic [" + t + "]"));
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return UMSinkTask.class;
    }

    // creates a configuration for each task
    // each task is a separate thread assigned to one or more partitions
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            config.put(KAFKA_TOPICS, String.valueOf((um_config.kafka_topics)));
            config.put(UM_VERBOSE, String.valueOf(um_config.um_verbose));
            config.put(UM_PERSIST, String.valueOf(um_config.um_persist));
            config.put(UM_WILDCARD, String.valueOf(um_config.um_wildcard));
            config.put(UM_CONFIG_FILE, um_config.um_config_file);
            config.put(UM_LICENSE_FILE, um_config.um_license_file);
            config.put(UM_TOPIC, um_config.um_topic);
            if (um_config.filename != null)
                config.put(FILE_CONFIG, um_config.filename);
            taskConfigs.add(config);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        // Nothing to do since UMSinkConnector has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
