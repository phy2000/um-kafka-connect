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
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.*;

/**
 * Very simple connector that works with the console. This connector supports both source and
 * sink modes via its 'mode' setting.
 */
public class UMSourceConnector extends SourceConnector {
    public static final String UM_CONFIG_FILE = "um.um_config.file";
    public static final String UM_LICENSE_FILE = "um.license.file";
    public static final String UM_LICENSE_STRING = "um.license.string";
    public static final String UM_TOPIC = "um.topic";
    public static final String KAFKA_TOPIC = "kafka.topic";
    public static final String TASK_BATCH_SIZE_CONFIG = "batch.size";

    public static final String DEFAULT_UM_CONFIG_FILE = "um.license.file";
    public static final String DEFAULT_UM_LICENSE_FILE = "um.license.file";
    public static final String DEFAULT_UM_LICENSE_STRING = "Product=LBM,UME,UMQ,UMDRO:Organization=UM (RnD):Expiration-Date=never:License-Key=FCB4 C6FE 6763 5EC9";
    public static final String DEFAULT_UM_TOPIC = "UM topic 1";
    public static final String DEFAULT_KAFKA_TOPIC = "Kafka topic 1";
    public static final int DEFAULT_TASK_BATCH_SIZE = 2020;

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(UM_CONFIG_FILE, Type.STRING, DEFAULT_UM_CONFIG_FILE, Importance.HIGH,"Full path to the UM configuration file")
            .define(UM_LICENSE_FILE, Type.STRING, DEFAULT_UM_LICENSE_FILE, Importance.HIGH,"Full path to the UM License file")
            .define(UM_LICENSE_STRING, Type.STRING, DEFAULT_UM_LICENSE_STRING, Importance.HIGH,"License string - if set, used instead of license file")
            .define(UM_TOPIC, Type.STRING, DEFAULT_UM_TOPIC, Importance.HIGH,"UM Topic name to subscribe to")
            .define(KAFKA_TOPIC, Type.STRING, DEFAULT_KAFKA_TOPIC, Importance.HIGH,"The kafka topic to publish data to")
            .define(TASK_BATCH_SIZE_CONFIG, Type.INT, DEFAULT_TASK_BATCH_SIZE, Importance.LOW,"The maximum number of records the Source task can read from UM topic");

    private um_kafka_config um_config = new um_kafka_config();

    class um_kafka_config {
        String um_config_file;
        String um_license_file;
        String um_license_string;
        String um_topic;
        String kafka_topic;
        int task_batch_size;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
        String errMsg = "";

        // TODO - need logic to map multiple topics->topics
        um_config.um_config_file = parsedConfig.getString(UM_CONFIG_FILE);
        um_config.um_license_file = parsedConfig.getString(UM_LICENSE_FILE);
        um_config.um_license_string = parsedConfig.getString(UM_LICENSE_STRING);
        um_config.um_topic = parsedConfig.getString(UM_TOPIC);
        um_config.kafka_topic = parsedConfig.getString(KAFKA_TOPIC);
        um_config.task_batch_size = parsedConfig.getInt(TASK_BATCH_SIZE_CONFIG);

        if (um_config.um_config_file == null) {
            errMsg += String.format("%s must be set\n", UM_CONFIG_FILE);
        }
        if (um_config.um_license_file == null && um_config.um_license_string == null) {
            errMsg += String.format("one of %s or %s must be set\n", UM_LICENSE_FILE, UM_LICENSE_STRING);
        }
        if (errMsg.length() >= 1) {
            System.out.println("BAM!");
            throw new ConfigException(errMsg);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return UMSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();

        config.put(UM_CONFIG_FILE, um_config.um_config_file);
        System.out.println("dude... um_config_file [" + config.get(UM_CONFIG_FILE) + "]");
        config.put(UM_LICENSE_FILE, um_config.um_license_file);
        config.put(UM_LICENSE_STRING, um_config.um_license_string);
        config.put(UM_TOPIC, um_config.um_topic);
        config.put(KAFKA_TOPIC, um_config.kafka_topic);
        config.put(TASK_BATCH_SIZE_CONFIG, String.valueOf(um_config.task_batch_size));
        taskConfigs.add(config);

        return taskConfigs;
    }

    @Override
    public void stop() {
        // Nothing to do since UMSourceConnector has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
