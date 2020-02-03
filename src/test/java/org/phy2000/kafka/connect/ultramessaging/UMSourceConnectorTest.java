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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class UMSourceConnectorTest extends EasyMockSupport {

    private static final String SINGLE_TOPIC = "test";
    private static final String MULTIPLE_TOPICS = "test1,test2";
    private static final String FILENAME = "/somefilename";

    private UMSourceConnector connector;
    private ConnectorContext ctx;
    private Map<String, String> sourceProperties;

    @Before
    public void setup() {
        connector = new UMSourceConnector();
        ctx = createMock(ConnectorContext.class);
        connector.initialize(ctx);

        sourceProperties = new HashMap<>();
        sourceProperties.put(UMSourceConnector.KAFKA_TOPIC, SINGLE_TOPIC);
        sourceProperties.put(UMSourceConnector.UM_CONFIG_FILE, FILENAME);
    }

    @Test
    public void testConnectorConfigValidation() {
        replayAll();
        List<ConfigValue> configValues = connector.config().validate(sourceProperties);
        for (ConfigValue val : configValues) {
            assertEquals("Config property errors: " + val.errorMessages(), 0, val.errorMessages().size());
        }
        verifyAll();
    }

    @Test
    public void testSourceTasks() {
        replayAll();

        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size());
        assertEquals(FILENAME,
                taskConfigs.get(0).get(UMSourceConnector.UM_CONFIG_FILE));
        assertEquals(SINGLE_TOPIC,
                taskConfigs.get(0).get(UMSourceConnector.KAFKA_TOPIC));

        // Should be able to return fewer than requested #
        taskConfigs = connector.taskConfigs(2);
        assertEquals(1, taskConfigs.size());
        assertEquals(FILENAME,
                taskConfigs.get(0).get(UMSourceConnector.UM_CONFIG_FILE));
        assertEquals(SINGLE_TOPIC,
                taskConfigs.get(0).get(UMSourceConnector.KAFKA_TOPIC));

        verifyAll();
    }

    @Test
    public void testSourceTasksStdin() {
        EasyMock.replay(ctx);

        sourceProperties.remove(UMSourceConnector.UM_CONFIG_FILE);
        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size());
        assertNull(taskConfigs.get(0).get(UMSourceConnector.UM_CONFIG_FILE));

        EasyMock.verify(ctx);
    }

    @Test(expected = ConfigException.class)
    public void testMultipleSourcesInvalid() {
        sourceProperties.put(UMSourceConnector.KAFKA_TOPIC, MULTIPLE_TOPICS);
        connector.start(sourceProperties);
    }

    @Test
    public void testTaskClass() {
        EasyMock.replay(ctx);

        connector.start(sourceProperties);
        assertEquals(UMSourceTask.class, connector.taskClass());

        EasyMock.verify(ctx);
    }

    @Test(expected = ConfigException.class)
    public void testMissingTopic() {
        sourceProperties.remove(UMSourceConnector.KAFKA_TOPIC);
        connector.start(sourceProperties);
    }

    @Test(expected = ConfigException.class)
    public void testBlankTopic() {
        // Because of trimming this tests is same as testing for empty string.
        sourceProperties.put(UMSourceConnector.KAFKA_TOPIC, "     ");
        connector.start(sourceProperties);
    }

    @Test(expected = ConfigException.class)
    public void testInvalidBatchSize() {
        sourceProperties.put(UMSourceConnector.TASK_BATCH_SIZE_CONFIG, "abcd");
        connector.start(sourceProperties);
    }
}
