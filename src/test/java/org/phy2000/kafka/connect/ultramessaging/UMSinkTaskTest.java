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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class UMSinkTaskTest {

    private UMSinkTask task;
    private ByteArrayOutputStream os;
    private PrintStream printStream;

    @Rule
    public TemporaryFolder topDir = new TemporaryFolder();
    private String outputFile;

    @Before
    public void setup() throws Exception {
        os = new ByteArrayOutputStream();
        printStream = new PrintStream(os);
        task = new UMSinkTask(printStream);
        File outputDir = topDir.newFolder("file-stream-sink-" + UUID.randomUUID().toString());
        outputFile = outputDir.getCanonicalPath() + "/connect.output";
    }

    @Test
    public void testPutFlush() {
        HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        final String newLine = System.getProperty("line.separator"); 

        // We do not call task.start() since it would override the output stream

        task.put(Arrays.asList(
                new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, "line1", 1)
        ));
        offsets.put(new TopicPartition("topic1", 0), new OffsetAndMetadata(1L));
        task.flush(offsets);
        assertEquals("line1" + newLine, os.toString());

        task.put(Arrays.asList(
                new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, "line2", 2),
                new SinkRecord("topic2", 0, null, null, Schema.STRING_SCHEMA, "line3", 1)
        ));
        offsets.put(new TopicPartition("topic1", 0), new OffsetAndMetadata(2L));
        offsets.put(new TopicPartition("topic2", 0), new OffsetAndMetadata(1L));
        task.flush(offsets);
        assertEquals("line1" + newLine + "line2" + newLine + "line3" + newLine, os.toString());
    }

    @Test
    public void testStart() throws IOException {
        task = new UMSinkTask();
        Map<String, String> props = new HashMap<>();
        props.put(UMSinkConnector.FILE_CONFIG, outputFile);
        task.start(props);

        HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        task.put(Arrays.asList(
                new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, "line0", 1)
        ));
        offsets.put(new TopicPartition("topic1", 0), new OffsetAndMetadata(1L));
        task.flush(offsets);

        int numLines = 3;
        String[] lines = new String[numLines];
        int i = 0;
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(outputFile))) {
            lines[i++] = reader.readLine();
            task.put(Arrays.asList(
                    new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, "line1", 2),
                    new SinkRecord("topic2", 0, null, null, Schema.STRING_SCHEMA, "line2", 1)
            ));
            offsets.put(new TopicPartition("topic1", 0), new OffsetAndMetadata(2L));
            offsets.put(new TopicPartition("topic2", 0), new OffsetAndMetadata(1L));
            task.flush(offsets);
            lines[i++] = reader.readLine();
            lines[i++] = reader.readLine();
        }

        while (--i >= 0) {
            assertEquals("line" + i, lines[i]);
        }
    }
}
