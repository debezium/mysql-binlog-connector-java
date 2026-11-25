/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.mysql.binlog;

import static org.testng.Assert.assertEquals;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.github.shyiko.mysql.binlog.network.protocol.PacketChannel;

public class BinaryLogClientDumpCommandTest {

    private static final String BINLOG_FILENAME = "mysql-bin.000001";
    private static final long BINLOG_POSITION = 1234L;
    private static final String MYSQL_GTID_SET = "24bc7850-2c16-11e6-a073-0242ac110002:1-7";

    @DataProvider(name = "mysqlDumpVersions")
    public Object[][] mysqlDumpVersions() {
        return new Object[][]{
            {"8.0.27", false, 0},
            {"8.0.27", true, 0},
            {"8.0.28", false, 2},
            {"8.0.28", true, 2},
            {"8.0.29", false, 2},
            {"8.0.29", true, 2},
            {"8.4.0", false, 2},
            {"8.4.0", true, 2}
        };
    }

    @Test(dataProvider = "mysqlDumpVersions")
    public void testMySqlNegotiatesHeartbeatV2ByServerVersion(String version, boolean gtid, int expectedFlags)
            throws Exception {
        // MariaDB's annotation setting must not select the same bit on an older MySQL server.
        BinaryLogClient client = client(version);
        client.setUseSendAnnotateRowsEvent(true);
        if (gtid) {
            client.setGtidSet(MYSQL_GTID_SET);
        }
        List<byte[]> commands = requestCommands(client, 0);

        assertEquals(commands.size(), 1);
        byte[] dump = commands.get(0);
        assertEquals(dump[0] & 0xff, gtid ? 0x1e : 0x12);
        assertFlags(dump, gtid ? 1 : 5, expectedFlags);
    }

    @DataProvider(name = "mariaDbFilePositionStarts")
    public Object[][] mariaDbFilePositionStarts() {
        return new Object[][]{
            {null, false, null},   // GTID disabled
            {"", false, null},     // GTID enabled before setupGtidSet()
            {"", true, ""}         // setupGtidSet() initialized an empty MariaDB GTID set
        };
    }

    @Test(dataProvider = "mariaDbFilePositionStarts")
    public void testMariaDbFallsBackToFilePosition(String configuredGtids, boolean initialize, String expectedGtids)
            throws Exception {
        BinaryLogClient client = client("12.3.3-MariaDB-log");
        client.setGtidSet(configuredGtids);
        if (initialize) {
            client.setupGtidSet();
        }
        assertEquals(client.getGtidSet(), expectedGtids);
        assertMariaDbDump(client, null);
    }

    @Test
    public void testMariaDbSendsPopulatedGtidSet() throws Exception {
        BinaryLogClient client = client("12.3.3-MariaDB-log");
        client.setGtidSet("0-1-7");
        client.setupGtidSet();
        assertEquals(client.getGtidSet(), "0-1-7");
        assertMariaDbDump(client, "0-1-7");
    }

    private static void assertMariaDbDump(BinaryLogClient client, String expectedConnectState) throws Exception {
        boolean useGtid = expectedConnectState != null;
        for (boolean annotated : List.of(false, true)) {
            client.setUseSendAnnotateRowsEvent(annotated);
            List<byte[]> commands = requestCommands(client, useGtid ? 4 : 1);
            assertEquals(commands.size(), useGtid ? 5 : 2);
            assertQuery(commands.get(0), "SET @mariadb_slave_capability=4");
            if (useGtid) {
                assertQuery(commands.get(1), "SET @slave_connect_state = '" + expectedConnectState + "'");
                assertQuery(commands.get(2), "SET @slave_gtid_strict_mode = 0");
                assertQuery(commands.get(3), "SET @slave_gtid_ignore_duplicates = 0");
            }
            ByteArrayInputStream dump = new ByteArrayInputStream(commands.get(commands.size() - 1));
            assertEquals(dump.read(), 0x12, "MariaDB uses COM_BINLOG_DUMP in both modes");
            assertEquals(dump.readLong(4), useGtid ? 0L : BINLOG_POSITION);
            assertEquals(dump.readInteger(2), annotated ? 2 : 0);
            assertEquals(dump.readLong(4), client.getServerId());
            assertEquals(dump.readString(dump.available()), useGtid ? "" : BINLOG_FILENAME);
        }
    }

    private static BinaryLogClient client(String serverVersion) throws ReflectiveOperationException {
        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "");
        client.setBinlogFilename(BINLOG_FILENAME);
        client.setBinlogPosition(BINLOG_POSITION);
        Field databaseVersion = BinaryLogClient.class.getDeclaredField("databaseVersion");
        databaseVersion.setAccessible(true);
        databaseVersion.set(client, BinaryLogDatabaseVersion.parse(serverVersion));
        return client;
    }

    private static List<byte[]> requestCommands(BinaryLogClient client, int queryCount) throws Exception {
        byte[] responses = okResponses(queryCount);
        java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
        try (PacketChannel channel = new PacketChannel(new Socket() {
            @Override
            public InputStream getInputStream() {
                return new java.io.ByteArrayInputStream(responses);
            }

            @Override
            public OutputStream getOutputStream() {
                return output;
            }
        })) {
            client.channel = channel;
            channel.authenticationComplete();
            client.requestBinaryLogStream();
        }

        List<byte[]> commands = new ArrayList<>();
        ByteArrayInputStream packets = new ByteArrayInputStream(output.toByteArray());
        while (packets.available() > 0) {
            int length = packets.readInteger(3);
            assertEquals(packets.read(), 0, "Each command starts a new packet sequence");
            commands.add(packets.read(length));
        }
        return commands;
    }

    private static byte[] okResponses(int count) {
        java.io.ByteArrayOutputStream responses = new java.io.ByteArrayOutputStream();
        for (int i = 0; i < count; i++) {
            // A command is sequence 0; its OK response is sequence 1.
            responses.writeBytes(new byte[]{7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0});
        }
        return responses.toByteArray();
    }

    private static void assertQuery(byte[] command, String query) {
        assertEquals(command[0] & 0xff, 0x03);
        assertEquals(new String(command, 1, command.length - 1, StandardCharsets.UTF_8), query);
    }

    private static void assertFlags(byte[] command, int offset, int expected) {
        assertEquals((command[offset] & 0xff) | ((command[offset + 1] & 0xff) << 8), expected);
    }
}
