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
package com.github.shyiko.mysql.binlog.network.protocol.command;

import static org.testng.Assert.assertEquals;

import java.io.IOException;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.github.shyiko.mysql.binlog.GtidSet;

public class DumpBinaryLogGtidCommandTest {

    private static final long SERVER_ID = 0x89abcdefL;
    private static final String BINLOG_FILENAME = "binlog.000001";
    private static final long BINLOG_POSITION = 0x1f1234567L;

    @Test
    public void testLegacyConstructorKeepsZeroFlags() throws IOException {
        final var gtidSet = new GtidSet("");
        assertEquals(new DumpBinaryLogGtidCommand(SERVER_ID, BINLOG_FILENAME, BINLOG_POSITION, gtidSet).toByteArray(),
            defaultPacket());
        assertEquals(new DumpBinaryLogGtidCommand(SERVER_ID, BINLOG_FILENAME, BINLOG_POSITION, gtidSet, 0).toByteArray(),
            defaultPacket());
    }

    @DataProvider(name = "dumpFlags")
    public Object[][] dumpFlags() {
        return new Object[][] {
            { DumpBinaryLogCommand.USE_HEARTBEAT_EVENT_V2, new byte[]{0x02, 0} },
            { 0x0102, new byte[]{0x02, 0x01} }
        };
    }

    @Test(dataProvider = "dumpFlags")
    public void testExplicitFlagsImmediatelyFollowTheCommand(final int flags, final byte[] encodedFlags) throws IOException {
        final byte[] expected = defaultPacket();
        expected[1] = encodedFlags[0];
        expected[2] = encodedFlags[1];
        assertEquals(new DumpBinaryLogGtidCommand(SERVER_ID, BINLOG_FILENAME, BINLOG_POSITION, new GtidSet(""), flags)
            .toByteArray(), expected);
    }

    @DataProvider(name = "gtidSets")
    public Object[][] gtidSets() {
        return new Object[][] {
            { "24bc7850-2c16-11e6-a073-0242ac110002:1-3" },
            { "24bc7850-2c16-11e6-a073-0242ac110002:blue:1-3" }
        };
    }

    @Test(dataProvider = "gtidSets")
    public void testHeartbeatFlagDoesNotChangeTheGtidPayload(final String gtids) throws IOException {
        final var gtidSet = new GtidSet(gtids);
        final byte[] expected = new DumpBinaryLogGtidCommand(SERVER_ID, BINLOG_FILENAME, BINLOG_POSITION, gtidSet)
            .toByteArray();
        expected[1] = 0x02;
        assertEquals(new DumpBinaryLogGtidCommand(SERVER_ID, BINLOG_FILENAME, BINLOG_POSITION, gtidSet,
            DumpBinaryLogCommand.USE_HEARTBEAT_EVENT_V2).toByteArray(), expected);
    }

    private static byte[] defaultPacket() {
        return new byte[] {
            0x1e, // COM_BINLOG_DUMP_GTID
            0, 0, // flags
            (byte) 0xef, (byte) 0xcd, (byte) 0xab, (byte) 0x89, // server id
            0x0d, 0, 0, 0, // filename length
            'b', 'i', 'n', 'l', 'o', 'g', '.', '0', '0', '0', '0', '0', '1',
            0x67, 0x45, 0x23, (byte) 0xf1, 0x01, 0, 0, 0, // eight-byte position
            0x08, 0, 0, 0, // encoded empty GTID set length
            0, 0, 0, 0, 0, 0, 0, 0 // UUID set count
        };
    }
}
