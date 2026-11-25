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
import static org.testng.Assert.expectThrows;

import java.io.IOException;
import java.util.Arrays;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DumpBinaryLogCommandTest {

    private static final long SERVER_ID = 0x89abcdefL;
    private static final String BINLOG_FILENAME = "binlog.000001";
    private static final long BINLOG_POSITION = 0xf1234567L;

    @Test
    public void testLegacyConstructorsKeepZeroFlags() throws IOException {
        assertEquals(new DumpBinaryLogCommand(SERVER_ID, BINLOG_FILENAME, BINLOG_POSITION).toByteArray(),
            defaultPacket());
        assertEquals(new DumpBinaryLogCommand(SERVER_ID, BINLOG_FILENAME, BINLOG_POSITION, false).toByteArray(),
            defaultPacket());
        assertEquals(new DumpBinaryLogCommand(SERVER_ID, BINLOG_FILENAME, BINLOG_POSITION, 0).toByteArray(),
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
    public void testExplicitFlagsFollowTheFourBytePosition(final int flags, final byte[] encodedFlags) throws IOException {
        final byte[] expected = defaultPacket();
        expected[5] = encodedFlags[0];
        expected[6] = encodedFlags[1];
        assertEquals(new DumpBinaryLogCommand(SERVER_ID, BINLOG_FILENAME, BINLOG_POSITION, flags).toByteArray(),
            expected);
    }

    @Test
    public void testMariaDbAnnotationFlagKeepsItsExistingMeaning() throws IOException {
        final byte[] expected = defaultPacket();
        expected[5] = 0x02;
        assertEquals(new DumpBinaryLogCommand(SERVER_ID, BINLOG_FILENAME, BINLOG_POSITION, true).toByteArray(),
            expected);
    }

    @Test
    public void testLargestFilePositionFitsLegacyDump() throws IOException {
        byte[] expected = defaultPacket();
        Arrays.fill(expected, 1, 5, (byte) 0xff);
        assertEquals(new DumpBinaryLogCommand(SERVER_ID, BINLOG_FILENAME, 0xffffffffL).toByteArray(), expected);
    }

    @DataProvider
    public Object[][] unrepresentablePositions() {
        return new Object[][] {{-1L}, {1L << 32}, {(1L << 32) + 123}};
    }

    @Test(dataProvider = "unrepresentablePositions")
    public void testLegacyDumpRejectsPositionsOutsideUnsignedFourByteRange(long position) {
        BinlogPositionOutOfRangeException error = expectThrows(BinlogPositionOutOfRangeException.class,
            () -> new DumpBinaryLogCommand(SERVER_ID, BINLOG_FILENAME, position).toByteArray());
        assertEquals(error.getMessage(), "COM_BINLOG_DUMP cannot encode binlog position " + position
            + " in its unsigned 4-byte position field");
    }

    private static byte[] defaultPacket() {
        return new byte[] {
            0x12, // COM_BINLOG_DUMP
            0x67, 0x45, 0x23, (byte) 0xf1, // four-byte position
            0, 0, // flags
            (byte) 0xef, (byte) 0xcd, (byte) 0xab, (byte) 0x89, // server id
            'b', 'i', 'n', 'l', 'o', 'g', '.', '0', '0', '0', '0', '0', '1'
        };
    }
}
