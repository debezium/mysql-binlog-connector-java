/*
 * Copyright 2026 Debezium Authors
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
package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.HeartbeatV2EventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.github.shyiko.mysql.binlog.io.ByteArrayOutputStream;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

public class HeartbeatV2EventDataDeserializerTest {

    // MySQL's Heartbeat::encode writes TLV filename, length-encoded position, then END0:
    // https://github.com/mysql/mysql-server/blob/8a180268d832300c09a375115dfa0ea77d83f1af/libs/mysql/binlog/event/codecs/binary.cpp
    // Filename is "binlog.000001" and the 64-bit position is 2^32 + 123.
    private static final byte[] DATA = {
        1, 13, 'b', 'i', 'n', 'l', 'o', 'g', '.', '0', '0', '0', '0', '0', '1',
        2, 9, (byte) 0xfe, 123, 0, 0, 0, 1, 0, 0, 0,
        0
    };

    @Test
    public void deserializeFullPosition() throws IOException {
        HeartbeatV2EventData data = deserialize(DATA);
        assertEquals(data.getBinlogFilename(), "binlog.000001");
        assertEquals(data.getBinlogPosition(), (1L << 32) + 123);
    }

    @DataProvider(name = "encodedPositions")
    public Object[][] encodedPositions() {
        return new Object[][] {
            {new byte[] {0}, 0L},
            {new byte[] {(byte) 250}, 250L},
            {new byte[] {(byte) 0xfc, (byte) 251, 0}, 251L},
            {new byte[] {(byte) 0xfc, -1, -1}, 65535L},
            {new byte[] {(byte) 0xfd, 0, 0, 1}, 65536L},
            {new byte[] {(byte) 0xfd, -1, -1, -1}, 0xffffffL},
            {new byte[] {(byte) 0xfe, 0, 0, 0, 1, 0, 0, 0, 0}, 0x1000000L},
            {new byte[] {(byte) 0xfe, -1, -1, -1, -1, -1, -1, -1, 0x7f}, Long.MAX_VALUE}
        };
    }

    @Test(dataProvider = "encodedPositions")
    public void deserializeLengthEncodedPosition(byte[] encodedPosition, long position) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(2);
        outputStream.write(encodedPosition.length);
        outputStream.write(encodedPosition);
        outputStream.write(0);
        assertEquals(deserialize(outputStream.toByteArray()).getBinlogPosition(), position);
    }

    @Test
    public void deserializeUsingDefaultRegistration() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.writeLong(0, 4);
        outputStream.writeInteger(41, 1); // HEARTBEAT_V2 wire event number.
        outputStream.writeLong(1, 4);
        outputStream.writeLong(19 + DATA.length, 4);
        outputStream.writeLong(123, 4);
        outputStream.writeInteger(0, 2);
        outputStream.write(DATA);

        Event event = new EventDeserializer().nextEvent(new ByteArrayInputStream(outputStream.toByteArray()));
        assertEquals(event.getHeader().getEventType(), EventType.HEARTBEAT_V2);
        HeartbeatV2EventData data = event.getData();
        assertEquals(data.getBinlogFilename(), "binlog.000001");
        assertEquals(data.getBinlogPosition(), (1L << 32) + 123);
    }

    @Test
    public void skipUnknownFields() throws IOException {
        HeartbeatV2EventData data = deserialize(new byte[] {
            (byte) 0xfc, 0, 1, 3, 1, 2, 3, // Unknown field type 256, containing three bytes.
            2, 1, 42, 0
        });
        assertEquals(data.getBinlogPosition(), 42);
    }

    @Test
    public void deserializeFieldsInAnyOrder() throws IOException {
        HeartbeatV2EventData data = deserialize(new byte[] {2, 1, 42, 1, 1, 'a', 0});
        assertEquals(data.getBinlogFilename(), "a");
        assertEquals(data.getBinlogPosition(), 42);
    }

    @Test
    public void allowMissingPositionForHeaderFallback() throws IOException {
        HeartbeatV2EventData data = deserialize(new byte[] {1, 1, 'a', 0});
        assertEquals(data.getBinlogPosition(), 0);
    }

    @DataProvider(name = "malformedFields")
    public Object[][] malformedFields() {
        return new Object[][] {
            {new byte[] {(byte) 0xfc}}, // Truncated field type.
            {new byte[] {1, (byte) 0xfc}}, // Truncated field length.
            {new byte[] {1, 2, 'a'}}, // Truncated filename.
            {new byte[] {3, 2, 'a'}}, // Truncated unknown field.
            {new byte[] {2, 0, 0}}, // Empty position.
            {new byte[] {2, 1, (byte) 0xfc, 0}}, // Integer cannot consume the next field.
            {new byte[] {2, 2, 42, 0, 0}}, // Integer shorter than its declared field.
            {new byte[] {2, 1, (byte) 0xfb, 0}}, // NULL position.
            {new byte[] {2, 1, (byte) 0xff, 0}}, // Invalid length-encoded integer marker.
            {new byte[] {1, (byte) 0xfe, 0, 0, 0, 0, 1, 0, 0, 0}}, // Length does not fit in an int.
            {new byte[] {3, (byte) 0xfe, -1, -1, -1, -1, -1, -1, -1, -1}}, // Unsigned length overflow.
            {new byte[] {2, 9, (byte) 0xfe, 0, 0, 0, 0, 0, 0, 0, (byte) 0x80, 0}}
        };
    }

    @Test(dataProvider = "malformedFields", expectedExceptions = IOException.class)
    public void rejectMalformedFields(byte[] bytes) throws IOException {
        deserialize(bytes);
    }

    private HeartbeatV2EventData deserialize(byte[] bytes) throws IOException {
        return new HeartbeatV2EventDataDeserializer().deserialize(new ByteArrayInputStream(bytes));
    }
}
