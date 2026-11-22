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

import com.github.shyiko.mysql.binlog.event.HeartbeatV2EventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Reads the length-encoded TLV fields of a MySQL heartbeat version 2.
 */
public class HeartbeatV2EventDataDeserializer implements EventDataDeserializer<HeartbeatV2EventData> {

    private static final long END_MARK = 0;
    private static final long BINLOG_FILENAME = 1;
    private static final long BINLOG_POSITION = 2;

    @Override
    public HeartbeatV2EventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        HeartbeatV2EventData eventData = new HeartbeatV2EventData();
        while (inputStream.available() > 0) {
            long fieldType = inputStream.readPackedLong();
            if (fieldType == END_MARK) {
                break;
            }
            long fieldLength = inputStream.readPackedLong();
            if (fieldLength < 0 || fieldLength > inputStream.available()) {
                throw new IOException("Invalid heartbeat field length: " + fieldLength);
            }
            if (fieldType == BINLOG_FILENAME) {
                eventData.setBinlogFilename(inputStream.readString((int) fieldLength));
            } else if (fieldType == BINLOG_POSITION) {
                // Bound the integer to its own TLV field so a malformed value cannot
                // consume the following field or the end marker.
                if (fieldLength < 1 || fieldLength > 9) {
                    throw new IOException("Invalid heartbeat position field length: " + fieldLength);
                }
                ByteArrayInputStream positionStream =
                    new ByteArrayInputStream(inputStream.read((int) fieldLength));
                long position = positionStream.readPackedLong();
                if (position < 0 || positionStream.available() != 0) {
                    throw new IOException("Invalid heartbeat binlog position");
                }
                eventData.setBinlogPosition(position);
            } else if (inputStream.skip(fieldLength) != fieldLength) {
                throw new IOException("Truncated heartbeat field: " + fieldType);
            }
        }
        return eventData;
    }
}
