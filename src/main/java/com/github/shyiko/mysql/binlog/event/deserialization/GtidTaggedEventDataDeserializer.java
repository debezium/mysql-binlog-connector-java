/*
 * Copyright 2013 Patrick Prasse
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

import com.github.shyiko.mysql.binlog.event.GtidTaggedEventData;
import com.github.shyiko.mysql.binlog.event.MySqlGtid;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Deserializer for GTID_TAGGED_LOG_EVENT (MySQL 8.3+).
 *
 * @author <a href="mailto:pprasse@actindo.de">Patrick Prasse</a>
 */
public class GtidTaggedEventDataDeserializer implements EventDataDeserializer<GtidTaggedEventData> {
    private static final int SERIALIZATION_FORMAT_VERSION = 1;

    @Override
    public GtidTaggedEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        final Cursor cursor = new Cursor(inputStream.read(inputStream.available()));
        final long serializationFormatVersion = cursor.readUnsignedVarLong();
        if (serializationFormatVersion != SERIALIZATION_FORMAT_VERSION) {
            throw new IOException("Unexpected GTID_TAGGED serialization format version " + serializationFormatVersion);
        }
        final int eventEnd = cursor.checkedInt(cursor.readUnsignedVarLong(), "GTID_TAGGED event length");
        cursor.readUnsignedVarLong(); // last non-ignorable field id

        cursor.readFieldId(0);
        final byte flags = (byte) cursor.readUnsignedVarLong();

        cursor.readFieldId(1);
        final long sourceIdMostSignificantBits = cursor.readUuidLong();
        final long sourceIdLeastSignificantBits = cursor.readUuidLong();

        cursor.readFieldId(2);
        final long transactionId = cursor.readSignedVarLong();

        cursor.readFieldId(3);
        final String tag = cursor.readString(MySqlGtid.TAG_MAX_LENGTH);

        final MySqlGtid gtid = new MySqlGtid(
                tag.isEmpty() ? null : tag,
                new UUID(sourceIdMostSignificantBits, sourceIdLeastSignificantBits),
                transactionId
        );

        cursor.readFieldId(4);
        final long lastCommitted = cursor.readSignedVarLong();

        cursor.readFieldId(5);
        final long sequenceNumber = cursor.readSignedVarLong();

        cursor.readFieldId(6);
        final long immediateCommitTimestamp = cursor.readUnsignedVarLong();

        final long originalCommitTimestamp;
        if (cursor.nextFieldId(eventEnd) == 7) {
            cursor.readFieldId(7);
            originalCommitTimestamp = cursor.readUnsignedVarLong();
        } else {
            originalCommitTimestamp = immediateCommitTimestamp;
        }

        cursor.readFieldId(8);
        final long transactionLength = cursor.readUnsignedVarLong();

        cursor.readFieldId(9);
        final int immediateServerVersion = cursor.checkedInt(cursor.readUnsignedVarLong(), "immediate server version");

        final int originalServerVersion;
        if (cursor.nextFieldId(eventEnd) == 10) {
            cursor.readFieldId(10);
            originalServerVersion = cursor.checkedInt(cursor.readUnsignedVarLong(), "original server version");
        } else {
            originalServerVersion = immediateServerVersion;
        }

        long commitGroupTicket = 0;
        if (cursor.nextFieldId(eventEnd) == 11) {
            cursor.readFieldId(11);
            commitGroupTicket = cursor.readUnsignedVarLong();
        }

        return new GtidTaggedEventData(gtid, flags, lastCommitted, sequenceNumber,
                immediateCommitTimestamp, originalCommitTimestamp, transactionLength,
                immediateServerVersion, originalServerVersion, commitGroupTicket);
    }

    private static final class Cursor {
        private final byte[] bytes;
        private int position;

        private Cursor(byte[] bytes) {
            this.bytes = bytes;
        }

        private void readFieldId(int expectedFieldId) throws IOException {
            final long fieldId = readUnsignedVarLong();
            if (fieldId != expectedFieldId) {
                throw new IOException("Expected GTID_TAGGED field " + expectedFieldId + " but found " + fieldId);
            }
        }

        private int nextFieldId(int eventEnd) throws IOException {
            if (position >= Math.min(eventEnd, bytes.length)) {
                return -1;
            }
            final int savedPosition = position;
            final long fieldId = readUnsignedVarLong();
            position = savedPosition;
            return checkedInt(fieldId, "field id");
        }

        private long readSignedVarLong() throws IOException {
            final long value = readUnsignedVarLong();
            return (value & 1) == 0 ? value >>> 1 : -(value >>> 1) - 1;
        }

        private long readUnsignedVarLong() throws IOException {
            final int firstByte = readByte();
            int byteCount = 1;
            while (byteCount < 9 && ((firstByte >>> (byteCount - 1)) & 1) == 1) {
                byteCount++;
            }
            if (position + byteCount - 1 > bytes.length) {
                throw new IOException("Unexpected end of GTID_TAGGED variable-length integer");
            }

            long result = (firstByte & 0xffL) >>> byteCount;
            if (byteCount == 1) {
                return result;
            }

            long remaining = 0;
            for (int i = 0; i < byteCount - 1; ++i) {
                remaining |= (long) readByte() << (i << 3);
            }
            final int shift = 8 - byteCount + ((byteCount + 7) >> 4);
            return result | (remaining << shift);
        }

        private long readUuidLong() throws IOException {
            long result = 0;
            for (int i = 0; i < 8; ++i) {
                final long value = readUnsignedVarLong();
                if (value > 0xff) {
                    throw new IOException("GTID_TAGGED UUID byte exceeds byte range: " + value);
                }
                result = (result << 8) | value;
            }
            return result;
        }

        private String readString(int maxLength) throws IOException {
            final int length = checkedInt(readUnsignedVarLong(), "tag length");
            if (length > maxLength) {
                throw new IOException("GTID tag length " + length + " exceeds maximum " + maxLength);
            }
            if (position + length > bytes.length) {
                throw new IOException("Unexpected end of GTID tag");
            }
            final String result = new String(bytes, position, length, StandardCharsets.UTF_8);
            position += length;
            return result;
        }

        private int checkedInt(long value, String name) throws IOException {
            if (value > Integer.MAX_VALUE) {
                throw new IOException("GTID_TAGGED " + name + " exceeds integer range: " + value);
            }
            return (int) value;
        }

        private int readByte() throws IOException {
            if (position >= bytes.length) {
                throw new IOException("Unexpected end of GTID_TAGGED event");
            }
            return bytes[position++] & 0xff;
        }
    }

}