package com.github.shyiko.mysql.binlog.event.deserialization;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.github.shyiko.mysql.binlog.event.GtidTaggedEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.testng.annotations.Test;

public class GtidTaggedEventDataDeserializerTest {

    private final GtidTaggedEventDataDeserializer deserializer = new GtidTaggedEventDataDeserializer();

    @Test
    public void testDeserializeWithTag() throws IOException {
        final GtidTaggedEventData data = deserialize(eventBuilder()
                .flags(1)
                .uuid("aae57b2f-8e44-11ee-a3d6-a036bcda1a41")
                .gno(4)
                .tag("test_tag")
                .lastCommitted(3)
                .sequenceNumber(4)
                .immediateCommitTimestamp(1701215692713879L)
                .transactionLength(0)
                .immediateServerVersion(999999)
                .build());

        assertEquals(data.getFlags(), 0x01);
        assertEquals(data.getMySqlGtid().toString(), "aae57b2f-8e44-11ee-a3d6-a036bcda1a41:test_tag:4");
        assertEquals(data.getMySqlGtid().getTag(), "test_tag");
        assertEquals(data.getLastCommitted(), 3);
        assertEquals(data.getSequenceNumber(), 4);
        assertEquals(data.getImmediateCommitTimestamp(), 1701215692713879L);
        assertEquals(data.getOriginalCommitTimestamp(), 1701215692713879L);
        assertEquals(data.getTransactionLength(), 0);
        assertEquals(data.getImmediateServerVersion(), 999999);
        assertEquals(data.getOriginalServerVersion(), 999999);
        assertEquals(data.getCommitGroupTicket(), 0);
    }

    @Test
    public void testDeserializeWithoutTag() throws IOException {
        final GtidTaggedEventData data = deserialize(eventBuilder()
                .flags(0)
                .uuid("994ab859-8ea8-11ee-a568-a036bcda1a41")
                .gno(3)
                .tag("")
                .lastCommitted(2)
                .sequenceNumber(3)
                .immediateCommitTimestamp(1701257014433088L)
                .transactionLength(308)
                .immediateServerVersion(999999)
                .build());

        assertEquals(data.getFlags(), 0x00);
        assertEquals(data.getMySqlGtid().toString(), "994ab859-8ea8-11ee-a568-a036bcda1a41:3");
        assertNull(data.getMySqlGtid().getTag());
        assertEquals(data.getLastCommitted(), 2);
        assertEquals(data.getSequenceNumber(), 3);
        assertEquals(data.getImmediateCommitTimestamp(), 1701257014433088L);
        assertEquals(data.getOriginalCommitTimestamp(), 1701257014433088L);
        assertEquals(data.getTransactionLength(), 308);
        assertEquals(data.getImmediateServerVersion(), 999999);
        assertEquals(data.getOriginalServerVersion(), 999999);
        assertEquals(data.getCommitGroupTicket(), 0);
    }

    @Test
    public void testDeserializeWithFullFields() throws IOException {
        final GtidTaggedEventData data = deserialize(eventBuilder()
                .flags(0)
                .uuid("bd9794e0-1d65-11ed-a7e7-0adb305b3a12")
                .gno(9)
                .tag("prod")
                .lastCommitted(7)
                .sequenceNumber(8)
                .immediateCommitTimestamp(1699112309893478L)
                .transactionLength(315)
                .immediateServerVersion(80100)
                .commitGroupTicket(5)
                .build());

        assertEquals(data.getFlags(), 0x00);
        assertEquals(data.getMySqlGtid().toString(), "bd9794e0-1d65-11ed-a7e7-0adb305b3a12:prod:9");
        assertEquals(data.getMySqlGtid().getTag(), "prod");
        assertEquals(data.getLastCommitted(), 7);
        assertEquals(data.getSequenceNumber(), 8);
        assertEquals(data.getImmediateCommitTimestamp(), 1699112309893478L);
        assertEquals(data.getOriginalCommitTimestamp(), 1699112309893478L);
        assertEquals(data.getTransactionLength(), 315);
        assertEquals(data.getImmediateServerVersion(), 80100);
        assertEquals(data.getOriginalServerVersion(), 80100);
        assertEquals(data.getCommitGroupTicket(), 5);
    }

    @Test
    public void testDeserializeWithDifferentOriginalTimestamp() throws IOException {
        final GtidTaggedEventData data = deserialize(eventBuilder()
                .flags(1)
                .uuid("aae57b2f-8e44-11ee-a3d6-a036bcda1a41")
                .gno(4)
                .tag("replica")
                .lastCommitted(3)
                .sequenceNumber(4)
                .immediateCommitTimestamp(1701215692713879L)
                .originalCommitTimestamp(1701215692713878L)
                .transactionLength(0)
                .immediateServerVersion(999999)
                .build());

        assertEquals(data.getFlags(), 0x01);
        assertEquals(data.getMySqlGtid().toString(), "aae57b2f-8e44-11ee-a3d6-a036bcda1a41:replica:4");
        assertEquals(data.getMySqlGtid().getTag(), "replica");
        assertEquals(data.getLastCommitted(), 3);
        assertEquals(data.getSequenceNumber(), 4);
        assertEquals(data.getImmediateCommitTimestamp(), 1701215692713879L);
        assertEquals(data.getOriginalCommitTimestamp(), 1701215692713878L);
    }

    @Test
    public void testDeserializeWithDifferentOriginalServerVersion() throws IOException {
        final GtidTaggedEventData data = deserialize(eventBuilder()
                .flags(0)
                .uuid("bd9794e0-1d65-11ed-a7e7-0adb305b3a12")
                .gno(9)
                .tag("staging")
                .lastCommitted(7)
                .sequenceNumber(8)
                .immediateCommitTimestamp(1699112309893478L)
                .transactionLength(315)
                .immediateServerVersion(80100)
                .originalServerVersion(80099)
                .build());

        assertEquals(data.getFlags(), 0x00);
        assertEquals(data.getMySqlGtid().toString(), "bd9794e0-1d65-11ed-a7e7-0adb305b3a12:staging:9");
        assertEquals(data.getMySqlGtid().getTag(), "staging");
        assertEquals(data.getLastCommitted(), 7);
        assertEquals(data.getSequenceNumber(), 8);
        assertEquals(data.getImmediateCommitTimestamp(), 1699112309893478L);
        assertEquals(data.getOriginalCommitTimestamp(), 1699112309893478L);
        assertEquals(data.getTransactionLength(), 315);
        assertEquals(data.getImmediateServerVersion(), 80100);
        assertEquals(data.getOriginalServerVersion(), 80099);
    }

    private GtidTaggedEventData deserialize(byte[] bytes) throws IOException {
        return deserializer.deserialize(new ByteArrayInputStream(bytes));
    }

    private static EventBuilder eventBuilder() {
        return new EventBuilder();
    }

    private static final class EventBuilder {
        private int flags;
        private UUID uuid;
        private long gno;
        private String tag = "";
        private long lastCommitted;
        private long sequenceNumber;
        private long immediateCommitTimestamp;
        private Long originalCommitTimestamp;
        private long transactionLength;
        private int immediateServerVersion;
        private Integer originalServerVersion;
        private Long commitGroupTicket;

        private EventBuilder flags(int flags) {
            this.flags = flags;
            return this;
        }

        private EventBuilder uuid(String uuid) {
            this.uuid = UUID.fromString(uuid);
            return this;
        }

        private EventBuilder gno(long gno) {
            this.gno = gno;
            return this;
        }

        private EventBuilder tag(String tag) {
            this.tag = tag;
            return this;
        }

        private EventBuilder lastCommitted(long lastCommitted) {
            this.lastCommitted = lastCommitted;
            return this;
        }

        private EventBuilder sequenceNumber(long sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
            return this;
        }

        private EventBuilder immediateCommitTimestamp(long immediateCommitTimestamp) {
            this.immediateCommitTimestamp = immediateCommitTimestamp;
            return this;
        }

        private EventBuilder originalCommitTimestamp(long originalCommitTimestamp) {
            this.originalCommitTimestamp = originalCommitTimestamp;
            return this;
        }

        private EventBuilder transactionLength(long transactionLength) {
            this.transactionLength = transactionLength;
            return this;
        }

        private EventBuilder immediateServerVersion(int immediateServerVersion) {
            this.immediateServerVersion = immediateServerVersion;
            return this;
        }

        private EventBuilder originalServerVersion(int originalServerVersion) {
            this.originalServerVersion = originalServerVersion;
            return this;
        }

        private EventBuilder commitGroupTicket(long commitGroupTicket) {
            this.commitGroupTicket = commitGroupTicket;
            return this;
        }

        private byte[] build() throws IOException {
            final java.io.ByteArrayOutputStream fields = new java.io.ByteArrayOutputStream();
            writeFieldId(fields, 0);
            writeUnsignedVarLong(fields, flags);
            writeFieldId(fields, 1);
            writeUuid(fields, uuid);
            writeFieldId(fields, 2);
            writeSignedVarLong(fields, gno);
            writeFieldId(fields, 3);
            writeString(fields, tag);
            writeFieldId(fields, 4);
            writeSignedVarLong(fields, lastCommitted);
            writeFieldId(fields, 5);
            writeSignedVarLong(fields, sequenceNumber);
            writeFieldId(fields, 6);
            writeUnsignedVarLong(fields, immediateCommitTimestamp);
            if (originalCommitTimestamp != null) {
                writeFieldId(fields, 7);
                writeUnsignedVarLong(fields, originalCommitTimestamp);
            }
            writeFieldId(fields, 8);
            writeUnsignedVarLong(fields, transactionLength);
            writeFieldId(fields, 9);
            writeUnsignedVarLong(fields, immediateServerVersion);
            if (originalServerVersion != null) {
                writeFieldId(fields, 10);
                writeUnsignedVarLong(fields, originalServerVersion);
            }
            if (commitGroupTicket != null) {
                writeFieldId(fields, 11);
                writeUnsignedVarLong(fields, commitGroupTicket);
            }

            final int lastNonIgnorableFieldId = commitGroupTicket != null ? 12 : originalServerVersion != null ? 11 : 10;
            int eventSize = fields.size();
            int previousEventSize;
            do {
                previousEventSize = eventSize;
                eventSize = fields.size() + sizeUnsignedVarLong(1) + sizeUnsignedVarLong(previousEventSize)
                        + sizeUnsignedVarLong(lastNonIgnorableFieldId);
            } while (eventSize != previousEventSize);

            final java.io.ByteArrayOutputStream event = new java.io.ByteArrayOutputStream();
            writeUnsignedVarLong(event, 1);
            writeUnsignedVarLong(event, eventSize);
            writeUnsignedVarLong(event, lastNonIgnorableFieldId);
            fields.writeTo(event);
            return event.toByteArray();
        }

        private static void writeFieldId(java.io.ByteArrayOutputStream output, int fieldId) throws IOException {
            writeUnsignedVarLong(output, fieldId);
        }

        private static void writeUuid(java.io.ByteArrayOutputStream output, UUID uuid) throws IOException {
            writeUuidLong(output, uuid.getMostSignificantBits());
            writeUuidLong(output, uuid.getLeastSignificantBits());
        }

        private static void writeString(java.io.ByteArrayOutputStream output, String value) throws IOException {
            final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            writeUnsignedVarLong(output, bytes.length);
            output.write(bytes);
        }

        private static void writeSignedVarLong(java.io.ByteArrayOutputStream output, long value) throws IOException {
            writeUnsignedVarLong(output, value >= 0 ? value << 1 : ((-value - 1) << 1) | 1);
        }

        private static void writeUnsignedVarLong(java.io.ByteArrayOutputStream output, long value) throws IOException {
            final int byteCount = sizeUnsignedVarLong(value);
            output.write((int) (((1L << (byteCount - 1)) - 1) | (value << byteCount)));
            if (byteCount == 1) {
                return;
            }
            long remaining = value >>> (8 - byteCount + ((byteCount + 7) >> 4));
            for (int i = 0; i < byteCount - 1; ++i) {
                output.write((int) (remaining & 0xff));
                remaining >>>= 8;
            }
        }

        private static int sizeUnsignedVarLong(long value) {
            for (int byteCount = 1; byteCount < 9; ++byteCount) {
                if ((value >>> (7 * byteCount)) == 0) {
                    return byteCount;
                }
            }
            return 9;
        }

        private static void writeUuidLong(java.io.ByteArrayOutputStream output, long value) throws IOException {
            for (int i = 7; i >= 0; --i) {
                writeUnsignedVarLong(output, (value >>> (i << 3)) & 0xff);
            }
        }
    }
}
