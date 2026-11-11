package com.github.shyiko.mysql.binlog.event.deserialization.json;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class JsonBinaryTest {

    @Test
    public void testInlinedUInt32InLargeObject() throws Exception {
        byte[] json = new byte[] {
            0x01, // large object
            2, 0, 0, 0, // element count
            32, 0, 0, 0, // size
            30, 0, 0, 0, 1, 0, // key "a": offset 30, length 1
            31, 0, 0, 0, 1, 0, // key "b": offset 31, length 1
            0x08, (byte) 0xA0, (byte) 0x86, 0x01, 0x00, // UINT32 100000, inlined
            0x07, 0x60, 0x79, (byte) 0xFE, (byte) 0xFF, // INT32 -100000, inlined
            'a', 'b'
        };
        assertEquals(JsonBinary.parseAsString(json), "{\"a\":100000,\"b\":-100000}");
    }

    @Test
    public void testInlinedUInt32InLargeArray() throws Exception {
        byte[] json = new byte[] {
            0x03, // large array
            2, 0, 0, 0, // element count
            18, 0, 0, 0, // size
            0x08, (byte) 0xA0, (byte) 0x86, 0x01, 0x00, // UINT32 100000, inlined
            0x07, 0x60, 0x79, (byte) 0xFE, (byte) 0xFF // INT32 -100000, inlined
        };
        assertEquals(JsonBinary.parseAsString(json), "[100000,-100000]");
    }

    @Test
    public void testUInt32AboveIntegerMaxValue() throws Exception {
        assertEquals(JsonBinary.parseAsString(new byte[] {
            0x08, 0x00, 0x00, 0x00, (byte) 0x80 // UINT32 2147483648
        }), "2147483648");
        assertEquals(JsonBinary.parseAsString(new byte[] {
            0x08, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF // UINT32 4294967295
        }), "4294967295");
        assertEquals(JsonBinary.parseAsString(new byte[] {
            0x03, // large array
            1, 0, 0, 0, // element count
            13, 0, 0, 0, // size
            0x08, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF // UINT32 4294967295, inlined
        }), "[4294967295]");
    }

    @Test
    public void testNegativeTime() throws Exception {
        assertEquals(parseTime(true, 1, 30, 15, 500000), "\"-01:30:15.5\"");
        assertEquals(parseTime(true, 1, 0, 0, 0), "\"-01:00:00\"");
        assertEquals(parseTime(true, 23, 59, 59, 999999), "\"-23:59:59.999999\"");
        assertEquals(parseTime(false, 23, 59, 59, 999999), "\"23:59:59.999999\"");
    }

    @Test
    public void testNegativeTimeWithZeroHour() throws Exception {
        assertEquals(parseTime(true, 0, 30, 0, 0), "\"-00:30:00\"");
        assertEquals(parseTime(true, 0, 0, 1, 0), "\"-00:00:01\"");
        assertEquals(parseTime(true, 0, 0, 0, 500000), "\"-00:00:00.5\"");
    }

    private static String parseTime(boolean negative, int hour, int min, int sec, int microSeconds)
            throws Exception {
        long packed = ((((long) hour << 12) | (min << 6) | sec) << 24) + microSeconds;
        if (negative) {
            packed = -packed;
        }
        byte[] json = new byte[11];
        json[0] = 0x0F; // opaque
        json[1] = 11; // MYSQL_TYPE_TIME
        json[2] = 8; // length
        for (int i = 0; i < 8; i++) {
            json[3 + i] = (byte) (packed >>> (8 * i));
        }
        return JsonBinary.parseAsString(json);
    }
}
