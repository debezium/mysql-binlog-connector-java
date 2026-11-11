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
    public void testDatetimeMinutesInLargeYears() throws Exception {
        // bit 15 of year * 13 + month is set from 2520-08 to 5041-02, and from 7561-11 on
        assertEquals(parseDatetime(2520, 7, 31, 23, 59, 59, 0), "\"2520-07-31 23:59:59\"");
        assertEquals(parseDatetime(2520, 8, 1, 23, 59, 59, 0), "\"2520-08-01 23:59:59\"");
        assertEquals(parseDatetime(5041, 2, 28, 0, 1, 0, 0), "\"5041-02-28 00:01:00\"");
        assertEquals(parseDatetime(5041, 3, 1, 23, 59, 59, 0), "\"5041-03-01 23:59:59\"");
        assertEquals(parseDatetime(7561, 11, 1, 12, 30, 0, 0), "\"7561-11-01 12:30:00\"");
        assertEquals(parseDatetime(9999, 12, 31, 23, 59, 59, 999999), "\"9999-12-31 23:59:59.999999\"");
    }

    // checkstyle, please ignore ParameterNumber for the next line
    private static String parseDatetime(int year, int month, int day, int hour, int min, int sec, int microSeconds)
            throws Exception {
        long ymd = ((long) (year * 13 + month) << 5) | day;
        long hms = ((long) hour << 12) | (min << 6) | sec;
        long packed = (((ymd << 17) | hms) << 24) + microSeconds;
        byte[] json = new byte[11];
        json[0] = 0x0F; // opaque
        json[1] = 12; // MYSQL_TYPE_DATETIME
        json[2] = 8; // length
        for (int i = 0; i < 8; i++) {
            json[3 + i] = (byte) (packed >>> (8 * i));
        }
        return JsonBinary.parseAsString(json);
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
}
