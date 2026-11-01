package com.github.shyiko.mysql.binlog.network.protocol;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class ResultSetRowPacketTest {

    @Test
    public void testNullAndEmptyValues() throws Exception {
        ResultSetRowPacket row = new ResultSetRowPacket(new byte[] {
            3, 'a', 'b', 'c', // "abc"
            (byte) 0xFB, // NULL
            0 // ""
        });
        assertEquals(row.size(), 3);
        assertEquals(row.getValue(0), "abc");
        assertNull(row.getValue(1));
        assertEquals(row.getValue(2), "");
    }
}
