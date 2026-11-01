package com.github.shyiko.mysql.binlog.network.protocol;

import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;

import static org.testng.Assert.assertEquals;

public class ColumnDefinitionPacketTest {

    @Test
    public void testName() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (String value : new String[] {"def", "", "", "", "Variable_name", "Variable_name"}) {
            bytes.write(value.length());
            bytes.write(value.getBytes("US-ASCII"));
        }
        // the fixed length fields that follow the names
        bytes.write(new byte[] {0x0c, 0x21, 0x00, 0x00, 0x01, 0x00, 0x00, (byte) 0xFD, 0x01, 0x00, 0x1F, 0x00, 0x00});
        assertEquals(new ColumnDefinitionPacket(bytes.toByteArray()).getName(), "Variable_name");
    }
}
