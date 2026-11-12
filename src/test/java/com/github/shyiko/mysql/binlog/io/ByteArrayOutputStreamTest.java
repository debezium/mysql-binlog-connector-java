package com.github.shyiko.mysql.binlog.io;

import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;

public class ByteArrayOutputStreamTest {

    @Test
    public void testWriteStringAsUtf8() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeString("주문");
        assertEquals(out.toByteArray(), "주문".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testWriteZeroTerminatedStringAsUtf8() throws Exception {
        byte[] name = "주문".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeZeroTerminatedString("주문");
        assertEquals(out.toByteArray(), Arrays.copyOf(name, name.length + 1));
    }
}
