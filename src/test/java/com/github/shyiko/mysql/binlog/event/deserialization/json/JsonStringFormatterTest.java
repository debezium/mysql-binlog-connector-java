package com.github.shyiko.mysql.binlog.event.deserialization.json;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class JsonStringFormatterTest {

    @Test
    public void testFormatLong() {
        assertLong(1234567890L, "1234567890");
        assertLong(-1234567890L, "-1234567890");
        assertLong(0L, "0");
        assertLong(Long.MAX_VALUE, Long.toString(Long.MAX_VALUE));
        assertLong(Long.MIN_VALUE, Long.toString(Long.MIN_VALUE));
    }

    @Test
    public void testFormatInt() {
        assertInt(1234567890, "1234567890");
        assertInt(-1234567890, "-1234567890");
        assertInt(0, "0");
        assertInt(Integer.MAX_VALUE, Integer.toString(Integer.MAX_VALUE));
        assertInt(Integer.MIN_VALUE, Integer.toString(Integer.MIN_VALUE));
    }

    @Test
    public void testNewFormatterWithInitialCapacity() {
        JsonStringFormatter formatter = new JsonStringFormatter(32);
        formatter.value("test");
        assertEquals(formatter.getString(), "\"test\"");
    }

    @Test
    public void testFormatTime() {
        assertTime(false, 1, 2, 3, 0, "\"01:02:03\"");
        assertTime(true, 1, 2, 3, 0, "\"-01:02:03\"");
        assertTime(true, 0, 0, 1, 0, "\"-00:00:01\"");
        assertTime(true, 0, 0, 0, 500000, "\"-00:00:00.5\"");

        JsonStringFormatter json = new JsonStringFormatter();
        json.valueTime(-1, 2, 3, 0);
        assertEquals(json.getString(), "\"-01:02:03\"");
    }

    private static void assertTime(boolean negative, int hour, int min, int sec, int microSeconds, String expected) {
        JsonStringFormatter json = new JsonStringFormatter();
        json.valueTime(negative, hour, min, sec, microSeconds);
        assertEquals(json.getString(), expected);
    }

    private static void assertLong(long value, String expected) {
        JsonStringFormatter json = new JsonStringFormatter();
        json.value(value);
        assertEquals(json.getString(), expected);
    }

    private static void assertInt(int value, String expected) {
        JsonStringFormatter json = new JsonStringFormatter();
        json.value(value);
        assertEquals(json.getString(), expected);
    }
}
