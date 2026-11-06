package com.github.shyiko.mysql.binlog.event.deserialization.json;

import java.math.BigDecimal;
import java.util.Random;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

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
    public void testFormatSmallDoubles() {
        // Double.toString writes these in scientific notation; they keep its shortest digits, in plain notation
        assertDouble(0.0001, "0.0001");
        assertDouble(0.000313967, "0.000313967");
        assertDouble(-0.000313967, "-0.000313967");
        assertDouble(0.000999, "0.000999");
        assertDouble(1e-7, "0.0000001");
        assertDouble(1e-16, "0.0000000000000001");
    }

    @Test
    public void testFormatLargeDoubles() {
        // Double.toString writes these in scientific notation; they keep its shortest digits, in plain notation
        assertDouble(12345678.9, "12345678.9");
        assertDouble(-12345678.9, "-12345678.9");
        assertDouble(1234567890123456.8, "1234567890123456.8");
        // Whole numbers below 1e15 get ".0", as MySQL writes them
        assertDouble(1e7, "10000000.0");
        assertDouble(24655300.0, "24655300.0");
        assertDouble(-24655300.0, "-24655300.0");
        assertDouble(2400000000000.0, "2400000000000.0");
        assertDouble(999999999999999.0, "999999999999999.0");
        // From 1e15 up, whole numbers keep their exact integer value, as before
        assertDouble(1e15, "1000000000000000");
        assertDouble(1.5e20, "150000000000000000000");
        assertDouble(18446744073709551616.0, "18446744073709551616");
    }

    @Test
    public void testFormatDoublesBetween1e3And1e7() {
        // Double.toString already writes these in plain notation, so they are unchanged
        assertDouble(0.001, "0.001");
        assertDouble(0.5, "0.5");
        assertDouble(5.0, "5.0");
        assertDouble(-5.0, "-5.0");
        assertDouble(123.456, "123.456");
        assertDouble(9999999.0, "9999999.0");
        assertDouble(0.0, "0.0");
        assertDouble(-0.0, "-0.0");
    }

    @Test
    public void testFormattedDoublesKeepTheirValue() {
        Random random = new Random(2962);
        for (int i = 0; i < 200_000; i++) {
            // Half with random bits (any exponent), half between 1e-20 and 1e20, where the formatting changes
            double value = i % 2 == 0
                ? Double.longBitsToDouble(random.nextLong())
                : (random.nextBoolean() ? 1 : -1) * Math.pow(10, random.nextDouble() * 40 - 20);
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                continue;
            }
            JsonStringFormatter json = new JsonStringFormatter();
            json.value(value);
            assertFormattedDouble(value, json.getString());
        }
    }

    @Test
    public void testFormattedWholeDoublesKeepTheirValue() {
        Random random = new Random(2962);
        for (int i = 0; i < 100_000; i++) {
            // Whole numbers from 1e7 up to 2^53, the largest range where every whole number is a double
            double value = (random.nextBoolean() ? 1 : -1) * (double) (10_000_000L + (random.nextLong() >>> 11) % (1L << 53));
            JsonStringFormatter json = new JsonStringFormatter();
            json.value(value);
            assertFormattedDouble(value, json.getString());
        }
    }

    private static void assertFormattedDouble(double value, String text) {
        // Plain notation that reads back as the same double
        assertFalse(text.contains("E"), text);
        assertEquals(Double.parseDouble(text), value, text);
        BigDecimal written = new BigDecimal(text);
        if (value == Math.rint(value)) {
            // A whole number has its exact value, and ".0" below 1e15
            assertEquals(written.compareTo(new BigDecimal(value)), 0, text);
            assertEquals(text.endsWith(".0"), Math.abs(value) < 1e15, text);
        } else {
            // Any other number has Double.toString's (shortest) digits, not its exact binary value
            assertEquals(written.compareTo(new BigDecimal(Double.toString(value))), 0, text);
        }
    }

    private static void assertDouble(double value, String expected) {
        JsonStringFormatter json = new JsonStringFormatter();
        json.value(value);
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
