/*
 * Copyright 2013 Stanley Shyiko
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

import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer.UnixTime;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Calendar;
import java.util.TimeZone;

import static org.testng.Assert.assertEquals;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class AbstractRowsEventDataDeserializerTest {

    @Test
    public void testFrom() throws Exception {
        assetTimeEquals(UnixTime.from(2260, 8, 19, 0, 0, 0, 0),
            timestamp(2260, 8, 19, 0, 0, 0, 0));
        assetTimeEquals(UnixTime.from(2016, 8, 19, 0, 0, 0, 0),
            timestamp(2016, 8, 19, 0, 0, 0, 0));
        assetTimeEquals(UnixTime.from(1970, 1, 1, 0, 0, 0, 0), 0);
        assetTimeEquals(UnixTime.from(1969, 1, 1, 0, 0, 0, 0),
            -365L * 24 * 60 * 60 * 1000);
        assetTimeEquals(UnixTime.from(1582, 10, 15, 0, 0, 0, 0),
            timestamp(1582, 10, 15, 0, 0, 0, 0));
        assetTimeEquals(UnixTime.from(1582, 10, 14, 0, 0, 0, 0),
            timestamp(1582, 10, 14, 0, 0, 0, 0));
        assetTimeEquals(UnixTime.from(1, 1, 1, 0, 0, 0, 0),
            timestamp(1, 1, 1, 0, 0, 0, 0));
        assetTimeEquals(UnixTime.from(0000, 12, 30, 0, 0, 0, 0),
            timestamp(0000, 12, 30, 0, 0, 0, 0));
    }

    @Test
    public void testZeroInDate() throws Exception {
        final long invalidDate = 0L;
        AbstractRowsEventDataDeserializer<EventData> dummyDes = new AbstractRowsEventDataDeserializer<EventData>(null) {
            @Override
            public EventData deserialize(ByteArrayInputStream inputStream) throws IOException {
                return null;
            }
        };
        dummyDes.setInvalidDateAndTimeRepresentation(invalidDate);

        assetTimeEquals(dummyDes.asUnixTime(0000, 12, 30, 0, 0, 0, 0),
            timestamp(0000, 12, 30, 0, 0, 0, 0));
        assetTimeEquals(dummyDes.asUnixTime(2026, 0, 30, 0, 0, 0, 0),
            invalidDate);
        assetTimeEquals(dummyDes.asUnixTime(2026, 12, 0, 0, 0, 0, 0),
            invalidDate);
    }

    @Test
    public void testFromProlepticGregorian() throws Exception {
        assetTimeEquals(UnixTime.fromProlepticGregorian(1, 1, 1, 0, 0, 0, 0), isoTimestamp(1, 1, 1));
        assetTimeEquals(UnixTime.fromProlepticGregorian(1000, 1, 1, 0, 0, 0, 0), isoTimestamp(1000, 1, 1));
        assetTimeEquals(UnixTime.fromProlepticGregorian(1582, 10, 4, 0, 0, 0, 0), isoTimestamp(1582, 10, 4));
        // these ten days do not exist in the calendar of from()
        assetTimeEquals(UnixTime.fromProlepticGregorian(1582, 10, 10, 0, 0, 0, 0), isoTimestamp(1582, 10, 10));
        assetTimeEquals(UnixTime.fromProlepticGregorian(1582, 10, 14, 23, 59, 59, 999),
            isoTimestamp(1582, 10, 15) - 1);
        assetTimeEquals(UnixTime.fromProlepticGregorian(0, 3, 21, 0, 0, 0, 0), isoTimestamp(0, 3, 21));
        assetTimeEquals(UnixTime.fromProlepticGregorian(1000, 1, 1, 1, 2, 3, 4),
            isoTimestamp(1000, 1, 1) + ((1 * 60 + 2) * 60 + 3) * 1000 + 4);
        // the two calendars differ by five days in the year 1000
        assetTimeEquals(UnixTime.from(1000, 1, 1, 0, 0, 0, 0) - UnixTime.fromProlepticGregorian(1000, 1, 1, 0, 0, 0, 0),
            5L * 24 * 60 * 60 * 1000);
    }

    @Test
    public void testFromProlepticGregorianSinceCutover() throws Exception {
        assetTimeEquals(UnixTime.fromProlepticGregorian(1582, 10, 15, 0, 0, 0, 0), isoTimestamp(1582, 10, 15));
        assetTimeEquals(UnixTime.fromProlepticGregorian(1969, 12, 31, 23, 59, 59, 999), -1);
        assetTimeEquals(UnixTime.fromProlepticGregorian(9999, 12, 31, 0, 0, 0, 0), isoTimestamp(9999, 12, 31));
        assetTimeEquals(UnixTime.fromProlepticGregorian(2016, 8, 19, 0, 0, 0, 0),
            UnixTime.from(2016, 8, 19, 0, 0, 0, 0));
    }

    @Test
    public void testDateBeforeCutoverAsLong() throws Exception {
        AbstractRowsEventDataDeserializer<EventData> asLong = new AbstractRowsEventDataDeserializer<EventData>(null) {
            @Override
            public EventData deserialize(ByteArrayInputStream inputStream) throws IOException {
                return null;
            }
        };
        asLong.setDeserializeDateAndTimeAsLong(true);
        assetTimeEquals(asLong.asUnixTime(1000, 1, 1, 0, 0, 0, 0), isoTimestamp(1000, 1, 1));

        AbstractRowsEventDataDeserializer<EventData> asDate = new AbstractRowsEventDataDeserializer<EventData>(null) {
            @Override
            public EventData deserialize(ByteArrayInputStream inputStream) throws IOException {
                return null;
            }
        };
        // a java.util.Date of this value reads 1000-01-01 in GMT
        assetTimeEquals(asDate.asUnixTime(1000, 1, 1, 0, 0, 0, 0), timestamp(1000, 1, 1, 0, 0, 0, 0));
    }

    private static long isoTimestamp(int year, int month, int dayOfMonth) {
        return java.time.LocalDate.of(year, month, dayOfMonth).toEpochDay() * 24 * 60 * 60 * 1000;
    }

    private void assetTimeEquals(long actual, long expected) {
        assertEquals(actual, expected, actual + " != " + expected +
            ", discrepancy: " + (actual - expected));
    }

    // checkstyle, please ignore ParameterNumber for the next line
    private long timestamp(int year, int month, int dayOfMonth, int hourOfDay,
            int minute, int second, int millis) {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        c.set(Calendar.YEAR, year);
        c.set(Calendar.MONTH, month - 1);
        c.set(Calendar.DAY_OF_MONTH, dayOfMonth);
        c.set(Calendar.HOUR_OF_DAY, hourOfDay);
        c.set(Calendar.MINUTE, minute);
        c.set(Calendar.SECOND, second);
        c.set(Calendar.MILLISECOND, millis);
        return c.getTimeInMillis();
    }

}
