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
package com.github.shyiko.mysql.binlog.jmx;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class BinaryLogClientStatisticsTest {

    @Test
    public void testInitialState() throws Exception {
        BinaryLogClientStatistics statistics = new BinaryLogClientStatistics();
        assertNull(statistics.getLastEvent());
        assertEquals(statistics.getSecondsSinceLastEvent(), 0L);
        assertEquals(statistics.getSecondsBehindMaster(), -1L);
        assertEquals(statistics.getTotalNumberOfEventsSeen(), 0L);
        assertEquals(statistics.getNumberOfSkippedEvents(), 0L);
        assertEquals(statistics.getNumberOfDisconnects(), 0L);
    }

    @Test
    public void testOnEvent() throws Exception {
        ControlledTimeStatistics statistics = new ControlledTimeStatistics();
        assertNull(statistics.getLastEvent());
        assertEquals(statistics.getSecondsSinceLastEvent(), 0L);
        assertEquals(statistics.getTotalNumberOfEventsSeen(), 0L);
        assertEquals(statistics.getNumberOfSkippedEvents(), 0L);
        assertEquals(statistics.getNumberOfDisconnects(), 0L);
        statistics.onEvent(generateEvent(1L, EventType.FORMAT_DESCRIPTION, 1, 104));
        assertEquals(statistics.getLastEvent(), "FORMAT_DESCRIPTION/1 from server 1");
        statistics.nanoTime += TimeUnit.SECONDS.toNanos(1);
        assertEquals(statistics.getSecondsSinceLastEvent(), 1);
        assertEquals(statistics.getSecondsBehindMaster(), 0);
        assertEquals(statistics.getTotalNumberOfEventsSeen(), 1);
        assertEquals(statistics.getNumberOfSkippedEvents(), 0);
        statistics.currentTimeMillis = 3030;
        statistics.onEvent(generateEvent(1L, EventType.FORMAT_DESCRIPTION, 1, 104));
        assertEquals(statistics.getSecondsBehindMaster(), 3);
    }

    @DataProvider(name = "wallClockAdjustments")
    public Object[][] wallClockAdjustments() {
        return new Object[][] {
            { TimeUnit.HOURS.toMillis(1) },
            { -TimeUnit.HOURS.toMillis(1) }
        };
    }

    @Test(dataProvider = "wallClockAdjustments")
    public void testSecondsSinceLastEventIgnoresWallClockAdjustments(long adjustmentMillis) {
        ControlledTimeStatistics statistics = new ControlledTimeStatistics();
        statistics.currentTimeMillis = TimeUnit.DAYS.toMillis(1);
        long eventTimestamp = statistics.currentTimeMillis - TimeUnit.SECONDS.toMillis(2);
        statistics.onEvent(generateEvent(eventTimestamp, EventType.QUERY, 1, 104));

        statistics.currentTimeMillis += adjustmentMillis;
        statistics.nanoTime += TimeUnit.SECONDS.toNanos(5);

        assertEquals(statistics.getSecondsSinceLastEvent(), 5L);
        assertEquals(statistics.getSecondsBehindMaster(), 2L);
        assertEquals(statistics.getLastEvent(), "QUERY/" + eventTimestamp + " from server 1");
    }

    @DataProvider(name = "nanoTimeOrigins")
    public Object[][] nanoTimeOrigins() {
        return new Object[][] {
            { 0L },
            { -TimeUnit.SECONDS.toNanos(5) },
            { Long.MAX_VALUE - TimeUnit.SECONDS.toNanos(1) }
        };
    }

    @Test(dataProvider = "nanoTimeOrigins")
    public void testSecondsSinceLastEventSupportsNanoTimeOriginsAndWraparound(long nanoTime) {
        ControlledTimeStatistics statistics = new ControlledTimeStatistics();
        statistics.nanoTime = nanoTime;
        statistics.onEvent(generateEvent(1L, EventType.QUERY, 1, 104));

        statistics.nanoTime += TimeUnit.SECONDS.toNanos(2);

        assertEquals(statistics.getSecondsSinceLastEvent(), 2L);
        assertEquals(statistics.getSecondsBehindMaster(), 0L);
    }

    @Test
    public void testHeartbeatWithoutTimestampHasNoLag() {
        ControlledTimeStatistics statistics = new ControlledTimeStatistics();
        statistics.onEvent(generateEvent(0L, EventType.HEARTBEAT, 1, 104));

        statistics.nanoTime += TimeUnit.SECONDS.toNanos(2);

        assertEquals(statistics.getSecondsSinceLastEvent(), 2L);
        assertEquals(statistics.getSecondsBehindMaster(), 0L);
    }

    @Test
    public void testOnEventDeserializationFailure() throws Exception {
        ControlledTimeStatistics statistics = new ControlledTimeStatistics();
        statistics.onEvent(generateEvent(1L, EventType.FORMAT_DESCRIPTION, 1, 104));
        statistics.nanoTime += TimeUnit.SECONDS.toNanos(2);
        statistics.onEventDeserializationFailure(null, null);
        statistics.nanoTime += TimeUnit.SECONDS.toNanos(3);
        assertNull(statistics.getLastEvent());
        assertEquals(statistics.getSecondsSinceLastEvent(), 3L);
        assertEquals(statistics.getSecondsBehindMaster(), -1L);
        assertEquals(statistics.getTotalNumberOfEventsSeen(), 2L);
        assertEquals(statistics.getNumberOfSkippedEvents(), 1L);
    }

    @Test
    public void testOnDisconnect() throws Exception {
        ControlledTimeStatistics statistics = new ControlledTimeStatistics();
        statistics.onEvent(generateEvent(1L, EventType.QUERY, 1, 104));
        statistics.nanoTime += TimeUnit.SECONDS.toNanos(2);
        statistics.onDisconnect(null);
        assertEquals(statistics.getNumberOfDisconnects(), 1L);
        assertEquals(statistics.getSecondsSinceLastEvent(), 2L);
    }

    @Test
    public void testReset() throws Exception {
        ControlledTimeStatistics statistics = new ControlledTimeStatistics();
        statistics.onEventDeserializationFailure(null, null);
        statistics.onEvent(generateEvent(1L, EventType.FORMAT_DESCRIPTION, 1, 104));
        statistics.onDisconnect(null);
        statistics.reset();
        statistics.nanoTime += TimeUnit.SECONDS.toNanos(5);
        assertNull(statistics.getLastEvent());
        assertEquals(statistics.getSecondsSinceLastEvent(), 0L);
        assertEquals(statistics.getSecondsBehindMaster(), -1L);
        assertEquals(statistics.getTotalNumberOfEventsSeen(), 0L);
        assertEquals(statistics.getNumberOfSkippedEvents(), 0L);
        assertEquals(statistics.getNumberOfDisconnects(), 0L);

        statistics.onEvent(generateEvent(1L, EventType.QUERY, 1, 104));
        statistics.nanoTime += TimeUnit.SECONDS.toNanos(2);
        assertEquals(statistics.getSecondsSinceLastEvent(), 2L);
        assertEquals(statistics.getSecondsBehindMaster(), 0L);
    }

    private Event generateEvent(long timestamp, EventType type, long serverId, long nextPosition) {
        EventHeaderV4 header = new EventHeaderV4();
        header.setTimestamp(timestamp);
        header.setEventType(type);
        header.setServerId(serverId);
        header.setNextPosition(nextPosition);
        return new Event(header, null);
    }

    private static class ControlledTimeStatistics extends BinaryLogClientStatistics {

        private long currentTimeMillis = 1;
        private long nanoTime;

        @Override
        protected long getCurrentTimeMillis() {
            return currentTimeMillis;
        }

        @Override
        protected long getCurrentTimeNanos() {
            return nanoTime;
        }
    }
}
