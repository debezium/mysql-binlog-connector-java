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
package com.github.shyiko.mysql.binlog;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.RotateEventData;

/**
 * The log_pos field of the common event header is an unsigned 32-bit integer on disk, so the
 * value delivered by the server wraps around for events located past 4GiB in a binlog file.
 * These tests verify that the client rebuilds the real position from the ROTATE anchor plus
 * the accumulated event lengths and rewrites the header with it.
 */
public class BinaryLogClientLinearPositionTest {

    private static final int LOG_EVENT_ARTIFICIAL_F = 0x20;
    private static final long FOUR_GIB = 4294967296L;

    private BinaryLogClient client;

    @BeforeMethod
    public void setUp() {
        client = new BinaryLogClient("localhost", 3306, "root", "");
    }

    @Test
    public void testRewritesWrappedPositionPastFourGiB() {
        client.rewriteWrappedNextPosition(rotate(FOUR_GIB - 296)); // anchor just below the wrap
        Event event = inBandEvent(1000, /* wrapped header value */ 704);
        client.rewriteWrappedNextPosition(event);
        assertEquals(nextPosition(event), FOUR_GIB + 704);
    }

    @Test
    public void testPositionsBelowFourGiBAreUnchanged() {
        client.rewriteWrappedNextPosition(rotate(4));
        Event event = inBandEvent(119, 123);
        client.rewriteWrappedNextPosition(event);
        assertEquals(nextPosition(event), 123L);
    }

    @Test
    public void testArtificialEventsDoNotAdvanceThePosition() {
        client.rewriteWrappedNextPosition(rotate(1000));
        Event heartbeat = event(EventType.HEARTBEAT, 45, 7777, LOG_EVENT_ARTIFICIAL_F);
        client.rewriteWrappedNextPosition(heartbeat);
        assertEquals(nextPosition(heartbeat), 7777L); // left untouched

        Event event = inBandEvent(100, 1100);
        client.rewriteWrappedNextPosition(event);
        assertEquals(nextPosition(event), 1100L); // heartbeat did not shift the tracking
    }

    @Test
    public void testZeroPositionEventsDoNotAdvanceThePosition() {
        client.rewriteWrappedNextPosition(rotate(2000));
        // the format description re-sent for a dump started mid-file carries log_pos = 0
        Event formatDescription = event(EventType.FORMAT_DESCRIPTION, 122, 0, 0);
        client.rewriteWrappedNextPosition(formatDescription);
        assertEquals(nextPosition(formatDescription), 0L); // left untouched

        Event event = inBandEvent(500, 2500);
        client.rewriteWrappedNextPosition(event);
        assertEquals(nextPosition(event), 2500L);
    }

    @Test
    public void testRotateReanchorsAtTheNextFile() {
        client.rewriteWrappedNextPosition(rotate(FOUR_GIB + 5000));
        client.rewriteWrappedNextPosition(rotate(4)); // the next file starts over
        Event event = inBandEvent(119, 123);
        client.rewriteWrappedNextPosition(event);
        assertEquals(nextPosition(event), 123L);
    }

    @Test
    public void testAdoptsServerPositionWhenNoAnchorWasSeen() {
        Event first = inBandEvent(100, 600);
        client.rewriteWrappedNextPosition(first);
        assertEquals(nextPosition(first), 600L); // no anchor: left untouched, value adopted

        Event second = inBandEvent(50, 650);
        client.rewriteWrappedNextPosition(second);
        assertEquals(nextPosition(second), 650L); // accumulation continues from the adopted value
    }

    private static Event rotate(long position) {
        RotateEventData data = new RotateEventData();
        data.setBinlogFilename("mysql-bin.000001");
        data.setBinlogPosition(position);
        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(EventType.ROTATE);
        header.setFlags(LOG_EVENT_ARTIFICIAL_F);
        header.setNextPosition(0);
        return new Event(header, data);
    }

    private static Event inBandEvent(long length, long headerNextPosition) {
        return event(EventType.WRITE_ROWS, length, headerNextPosition, 0);
    }

    private static Event event(EventType type, long length, long headerNextPosition, int flags) {
        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(type);
        header.setEventLength(length);
        header.setNextPosition(headerNextPosition);
        header.setFlags(flags);
        return new Event(header, null);
    }

    private static long nextPosition(Event event) {
        return ((EventHeaderV4) event.getHeader()).getNextPosition();
    }
}
