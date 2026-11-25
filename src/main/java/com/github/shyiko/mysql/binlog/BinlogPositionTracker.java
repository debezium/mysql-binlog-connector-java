/*
 * Copyright 2026 Debezium Authors
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

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.HeartbeatV2EventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer.EventDataWrapper;

/**
 * Reconstructs MySQL's file positions from the common header's low 32 bits and
 * the full positions carried by rotate and heartbeat v2 events. One instance is
 * used per connection; decompressed transaction events are not file events.
 */
final class BinlogPositionTracker {

    private static final long UINT32_MASK = 0xffffffffL;
    private static final long UINT32_RANGE = 1L << 32;
    private static final int BINLOG_HEADER_LENGTH = 4;
    private static final int LOG_EVENT_ARTIFICIAL_F = 0x20;

    private long physicalPosition;

    BinlogPositionTracker(long physicalPosition) {
        this.physicalPosition = physicalPosition;
    }

    /** @return whether the event may update the client's resume position */
    boolean normalizeEventPosition(Event event) {
        EventHeader eventHeader = event.getHeader();
        EventType eventType = eventHeader.getEventType();
        if (eventType != EventType.ROTATE && eventType != EventType.HEARTBEAT_V2) {
            return normalizeHeaderPosition(eventHeader);
        }
        EventData internalEventData = EventDataWrapper.internal(event.getData());
        if (eventType == EventType.ROTATE) {
            // A real ROTATE ends the old file. Its payload already points into the new file.
            // The sender also synthesizes a zero-timestamp ROTATE when it opens each file.
            if (eventHeader.getTimestamp() != 0) {
                normalizeHeaderPosition(eventHeader);
            }
            if (internalEventData instanceof RotateEventData rotate) {
                physicalPosition = rotate.getBinlogPosition();
            }
            return true;
        }
        if (internalEventData instanceof HeartbeatV2EventData heartbeat && heartbeat.getBinlogPosition() > 0) {
            // The sender emits this anchor after GTID-filtered groups, even when periodic
            // heartbeats are disabled. A skipped range can span multiple 32-bit wraps.
            physicalPosition = heartbeat.getBinlogPosition();
            if (eventHeader instanceof EventHeaderV4 header) {
                header.setNextPosition(physicalPosition);
            }
            return true;
        }
        // An omitted or zero payload position can still use the common header's low bits.
        // If both are absent, preserve the last known physical position.
        if (eventHeader instanceof EventHeaderV4 header && header.getNextPosition() == 0) {
            return false;
        }
        return normalizeHeaderPosition(eventHeader);
    }

    long getPhysicalPosition() {
        return physicalPosition;
    }

    boolean normalizeHeaderPosition(EventHeader eventHeader) {
        if (!(eventHeader instanceof EventHeaderV4 header)) {
            return false;
        }
        EventType eventType = header.getEventType();
        boolean heartbeat = eventType == EventType.HEARTBEAT || eventType == EventType.HEARTBEAT_V2;
        // Heartbeat v2 reports a position even when it is marked artificial.
        if (eventType != EventType.HEARTBEAT_V2 && (header.getFlags() & LOG_EVENT_ARTIFICIAL_F) != 0) {
            return false;
        }
        if (eventType == EventType.STOP && physicalPosition == BINLOG_HEADER_LENGTH) {
            // A new file starts with an FDE. A trailing STOP immediately after rotation still belongs to the old file.
            return false;
        }
        if (eventType == EventType.FORMAT_DESCRIPTION && (physicalPosition > BINLOG_HEADER_LENGTH || header.getNextPosition() == 0)) {
            // A resumed stream replays the file's FDE without consuming bytes at the requested
            // position. In GTID mode, that replay can retain its original nonzero log_pos.
            return false;
        }
        // A heartbeat reports an existing file position; its own bytes are never written there.
        long minimumPosition = physicalPosition + (heartbeat ? 0 : header.getEventLength());
        long nextPosition = header.getNextPosition();
        if (nextPosition <= UINT32_MASK) {
            // Keep the server's low bits instead of assuming that all preceding bytes were sent.
            // GTID filtering and recoverable deserialization failures can leave gaps in the stream.
            nextPosition |= minimumPosition & ~UINT32_MASK;
            if (nextPosition < minimumPosition) {
                nextPosition += UINT32_RANGE;
            }
        }
        physicalPosition = nextPosition;
        header.setNextPosition(nextPosition);
        return true;
    }
}
