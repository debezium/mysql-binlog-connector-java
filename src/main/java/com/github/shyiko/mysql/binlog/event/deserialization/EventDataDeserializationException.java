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

import com.github.shyiko.mysql.binlog.event.EventHeader;

import java.io.IOException;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class EventDataDeserializationException extends IOException {

    private EventHeader eventHeader;
    private enum FailurePhase {
        UNSPECIFIED, READ, DECODE
    }

    private final FailurePhase positionAnchorFailurePhase;

    public EventDataDeserializationException(EventHeader eventHeader, Throwable cause) {
        this(eventHeader, cause, FailurePhase.UNSPECIFIED);
    }

    EventDataDeserializationException(EventHeader eventHeader, Throwable cause, boolean positionAnchorDecodeFailure) {
        this(eventHeader, cause, positionAnchorDecodeFailure ? FailurePhase.DECODE : FailurePhase.UNSPECIFIED);
    }

    private EventDataDeserializationException(EventHeader eventHeader, Throwable cause,
            FailurePhase positionAnchorFailurePhase) {
        super("Failed to deserialize data of " + eventHeader, cause);
        this.eventHeader = eventHeader;
        this.positionAnchorFailurePhase = positionAnchorFailurePhase;
    }

    static EventDataDeserializationException positionAnchorReadFailure(EventHeader eventHeader, IOException cause) {
        return new EventDataDeserializationException(eventHeader, cause, FailurePhase.READ);
    }

    public EventHeader getEventHeader() {
        return eventHeader;
    }

    /**
     * Whether a MySQL ROTATE or HEARTBEAT_V2 event declared an invalid body length or failed
     * to decode after receiving its complete payload.
     * Such a failure is local to the payload or decoder, even if its cause is an EOF/socket exception.
     * A false result does not by itself establish that the failure came from the transport.
     *
     * @return true for an invalid position anchor header or a failure decoding its complete payload
     */
    public boolean isPositionAnchorDecodeFailure() {
        return positionAnchorFailurePhase == FailurePhase.DECODE;
    }

    /**
     * Whether receiving a position anchor's body or checksum failed before invoking its decoder.
     * This identifies transport failures even when the cause is a timeout or another generic IOException.
     *
     * @return true for an I/O failure while receiving a position anchor
     */
    public boolean isPositionAnchorReadFailure() {
        return positionAnchorFailurePhase == FailurePhase.READ;
    }
}
