/*
 * Copyright Debezium Authors.
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

import com.github.luben.zstd.Zstd;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.ByteArrayEventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TransactionPayloadEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializationException;
import com.github.shyiko.mysql.binlog.event.deserialization.ChecksumType;
import com.github.shyiko.mysql.binlog.event.deserialization.ByteArrayEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.HeartbeatV2EventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.NullEventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.RotateEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.github.shyiko.mysql.binlog.io.ByteArrayOutputStream;
import com.github.shyiko.mysql.binlog.network.protocol.PacketChannel;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class BinaryLogClientPositionTest {

    private static final long UINT32_RANGE = 1L << 32;
    private static final String FILE = "mysql-bin.000001";

    @Test
    public void testWrappedPositionsReachListenersAndClient() throws Exception {
        BinaryLogClient client = client();
        List<Event> events = read(client, rotate(UINT32_RANGE - 40),
            normal(100, 60), normal(50, 110));

        assertPosition(events.get(1), UINT32_RANGE - 40, UINT32_RANGE + 60);
        assertPosition(events.get(2), UINT32_RANGE + 60, UINT32_RANGE + 110);
        assertEquals(client.getBinlogPosition(), UINT32_RANGE + 110);
    }

    @Test
    public void testNewConnectionResetsTrackerAfterRewind() throws Exception {
        BinaryLogClient client = client();
        read(client, rotate(UINT32_RANGE + 1207), normal(100, 1307));
        client.setBinlogPosition(1207);
        List<Event> replayed = read(client, formatDescription(0), normal(100, 1307));

        assertPosition(replayed.get(1), 1207, 1307);
        assertEquals(client.getBinlogPosition(), 1307L);
    }

    @Test
    public void testInternalAnchorsPreserveCustomExternalData() throws Exception {
        BinaryLogClient client = client();
        EventDeserializer deserializer = new EventDeserializer();
        deserializer.setEventDataDeserializer(EventType.ROTATE, new NullEventDataDeserializer());
        deserializer.setEventDataDeserializer(EventType.HEARTBEAT_V2, new ByteArrayEventDataDeserializer());
        client.setEventDeserializer(deserializer);
        // These are the same preparation calls made by connect() before entering the packet loop.
        client.ensureEventDataDeserializer(EventType.ROTATE, RotateEventDataDeserializer.class);
        client.ensureEventDataDeserializer(EventType.HEARTBEAT_V2, HeartbeatV2EventDataDeserializer.class);
        long start = 2 * UINT32_RANGE + 1207;
        List<Event> events = read(client, rotate(4), heartbeatV2(start), normal(100, 1307));

        assertNull(events.get(0).getData());
        assertTrue(events.get(1).getData() instanceof ByteArrayEventData);
        assertPosition(events.get(2), start, start + 100);
    }

    @Test
    public void testRepeatedHeartbeatsDoNotAdvanceFilePosition() throws Exception {
        BinaryLogClient client = client();
        long start = UINT32_RANGE + 1207;
        List<Event> events = read(client, rotate(start), heartbeatV2(start), heartbeatV2(start),
            packet(EventType.HEARTBEAT, start, 0, FILE.getBytes(StandardCharsets.UTF_8)), normal(100, 1307));

        assertPosition(events.get(4), start, start + 100);
    }

    @Test
    public void testRealEventEndingExactlyAtWrapBoundary() throws Exception {
        BinaryLogClient client = client();
        List<Event> events = read(client, rotate(UINT32_RANGE - 100),
            normal(100, 0), normal(50, 50));

        assertPosition(events.get(1), UINT32_RANGE - 100, UINT32_RANGE);
        assertPosition(events.get(2), UINT32_RANGE, UINT32_RANGE + 50);
    }

    @Test
    public void testGtidResumeDoesNotCountOnlyReceivedBytes() throws Exception {
        BinaryLogClient client = client();
        client.setGtidSet("24bc7850-2c16-11e6-a073-0242ac110002:1-7");
        List<Event> events = read(client, rotate(4), formatDescription(0),
            normal(100, 1307), normal(50, 2560));

        // GTID filtering omits bytes, both before the first event and between transactions.
        assertPosition(events.get(2), 1207, 1307);
        assertPosition(events.get(3), 2510, 2560);
        assertEquals(client.getBinlogPosition(), 2560L);
    }

    @Test
    public void testHeartbeatV2AnchorsSkippedRangeLargerThanFourGiB() throws Exception {
        BinaryLogClient client = client();
        long skippedEnd = 2 * UINT32_RANGE + 1207;
        List<Event> events = read(client, rotate(4), formatDescription(0),
            heartbeatV2(skippedEnd), normal(100, 1307));

        assertEquals(header(events.get(2)).getNextPosition(), skippedEnd);
        assertPosition(events.get(3), skippedEnd, skippedEnd + 100);
        assertEquals(client.getBinlogPosition(), skippedEnd + 100);
    }

    @Test
    public void testHeartbeatV2ReportsFullPositionAtExactWrapBoundary() throws Exception {
        BinaryLogClient client = client();
        List<Event> events = read(client, rotate(4), heartbeatV2(2 * UINT32_RANGE), normal(100, 100));

        assertEquals(header(events.get(1)).getNextPosition(), 2 * UINT32_RANGE);
        assertPosition(events.get(2), 2 * UINT32_RANGE, 2 * UINT32_RANGE + 100);
    }

    @Test
    public void testHeartbeatV2AtExactWrapBoundaryUpdatesClientResumePosition() throws Exception {
        BinaryLogClient client = client();
        List<Event> events = read(client, rotate(4), heartbeatV2(2 * UINT32_RANGE));

        assertEquals(header(events.get(1)).getNextPosition(), 2 * UINT32_RANGE);
        assertEquals(client.getBinlogPosition(), 2 * UINT32_RANGE,
            "The full payload anchor must update the public checkpoint without another file event");
    }

    @DataProvider(name = "missingHeartbeatPosition")
    public Object[][] missingHeartbeatPosition() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "missingHeartbeatPosition")
    public void testHeartbeatV2FallsBackToNonzeroHeaderWithoutConsumingBytes(boolean explicitZero) throws Exception {
        BinaryLogClient client = client();
        long start = UINT32_RANGE + 100;
        List<Event> events = read(client, rotate(start), heartbeatV2WithoutPosition(123, explicitZero));

        assertEquals(header(events.get(1)).getNextPosition(), UINT32_RANGE + 123);
        assertEquals(client.getBinlogPosition(), UINT32_RANGE + 123);
    }

    @Test
    public void testArtificialHeartbeatV2WithoutPayloadPositionStillReportsItsHeaderPosition() throws Exception {
        BinaryLogClient client = client();
        List<Event> events = read(client, rotate(UINT32_RANGE + 100), heartbeatV2WithoutPosition(123, false, 0x20));

        assertEquals(header(events.get(1)).getFlags(), 0x20);
        assertEquals(header(events.get(1)).getNextPosition(), UINT32_RANGE + 123);
        assertEquals(client.getBinlogPosition(), UINT32_RANGE + 123);
    }

    @Test(dataProvider = "missingHeartbeatPosition")
    public void testHeartbeatV2WithoutPayloadOrHeaderAnchorPreservesPosition(boolean explicitZero) throws Exception {
        BinaryLogClient client = client();
        long start = UINT32_RANGE - 100;
        List<Event> events = read(client, rotate(start), heartbeatV2WithoutPosition(0, explicitZero),
            normal(50, start + 50));

        assertEquals(header(events.get(1)).getNextPosition(), 0L);
        assertPosition(events.get(2), start, start + 50);
        assertEquals(client.getBinlogPosition(), start + 50);
    }

    @DataProvider(name = "failingAnchorDecoders")
    public Object[][] failingAnchorDecoders() {
        return new Object[][] {
            {EventType.HEARTBEAT_V2, "io"},
            {EventType.HEARTBEAT_V2, "runtime"},
            {EventType.HEARTBEAT_V2, "eof"},
            {EventType.HEARTBEAT_V2, "socket"},
            {EventType.HEARTBEAT_V2, "timeout"},
            {EventType.ROTATE, "io"},
            {EventType.ROTATE, "runtime"},
            {EventType.ROTATE, "eof"},
            {EventType.ROTATE, "socket"},
            {EventType.ROTATE, "timeout"}
        };
    }

    @Test(dataProvider = "failingAnchorDecoders")
    public void testFailedCustomAnchorDecoderStopsBeforeSubsequentEvents(EventType eventType, String failureKind)
            throws Exception {
        BinaryLogClient client = client();
        Exception failure = switch (failureKind) {
            case "runtime" -> new IllegalStateException("custom anchor decoder failed");
            case "eof" -> new EOFException("custom anchor decoder failed");
            case "socket" -> new SocketException("custom anchor decoder failed");
            case "timeout" -> new SocketTimeoutException("custom anchor decoder failed");
            default -> new IOException("custom anchor decoder failed");
        };
        boolean[] firstRotateSeen = {false};
        EventDeserializer deserializer = new EventDeserializer();
        deserializer.setEventDataDeserializer(eventType, input -> {
            if (eventType == EventType.ROTATE && !firstRotateSeen[0]) {
                firstRotateSeen[0] = true;
                return null;
            }
            if (failure instanceof IOException) {
                throw (IOException) failure;
            }
            throw (RuntimeException) failure;
        });
        client.setEventDeserializer(deserializer);
        if (eventType == EventType.ROTATE) {
            client.ensureEventDataDeserializer(eventType, RotateEventDataDeserializer.class);
        } else {
            client.ensureEventDataDeserializer(eventType, HeartbeatV2EventDataDeserializer.class);
        }
        long start = UINT32_RANGE + 100;
        byte[] failedAnchor = eventType == EventType.ROTATE ? rotate("mysql-bin.000002", 4) :
            heartbeatV2(2 * UINT32_RANGE + 1207);
        PacketReadResult result;
        ExecutorService keepAliveExecutor = installKeepAliveExecutor(client);
        try {
            result = readResult(client, rotate(start), failedAnchor, normal(100, 1307));
            assertTrue(keepAliveExecutor.isShutdown(), "A deterministic decoder failure must stop automatic retries");
        } finally {
            keepAliveExecutor.shutdownNow();
        }

        assertEquals(result.events().size(), 1, "An unreadable anchor must prevent delivery of later file events");
        assertEquals(result.notifications(), List.of("deserialization", "communication"));
        assertEquals(result.deserializationFailures().size(), 1);
        assertEquals(result.communicationFailures().size(), 1);
        assertSame(result.communicationFailures().get(0), result.deserializationFailures().get(0));
        assertTrue(((EventDataDeserializationException) result.deserializationFailures().get(0))
            .isPositionAnchorDecodeFailure());
        Throwable cause = result.deserializationFailures().get(0).getCause();
        if ("runtime".equals(failureKind)) {
            assertTrue(cause instanceof IOException, "Present anchor failures through the existing checked error contract");
            assertSame(cause.getCause(), failure);
        } else {
            assertSame(cause, failure);
        }
        assertEquals(client.getBinlogFilename(), FILE);
        assertEquals(client.getBinlogPosition(), start, "Keep the last safe filename/position for reconnect");
        assertFalse(client.isConnected());
    }

    @Test
    public void testFileReaderRetainsCustomDecoderExceptionAfterMySqlStreaming() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        EventDeserializer deserializer = new EventDeserializer() {
            @Override
            public Event nextEvent(ByteArrayInputStream input) throws IOException {
                calls.incrementAndGet();
                return super.nextEvent(input);
            }
        };
        IllegalStateException decoderFailure = new IllegalStateException("invalid rotate");
        deserializer.setEventDataDeserializer(EventType.ROTATE, input -> { throw decoderFailure; });
        BinaryLogClient client = client();
        client.setEventDeserializer(deserializer);
        client.ensureEventDataDeserializer(EventType.ROTATE, RotateEventDataDeserializer.class);
        PacketReadResult result = readResult(client, rotate(4));
        assertEquals(result.notifications(), List.of("deserialization", "communication"));
        assertTrue(((EventDataDeserializationException) result.communicationFailures().get(0)).isPositionAnchorDecodeFailure());

        ByteArrayOutputStream file = new ByteArrayOutputStream();
        file.write(BinaryLogFileReader.MAGIC_HEADER);
        file.write(rawEvent(EventType.ROTATE, 123, 0, rotateBody(FILE, 4)));
        try (BinaryLogFileReader reader = new BinaryLogFileReader(
                new java.io.ByteArrayInputStream(file.toByteArray()), deserializer)) {
            assertSame(expectThrows(IllegalStateException.class, reader::readEvent), decoderFailure);
        }
        assertEquals(calls.get(), 2, "Both the client and the file reader must call the configured override");
    }

    @Test
    public void testInvalidPositionAnchorLengthIsPermanent() throws Exception {
        byte[] event = packet(EventType.ROTATE, 0, 0, new byte[0]);
        // Declared event length is shorter than the v4 common header.
        event[5 + 9] = 18;
        PacketReadResult result = readResult(client(), event);
        assertEquals(result.notifications(), List.of("deserialization", "communication"));
        EventDataDeserializationException failure = (EventDataDeserializationException) result.communicationFailures().get(0);
        assertTrue(failure.isPositionAnchorDecodeFailure());
        assertFalse(failure.isPositionAnchorReadFailure());
    }

    @Test
    public void testThrowingListenerDoesNotHideTerminalPositionFailure() throws Exception {
        BinaryLogClient client = client();
        client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
            @Override
            public void onEventDeserializationFailure(BinaryLogClient source, Exception exception) {
                throw new IllegalStateException("observer failed");
            }
        });
        IOException decoderFailure = new IOException("invalid heartbeat");
        EventDeserializer deserializer = new EventDeserializer();
        deserializer.setEventDataDeserializer(EventType.HEARTBEAT_V2, input -> { throw decoderFailure; });
        client.setEventDeserializer(deserializer);
        client.ensureEventDataDeserializer(EventType.HEARTBEAT_V2, HeartbeatV2EventDataDeserializer.class);

        PacketReadResult result = readResult(client, rotate(4), heartbeatV2(UINT32_RANGE), normal(100, 100));
        assertEquals(result.notifications(), List.of("deserialization", "communication"));
        Exception failure = result.deserializationFailures().get(0);
        assertSame(failure.getCause(), decoderFailure);
        assertSame(result.communicationFailures().get(0), failure);
        assertEquals(result.events().size(), 1);
        assertEquals(client.getBinlogPosition(), 4L);
    }

    @Test
    public void testMalformedHeartbeatAnchorStopsBeforeSubsequentEvents() throws Exception {
        BinaryLogClient client = client();
        PacketReadResult result;
        ExecutorService keepAliveExecutor = installKeepAliveExecutor(client);
        try {
            result = readResult(client, rotate(4),
                packet(EventType.HEARTBEAT_V2, 1207, 0, new byte[] {2, 0, 0}), normal(100, 1307));
            assertTrue(keepAliveExecutor.isShutdown());
        } finally {
            keepAliveExecutor.shutdownNow();
        }

        assertEquals(result.events().size(), 1);
        assertEquals(result.notifications(), List.of("deserialization", "communication"));
        assertEquals(result.deserializationFailures().size(), 1);
        assertEquals(result.communicationFailures().size(), 1);
        assertSame(result.communicationFailures().get(0), result.deserializationFailures().get(0));
        assertEquals(client.getBinlogPosition(), 4L);
        assertFalse(client.isConnected());
    }

    @Test
    public void testTruncatedRotateStopsBeforeSubsequentEvents() throws Exception {
        BinaryLogClient client = client();
        PacketReadResult result;
        ExecutorService keepAliveExecutor = installKeepAliveExecutor(client);
        try {
            result = readResult(client, rotate(UINT32_RANGE + 100),
                packet(EventType.ROTATE, 0, 0, new byte[] {4}), normal(100, 1307));
            assertTrue(keepAliveExecutor.isShutdown(), "A complete packet with an incomplete anchor is deterministic");
        } finally {
            keepAliveExecutor.shutdownNow();
        }

        assertEquals(result.events().size(), 1);
        assertEquals(result.notifications(), List.of("deserialization", "communication"));
        assertEquals(result.deserializationFailures().size(), 1);
        assertEquals(result.communicationFailures().size(), 1);
        assertEquals(client.getBinlogPosition(), UINT32_RANGE + 100);
        assertFalse(client.isConnected());
    }

    @Test
    public void testTransportTruncationKeepsAutomaticReconnectEligible() throws Exception {
        BinaryLogClient client = client();
        byte[] complete = packet(EventType.ROTATE, 0, 0, new byte[8]);
        byte[] interrupted = Arrays.copyOf(complete, complete.length - 7);
        ExecutorService keepAliveExecutor = installKeepAliveExecutor(client);
        try {
            PacketReadResult result = readResult(client, rotate(UINT32_RANGE + 100), interrupted);
            assertTrue(result.deserializationFailures().isEmpty(), "Transport EOF keeps its direct communication-failure path");
            assertEquals(result.communicationFailures().size(), 1);
            assertFalse(((EventDataDeserializationException) result.communicationFailures().get(0))
                .isPositionAnchorDecodeFailure());
            assertFalse(keepAliveExecutor.isShutdown(), "An incomplete transport read must remain retryable");
            assertEquals(client.getBinlogPosition(), UINT32_RANGE + 100);
        } finally {
            keepAliveExecutor.shutdownNow();
        }
    }

    @DataProvider(name = "anchorTransportFailures")
    public Object[][] anchorTransportFailures() {
        return new Object[][] {{EventType.ROTATE, false}, {EventType.ROTATE, true},
            {EventType.HEARTBEAT_V2, false}, {EventType.HEARTBEAT_V2, true}};
    }

    @Test(dataProvider = "anchorTransportFailures")
    public void testAnchorTransportTimeoutAndGenericIoRemainRetryable(EventType type, boolean timeout) throws Exception {
        BinaryLogClient client = client();
        IOException transport = timeout ? new SocketTimeoutException("transport stalled") :
            new IOException("transport read failed");
        ByteArrayOutputStream prefix = new ByteArrayOutputStream();
        prefix.write(rotate(UINT32_RANGE + 100));
        byte[] body = new byte[1024];
        byte[] complete = packet(type, 0, 0, body);
        prefix.write(Arrays.copyOf(complete, complete.length - body.length + 1));
        AtomicInteger failedReads = new AtomicInteger();
        InputStream interrupted = new SequenceInputStream(new java.io.ByteArrayInputStream(prefix.toByteArray()), new InputStream() {
            @Override
            public int read() throws IOException {
                if (failedReads.incrementAndGet() == 1) {
                    throw transport;
                }
                throw new IOException("Anchor body read again after transport failure");
            }
        });
        ExecutorService executor = installKeepAliveExecutor(client);
        try {
            PacketReadResult result = readResult(client, interrupted);
            assertEquals(result.notifications(), List.of("communication"));
            EventDataDeserializationException failure = (EventDataDeserializationException) result.communicationFailures().get(0);
            assertSame(failure.getCause(), transport);
            assertEquals(failedReads.get(), 1);
            assertTrue(failure.isPositionAnchorReadFailure());
            assertFalse(failure.isPositionAnchorDecodeFailure());
            assertFalse(executor.isShutdown());
            assertEquals(client.getBinlogPosition(), UINT32_RANGE + 100);
        } finally {
            executor.shutdownNow();
        }
    }

    @DataProvider(name = "checksumDecoderFailure")
    public Object[][] checksumDecoderFailure() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "checksumDecoderFailure")
    public void testIncompleteAnchorChecksumRemainsTransportFailure(boolean decoderFails) throws Exception {
        BinaryLogClient client = client();
        long start = UINT32_RANGE + 100;
        client.setBinlogPosition(start);
        EventDeserializer deserializer = new EventDeserializer();
        deserializer.setChecksumType(ChecksumType.CRC32);
        if (decoderFails) {
            deserializer.setEventDataDeserializer(EventType.ROTATE, input -> {
                throw new IOException("custom decoder failed before checksum arrived");
            });
        }
        client.setEventDeserializer(deserializer);
        client.ensureEventDataDeserializer(EventType.ROTATE, RotateEventDataDeserializer.class);
        byte[] complete = packet(EventType.ROTATE, 0, 0, new byte[] {4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        ExecutorService executor = installKeepAliveExecutor(client);
        try {
            PacketReadResult result = readResult(client, Arrays.copyOf(complete, complete.length - 1));
            assertTrue(result.deserializationFailures().isEmpty());
            assertEquals(result.communicationFailures().size(), 1);
            EventDataDeserializationException failure = (EventDataDeserializationException) result.communicationFailures().get(0);
            assertTrue(failure.getCause() instanceof EOFException);
            assertFalse(failure.isPositionAnchorDecodeFailure());
            assertFalse(executor.isShutdown());
            assertEquals(client.getBinlogPosition(), start);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test(timeOut = 10000)
    public void testOversizedAnchorDeclarationDoesNotAllocateItsDeclaredBody() throws Exception {
        BinaryLogClient client = client();
        byte[] incomplete = packet(EventType.ROTATE, 0, 0, new byte[] {4});
        for (int i = 0; i < 4; i++) {
            incomplete[14 + i] = (byte) (Integer.MAX_VALUE >>> (8 * i));
        }
        ExecutorService executor = installKeepAliveExecutor(client);
        try {
            PacketReadResult result = readResult(client, incomplete);
            assertEquals(result.communicationFailures().size(), 1);
            assertTrue(result.communicationFailures().get(0).getCause() instanceof EOFException);
            assertFalse(executor.isShutdown());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testWorkerCanceledReaderStillReportsTerminalAnchorFailure() throws Exception {
        BinaryLogClient client = client();
        EventDeserializer deserializer = new EventDeserializer();
        deserializer.setEventDataDeserializer(EventType.HEARTBEAT_V2, input -> {
            client.connected = false; // A live keepalive worker has canceled the old channel first.
            throw new IOException("custom decoder failed after worker cancellation");
        });
        client.setEventDeserializer(deserializer);
        client.ensureEventDataDeserializer(EventType.HEARTBEAT_V2, HeartbeatV2EventDataDeserializer.class);
        ExecutorService executor = installKeepAliveExecutor(client);
        try {
            PacketReadResult result = readResult(client, rotate(4), heartbeatV2(2 * UINT32_RANGE + 1207));
            assertEquals(result.notifications(), List.of("deserialization", "communication"));
            assertTrue(executor.isShutdown());
            assertEquals(client.getBinlogPosition(), 4L);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testTerminalFailureNotificationsSurviveWorkerShutdownClosingTheChannel() throws Exception {
        BinaryLogClient client = client();
        ExecutorService executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>()) {
            @Override
            public List<Runnable> shutdownNow() {
                client.connected = false;
                return super.shutdownNow();
            }
        };
        installKeepAliveExecutor(client, executor);
        try {
            PacketReadResult result = readResult(client, rotate(4),
                packet(EventType.HEARTBEAT_V2, 1207, 0, new byte[] {2, 0, 0}));
            assertEquals(result.notifications(), List.of("deserialization", "communication"));
            assertTrue(executor.isShutdown());
            assertEquals(client.getBinlogPosition(), 4L);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test(timeOut = 10000)
    public void testFailureCallbackCanDisconnectWhileWorkerNeedsReaderLock() throws Exception {
        BinaryLogClient client = client();
        var field = BinaryLogClient.class.getDeclaredField("connectLock");
        field.setAccessible(true);
        Lock readerLock = (Lock) field.get(client);
        ExecutorService executor = installKeepAliveExecutor(client);
        CountDownLatch workerStarted = new CountDownLatch(1);
        client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
            @Override
            public void onCommunicationFailure(BinaryLogClient source, Exception exception) {
                try {
                    source.disconnect();
                } catch (IOException failure) {
                    throw new IllegalStateException(failure);
                }
            }
        });
        readerLock.lock();
        try {
            executor.submit(() -> {
                workerStarted.countDown();
                readerLock.lock();
                readerLock.unlock();
            });
            assertTrue(workerStarted.await(5, TimeUnit.SECONDS));
            PacketReadResult result = readResult(client, rotate(4),
                packet(EventType.HEARTBEAT_V2, 1207, 0, new byte[] {2, 0, 0}));
            assertEquals(result.notifications(), List.of("deserialization", "communication"));
            assertTrue(executor.isShutdown());
        } finally {
            readerLock.unlock();
            executor.shutdownNow();
        }
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test(timeOut = 10000)
    public void testCancelledKeepAliveDoesNotReconnectAfterTerminatingReader() throws Exception {
        CountDownLatch lostConnectionChecked = new CountDownLatch(1);
        AtomicInteger reconnects = new AtomicInteger();
        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "") {
            @Override
            boolean isConnectionLost() {
                lostConnectionChecked.countDown();
                return true;
            }

            @Override
            public void connect(long timeout) {
                reconnects.incrementAndGet();
            }
        };
        client.setKeepAliveInterval(0);
        var field = BinaryLogClient.class.getDeclaredField("connectLock");
        field.setAccessible(true);
        Lock connectLock = (Lock) field.get(client);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        connectLock.lock();
        try {
            executor.submit(() -> client.runKeepAlive(executor));
            assertTrue(lostConnectionChecked.await(5, TimeUnit.SECONDS));
            // Cancellation can interrupt terminateConnect while it waits for the reader's lock.
            executor.shutdownNow();
        } finally {
            connectLock.unlock();
            executor.shutdownNow();
        }
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(reconnects.get(), 0);
    }

    private static ExecutorService installKeepAliveExecutor(BinaryLogClient client) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        installKeepAliveExecutor(client, executor);
        return executor;
    }

    private static void installKeepAliveExecutor(BinaryLogClient client, ExecutorService executor) throws Exception {
        var field = BinaryLogClient.class.getDeclaredField("keepAliveThreadExecutor");
        field.setAccessible(true);
        field.set(client, executor);
    }

    @DataProvider(name = "mariaDbAnchorFailures")
    public Object[][] mariaDbAnchorFailures() {
        return new Object[][] {{"runtime"}, {"nested_socket"}, {"eof"}, {"socket"}};
    }

    @Test(dataProvider = "mariaDbAnchorFailures")
    public void testMariaDbRetainsCustomRotateFailureClassification(String failureKind) throws Exception {
        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "") {
            @Override
            public Boolean getMariaDB() {
                return true;
            }
        };
        client.setBinlogFilename(FILE);
        Exception failure = switch (failureKind) {
            case "nested_socket" -> new IOException("custom error wrapping a socket cause", new SocketException());
            case "eof" -> new EOFException("custom MariaDB decoder failed");
            case "socket" -> new SocketException("custom MariaDB decoder failed");
            default -> new IllegalStateException("custom MariaDB rotate decoder failed");
        };
        boolean[] firstRotateSeen = {false};
        EventDeserializer deserializer = new EventDeserializer();
        deserializer.setEventDataDeserializer(EventType.ROTATE, input -> {
            if (!firstRotateSeen[0]) {
                firstRotateSeen[0] = true;
                return null;
            }
            if (failure instanceof IOException checked) {
                throw checked;
            }
            throw (RuntimeException) failure;
        });
        client.setEventDeserializer(deserializer);
        client.ensureEventDataDeserializer(EventType.ROTATE, RotateEventDataDeserializer.class);
        ExecutorService executor = installKeepAliveExecutor(client);
        try {
            PacketReadResult result = readResult(client, rotate(4), rotate("mysql-bin.000002", 4), normal(100, 1307));
            if (failure instanceof EOFException || failure instanceof SocketException) {
                assertEquals(result.notifications(), List.of("communication"));
                assertEquals(result.events().size(), 1);
                assertEquals(client.getBinlogPosition(), 4L);
            } else {
                assertEquals(result.notifications(), List.of("deserialization"));
                assertEquals(result.deserializationFailures().size(), 1);
                assertTrue(result.communicationFailures().isEmpty());
                Exception reported = result.deserializationFailures().get(0);
                if (failure instanceof RuntimeException) {
                    assertSame(reported, failure);
                } else {
                    assertSame(reported.getCause(), failure);
                    assertFalse(((EventDataDeserializationException) reported).isPositionAnchorDecodeFailure());
                }
                assertEquals(result.events().size(), 2);
                assertPosition(result.events().get(1), 1207, 1307);
                assertEquals(client.getBinlogPosition(), 1307L);
            }
            assertFalse(executor.isShutdown());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testNonAnchorRuntimeDecoderFailureRemainsRecoverable() throws Exception {
        BinaryLogClient client = client();
        RuntimeException failure = new IllegalStateException("custom row-query decoder failed");
        EventDeserializer deserializer = new EventDeserializer();
        deserializer.setEventDataDeserializer(EventType.ROWS_QUERY, input -> { throw failure; });
        client.setEventDeserializer(deserializer);
        PacketReadResult result = readResult(client, rotate(UINT32_RANGE - 70),
            packet(EventType.ROWS_QUERY, 30, 0, new byte[81]), normal(40, 70));

        assertEquals(result.notifications(), List.of("deserialization"));
        assertEquals(result.deserializationFailures().size(), 1);
        assertSame(result.deserializationFailures().get(0), failure);
        assertTrue(result.communicationFailures().isEmpty());
        assertEquals(result.events().size(), 2);
        assertPosition(result.events().get(1), UINT32_RANGE + 30, UINT32_RANGE + 70);
    }

    @Test
    public void testHeartbeatDoesNotConsumeFileBytes() throws Exception {
        BinaryLogClient client = client();
        List<Event> events = read(client, rotate(UINT32_RANGE + 100),
            packet(EventType.HEARTBEAT, 100, 0, FILE.getBytes(StandardCharsets.UTF_8)), normal(50, 150));

        assertPosition(events.get(2), UINT32_RANGE + 100, UINT32_RANGE + 150);
    }

    @Test
    public void testHeartbeatV1ReportsExactWrapBoundary() throws Exception {
        BinaryLogClient client = client();
        byte[] heartbeat = packet(EventType.HEARTBEAT, 0, 0, FILE.getBytes(StandardCharsets.UTF_8));
        List<Event> events = read(client, rotate(UINT32_RANGE - 96), heartbeat, heartbeat);

        assertEquals(header(events.get(1)).getNextPosition(), UINT32_RANGE);
        assertEquals(header(events.get(2)).getNextPosition(), UINT32_RANGE);
        assertEquals(client.getBinlogPosition(), UINT32_RANGE);
    }

    @Test
    public void testSyntheticEventsDoNotAdvanceFilePosition() throws Exception {
        BinaryLogClient client = client();
        List<Event> events = read(client, rotate(UINT32_RANGE - 40), formatDescription(0),
            packet(EventType.IGNORABLE, 7777, 0x20, new byte[10]), normal(100, 60));

        assertEquals(header(events.get(1)).getNextPosition(), 0L);
        assertEquals(header(events.get(2)).getNextPosition(), 7777L);
        assertPosition(events.get(3), UINT32_RANGE - 40, UINT32_RANGE + 60);
    }

    @Test
    public void testRotateResetsHighBitsForNextFile() throws Exception {
        BinaryLogClient client = client();
        List<Event> events = read(client, rotate(UINT32_RANGE + 100), normal(50, 150),
            rotate(4), normal(100, 104));

        assertPosition(events.get(1), UINT32_RANGE + 100, UINT32_RANGE + 150);
        assertPosition(events.get(3), 4, 104);
        assertEquals(client.getBinlogPosition(), 104L);
    }

    @Test
    public void testGtidFilePositionResumeIgnoresReplayedNonzeroFde() throws Exception {
        BinaryLogClient client = client();
        client.setGtidSet("24bc7850-2c16-11e6-a073-0242ac110002:1-7");
        long start = UINT32_RANGE + 1207;
        List<Event> events = read(client, rotate(start), formatDescription(103), normal(100, 1307));

        assertEquals(header(events.get(1)).getNextPosition(), 103L);
        assertPosition(events.get(2), start, start + 100);
        assertEquals(client.getBinlogPosition(), start + 100);
    }

    @Test
    public void testReplayedFdeDoesNotChangeClientResumePosition() throws Exception {
        BinaryLogClient client = client();
        long start = UINT32_RANGE + 1207;
        read(client, rotate(start), formatDescription(103));

        assertEquals(client.getBinlogPosition(), start);
    }

    @Test
    public void testRealFdeAtFileStartConsumesFileBytes() throws Exception {
        BinaryLogClient client = client();
        List<Event> events = read(client, rotate(4), formatDescription(103), normal(100, 203));

        assertPosition(events.get(1), 4, 103);
        assertPosition(events.get(2), 103, 203);
    }

    @Test
    public void testRealStopUpdatesResumePosition() throws Exception {
        BinaryLogClient client = client();
        List<Event> events = read(client, rotate(4), formatDescription(103), packet(EventType.STOP, 122, 0, new byte[0]));

        assertPosition(events.get(2), 103, 122);
        assertEquals(client.getBinlogPosition(), 122L);
    }

    @Test
    public void testRealStopEndingAtFourGiBUpdatesHeaderAndResumePosition() throws Exception {
        BinaryLogClient client = client();
        long stopStart = UINT32_RANGE - 19;
        List<Event> events = read(client, rotate(stopStart), packet(EventType.STOP, 0, 0, new byte[0]));

        assertPosition(events.get(1), stopStart, UINT32_RANGE);
        assertEquals(client.getBinlogPosition(), UINT32_RANGE);
    }

    @Test
    public void testOldFileStopDoesNotAdvancePositionAfterRotation() throws Exception {
        BinaryLogClient client = client();
        String nextFile = "mysql-bin.000002";
        read(client, rotate(UINT32_RANGE + 100), fileRotate(nextFile, UINT32_RANGE + 143),
            packet(EventType.STOP, UINT32_RANGE + 162, 0, new byte[0]));

        assertEquals(client.getBinlogFilename(), nextFile);
        assertEquals(client.getBinlogPosition(), 4L);
    }

    @DataProvider(name = "rotationEndPositions")
    public Object[][] rotationEndPositions() {
        return new Object[][] {{UINT32_RANGE}, {UINT32_RANGE + 500}};
    }

    @Test(dataProvider = "rotationEndPositions")
    public void testRealRotateHeaderBelongsToOldFileAndResumePositionToNewFile(long rotateEnd) throws Exception {
        BinaryLogClient client = client();
        String nextFile = "mysql-bin.000002";
        int rotateLength = 19 + 8 + nextFile.length();
        long rotateStart = rotateEnd - rotateLength;
        List<Event> events = read(client, rotate(rotateStart), fileRotate(nextFile, rotateEnd));

        assertEquals(header(events.get(0)).getNextPosition(), 0L, "The initial artificial rotation keeps its zero header");
        assertPosition(events.get(1), rotateStart, rotateEnd);
        assertEquals(client.getBinlogFilename(), nextFile);
        assertEquals(client.getBinlogPosition(), 4L);
    }

    @Test
    public void testTableMapConsumesBytesWithoutAdvancingClientResumePosition() throws Exception {
        BinaryLogClient client = client();
        ByteArrayOutputStream tableMap = new ByteArrayOutputStream();
        tableMap.writeLong(1, 6);
        tableMap.writeInteger(0, 2);
        tableMap.write(2);
        tableMap.writeZeroTerminatedString("db");
        tableMap.write(1);
        tableMap.writeZeroTerminatedString("t");
        tableMap.write(new byte[] { 1, 3, 0, 0 }); // One LONG column, no metadata, not nullable.
        byte[] tableMapBody = tableMap.toByteArray();
        int tableMapLength = 19 + tableMapBody.length;
        List<Long> resumePositions = new ArrayList<>();
        client.registerEventListener(event -> resumePositions.add(client.getBinlogPosition()));
        List<Event> events = read(client, rotate(UINT32_RANGE - tableMapLength),
            packet(EventType.TABLE_MAP, 0, 0, tableMapBody), normal(50, 50));

        assertPosition(events.get(1), UINT32_RANGE - tableMapLength, UINT32_RANGE);
        assertPosition(events.get(2), UINT32_RANGE, UINT32_RANGE + 50);
        assertEquals(resumePositions.get(2).longValue(), UINT32_RANGE - tableMapLength);
    }

    @Test
    public void testDeserializationFailureDoesNotLoseFileProgress() throws Exception {
        BinaryLogClient client = client();
        List<Exception> failures = new ArrayList<>();
        client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
            @Override
            public void onEventDeserializationFailure(BinaryLogClient source, Exception exception) {
                failures.add(exception);
            }
        });
        EventDeserializer deserializer = new EventDeserializer();
        deserializer.setEventDataDeserializer(EventType.ROWS_QUERY,
            input -> { throw new IOException("invalid event body"); });
        client.setEventDeserializer(deserializer);
        List<Event> events = read(client, rotate(UINT32_RANGE - 70),
            packet(EventType.ROWS_QUERY, 30, 0, new byte[81]), normal(40, 70));

        assertEquals(failures.size(), 1);
        assertEquals(events.size(), 2);
        assertPosition(events.get(1), UINT32_RANGE + 30, UINT32_RANGE + 70);
    }

    @Test
    public void testCompressedTransactionCountsOnlyOuterBytes() throws Exception {
        BinaryLogClient client = client();
        byte[] inner = rawEvent(EventType.XID, 0, 0, new byte[8]);
        byte[] compressed = Zstd.compress(inner);
        assertTrue(compressed.length < 251);
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        payload.write(new byte[] { 1, 1, (byte) compressed.length, 2, 1, 0, 3, 1, (byte) inner.length, 0 });
        payload.write(compressed);
        byte[] body = payload.toByteArray();
        long start = UINT32_RANGE - 10;
        long end = start + 19 + body.length;
        List<Event> events = read(client, rotate(start),
            packet(EventType.TRANSACTION_PAYLOAD, end, 0, body), normal(50, end + 50));

        assertPosition(events.get(1), start, end);
        TransactionPayloadEventData data = events.get(1).getData();
        assertEquals(header(data.getUncompressedEvents().get(0)).getNextPosition(), 0L);
        assertPosition(events.get(2), end, end + 50);
    }

    @Test
    public void testIncompleteTransactionRewindsToFullPosition() throws Exception {
        BinaryLogClient client = client();
        long start = UINT32_RANGE + 1207;
        byte[] begin = query("BEGIN");
        long end = start + 19 + begin.length;
        read(client, rotate(start), packet(EventType.QUERY, end, 0, begin), normal(50, end + 50));
        client.rewindToTransactionStartIfNeeded();

        assertEquals(client.getBinlogPosition(), start);
    }

    @Test
    public void testMariaDbPositionsRemainServerDefined() throws Exception {
        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "") {
            @Override
            public Boolean getMariaDB() {
                return true;
            }
        };
        client.setBinlogFilename(FILE);
        List<Event> events = read(client, rotate(4), formatDescription(0),
            normal(100, 1307), normal(50, 0), normal(50, 2560));

        assertPosition(events.get(2), 1207, 1307);
        assertEquals(header(events.get(3)).getNextPosition(), 0L);
        assertPosition(events.get(4), 2510, 2560);
    }

    @Test
    public void testListenerChangesToEventPositionDoNotChangeMySqlCheckpoint() throws Exception {
        BinaryLogClient client = client();
        long start = UINT32_RANGE - 10;
        long dataEnd = start + 20;
        long heartbeatEnd = dataEnd + 100;
        long heartbeatV2End = 2 * UINT32_RANGE + 140;
        client.setBinlogPosition(start);

        List<Long> eventPositions = new ArrayList<>();
        List<Long> checkpointsDuringCallback = new ArrayList<>();
        client.registerEventListener(event -> {
            eventPositions.add(header(event).getNextPosition());
            checkpointsDuringCallback.add(client.getBinlogPosition());
            header(event).setNextPosition(123);
        });
        read(client, normal(20, dataEnd), packet(EventType.HEARTBEAT, heartbeatEnd, 0, FILE.getBytes(StandardCharsets.UTF_8)),
            heartbeatV2(heartbeatV2End));

        assertEquals(eventPositions, List.of(dataEnd, heartbeatEnd, heartbeatV2End));
        assertEquals(checkpointsDuringCallback, List.of(start, dataEnd, heartbeatEnd));
        assertEquals(client.getBinlogPosition(), heartbeatV2End);
    }

    private static BinaryLogClient client() {
        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "");
        client.setBinlogFilename(FILE);
        return client;
    }

    private static List<Event> read(BinaryLogClient client, byte[]... packets) throws Exception {
        PacketReadResult result = readResult(client, packets);
        assertTrue(result.communicationFailures().isEmpty(), result.communicationFailures().toString());
        return result.events();
    }

    private static PacketReadResult readResult(BinaryLogClient client, byte[]... packets) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        for (byte[] packet : packets) {
            stream.write(packet);
        }
        return readResult(client, new java.io.ByteArrayInputStream(stream.toByteArray()));
    }

    private static PacketReadResult readResult(BinaryLogClient client, InputStream inputStream) throws Exception {
        client.channel = new PacketChannel(new Socket() {
            @Override
            public InputStream getInputStream() {
                return inputStream;
            }

            @Override
            public OutputStream getOutputStream() {
                return OutputStream.nullOutputStream();
            }
        });
        List<Event> events = new ArrayList<>();
        List<Exception> deserializationFailures = new ArrayList<>();
        List<Exception> communicationFailures = new ArrayList<>();
        List<String> notifications = new ArrayList<>();
        client.registerEventListener(events::add);
        client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
            @Override
            public void onEventDeserializationFailure(BinaryLogClient source, Exception exception) {
                deserializationFailures.add(exception);
                notifications.add("deserialization");
            }

            @Override
            public void onCommunicationFailure(BinaryLogClient source, Exception exception) {
                communicationFailures.add(exception);
                notifications.add("communication");
            }
        });
        client.connected = true;
        client.listenForEventPackets();
        return new PacketReadResult(events, deserializationFailures, communicationFailures, notifications);
    }

    private static byte[] rotate(long position) throws IOException {
        return rotate(FILE, position);
    }

    private static byte[] rotate(String filename, long position) throws IOException {
        return packet(EventType.ROTATE, 0, 0x20, rotateBody(filename, position));
    }

    private static byte[] fileRotate(String nextFile, long endPosition) throws IOException {
        return packet(EventType.ROTATE, endPosition, 0, rotateBody(nextFile, 4));
    }

    private static byte[] rotateBody(String filename, long position) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.writeLong(position, 8);
        body.writeString(filename);
        return body.toByteArray();
    }

    private static byte[] normal(int length, long nextPosition) throws IOException {
        return packet(EventType.IGNORABLE, nextPosition, 0, new byte[length - 19]);
    }

    private static byte[] formatDescription(long position) throws IOException {
        byte[] body = new byte[80];
        body[0] = 4;
        byte[] version = "8.4.0".getBytes(StandardCharsets.UTF_8);
        System.arraycopy(version, 0, body, 2, version.length);
        body[56] = 19;
        body[71] = (byte) body.length;
        return packet(EventType.FORMAT_DESCRIPTION, position, 0, body);
    }

    private static byte[] heartbeatV2(long position) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        byte[] filename = FILE.getBytes(StandardCharsets.UTF_8);
        body.write(1); // Filename TLV, followed by a length-encoded 64-bit position TLV.
        body.write(filename.length);
        body.write(filename);
        body.write(new byte[] { 2, 9, (byte) 254 });
        body.writeLong(position, 8);
        body.write(0);
        return packet(EventType.HEARTBEAT_V2, position, 0, body.toByteArray());
    }

    private static byte[] heartbeatV2WithoutPosition(long headerPosition, boolean explicitZero) throws IOException {
        return heartbeatV2WithoutPosition(headerPosition, explicitZero, 0);
    }

    private static byte[] heartbeatV2WithoutPosition(long headerPosition, boolean explicitZero, int flags) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        byte[] filename = FILE.getBytes(StandardCharsets.UTF_8);
        body.write(1);
        body.write(filename.length);
        body.write(filename);
        if (explicitZero) {
            body.write(new byte[] {2, 1, 0});
        }
        body.write(0);
        return packet(EventType.HEARTBEAT_V2, headerPosition, flags, body.toByteArray());
    }

    private static byte[] query(String sql) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(new byte[13]); // QUERY post-header with an empty database and status variables.
        body.write(0);
        body.writeString(sql);
        return body.toByteArray();
    }

    private static byte[] packet(EventType type, long nextPosition, int flags, byte[] body) throws IOException {
        byte[] event = rawEvent(type, nextPosition, flags, body);
        ByteArrayOutputStream packet = new ByteArrayOutputStream();
        packet.writeInteger(event.length + 1, 3);
        packet.write(0); // Sequence number.
        packet.write(0); // Replication event marker.
        packet.write(event);
        return packet.toByteArray();
    }

    private static byte[] rawEvent(EventType type, long nextPosition, int flags, byte[] body) throws IOException {
        ByteArrayOutputStream event = new ByteArrayOutputStream();
        // MySQL supplies timestamps for on-disk events. Artificial rotates and heartbeats have timestamp zero.
        boolean synthetic = (flags & 0x20) != 0 || type == EventType.HEARTBEAT || type == EventType.HEARTBEAT_V2;
        event.writeInteger(synthetic ? 0 : 1, 4);
        event.write(type.ordinal());
        event.writeInteger(1, 4);
        event.writeInteger(19 + body.length, 4);
        event.writeLong(nextPosition, 4);
        event.writeInteger(flags, 2);
        event.write(body);
        return event.toByteArray();
    }

    private static EventHeaderV4 header(Event event) {
        return event.getHeader();
    }

    private static void assertPosition(Event event, long start, long end) {
        assertEquals(header(event).getPosition(), start, "event start");
        assertEquals(header(event).getNextPosition(), end, "event end");
    }

    private record PacketReadResult(List<Event> events, List<Exception> deserializationFailures,
                                    List<Exception> communicationFailures, List<String> notifications) {
    }
}
