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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.testng.annotations.Test;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;

public class BinaryLogClientLargePositionStreamTest {

    @Test(timeOut = 30000)
    public void testCleanEofDoesNotSatisfyGoal() throws Exception {
        try (ScriptedClient client = new ScriptedClient(source -> { })) {
            AssertionError failure = expectThrows(AssertionError.class,
                () -> BinaryLogClientLargePositionIntegrationTest.stream(client, event -> false));

            assertTrue(failure.getMessage().contains("terminated before reaching its expected stopping event"));
            assertFalse(client.reader.isAlive());
        }
    }

    @Test(timeOut = 30000)
    public void testConsumerGoalSucceeds() throws Exception {
        try (ScriptedClient client = new ScriptedClient(ScriptedClient::emit)) {
            BinaryLogClientLargePositionIntegrationTest.stream(client, event -> true);

            assertFalse(client.reader.isAlive());
        }
    }

    @Test(timeOut = 30000)
    public void testAssertionSurvivesCleanupFailuresAndReaderIsJoined() throws Exception {
        AssertionError original = new AssertionError("row position mismatch");
        IOException readerCleanup = new IOException("reader cleanup failed");
        IOException callerCleanup = new IOException("caller cleanup failed");
        try (ScriptedClient client = new ScriptedClient(source -> {
            source.emit();
            source.awaitCallerDisconnect();
        })) {
            client.readerCleanupFailure = readerCleanup;
            client.callerCleanupFailure = callerCleanup;
            AssertionError failure = expectThrows(AssertionError.class,
                () -> BinaryLogClientLargePositionIntegrationTest.stream(client, event -> { throw original; }));

            assertSame(failure, original);
            assertEquals(failure.getSuppressed(), new Throwable[] { readerCleanup, callerCleanup });
            assertFalse(client.reader.isAlive());
        }
    }

    @Test(timeOut = 30000)
    public void testConnectFailureSurvivesCleanupFailure() throws Exception {
        IOException original = new IOException("connect failed");
        IOException cleanup = new IOException("caller cleanup failed");
        try (ScriptedClient client = new ScriptedClient(source -> { })) {
            client.connectFailure = original;
            client.callerCleanupFailure = cleanup;
            IOException failure = expectThrows(IOException.class,
                () -> BinaryLogClientLargePositionIntegrationTest.stream(client, event -> false));

            assertSame(failure, original);
            assertEquals(failure.getSuppressed(), new Throwable[] { cleanup });
            assertFalse(client.reader.isAlive());
        }
    }

    private static final class ScriptedClient extends BinaryLogClient implements AutoCloseable {
        private final Consumer<ScriptedClient> onConnect;
        private final CountDownLatch callerDisconnected = new CountDownLatch(1);
        private volatile Thread reader;
        private IOException connectFailure;
        private IOException readerCleanupFailure;
        private IOException callerCleanupFailure;

        private ScriptedClient(Consumer<ScriptedClient> onConnect) {
            super("localhost", 3306, "test", "");
            this.onConnect = onConnect;
        }

        @Override
        public void connect() throws IOException {
            reader = Thread.currentThread();
            if (connectFailure != null) {
                throw connectFailure;
            }
            onConnect.accept(this);
        }

        @Override
        public void disconnect() throws IOException {
            IOException failure = readerCleanupFailure;
            if (Thread.currentThread() != reader) {
                callerDisconnected.countDown();
                failure = callerCleanupFailure;
            }
            if (failure != null) {
                throw failure;
            }
        }

        private void emit() {
            Event event = new Event(new EventHeaderV4(), null);
            for (EventListener listener : getEventListeners()) {
                listener.onEvent(event);
            }
        }

        private void awaitCallerDisconnect() {
            try {
                assertTrue(callerDisconnected.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new AssertionError(error);
            }
        }

        @Override
        public void close() throws InterruptedException {
            callerDisconnected.countDown();
            if (reader != null) {
                reader.join(TimeUnit.SECONDS.toMillis(10));
                assertFalse(reader.isAlive(), "Scripted reader must be released after a test failure");
            }
        }
    }
}
