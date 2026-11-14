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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Exercises the real connection and lifecycle callbacks with a scripted greeting and authentication response.
 * The socket blocks after authentication until it is closed, without opening a network connection.
 */
public class BinaryLogClientLifecycleTest {

    @DataProvider(name = "callbackFailures")
    public Object[][] callbackFailures() {
        return new Object[][]{
            {new IllegalStateException("onConnect failed")},
            {new AssertionError("onConnect failed")}
        };
    }

    @DataProvider(name = "keepAliveChanges")
    public Object[][] keepAliveChanges() {
        return new Object[][]{
            {true, false},
            {false, true}
        };
    }

    @Test(dataProvider = "callbackFailures", timeOut = 15000)
    public void callbackFailureFromDirectConnectClosesItsResources(Throwable failure) throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.client.registerLifecycleListener(failingListener(failure));
            Throwable observed = null;
            try {
                fixture.client.connect();
            } catch (Throwable e) {
                observed = e;
            }

            assertSame(observed, failure, "direct connect must preserve the callback failure");
            fixture.assertStopped();
            assertEquals(fixture.sockets.size(), 1);
            assertTrue(fixture.sockets.get(0).isClosed(), "the failed connection must be closed");
        }
    }

    @Test(timeOut = 15000)
    public void callbackFailureFromTimedConnectClosesItsResources() throws Exception {
        try (Fixture fixture = new Fixture()) {
            final IllegalStateException failure = new IllegalStateException("onConnect failed");
            fixture.client.registerLifecycleListener(failingListener(failure));
            Throwable observed = null;
            try {
                fixture.client.connect(5000);
            } catch (Throwable e) {
                observed = e;
            }

            assertNotNull(observed, "timed connect must report the callback failure");
            assertFalse(observed instanceof TimeoutException, "callback failure must not become a timeout");
            assertTrue(hasCause(observed, failure), "the original callback failure must be retained");
            fixture.assertStopped();
            assertEquals(fixture.sockets.size(), 1);
            assertTrue(fixture.sockets.get(0).isClosed(), "the failed connection must be closed");
        }
    }

    @Test(dataProvider = "callbackFailures", timeOut = 15000)
    public void callbackFailureStopsThePreparedWorkerWithoutStartingItsLoop(Throwable failure) throws Exception {
        try (Fixture fixture = new Fixture()) {
            final AtomicReference<ThreadPoolExecutor> prepared = new AtomicReference<>();
            fixture.client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
                @Override
                public void onConnect(BinaryLogClient client) {
                    prepared.set(fixture.assertPreparedWorker());
                    fixture.allowReconnectAttempts(1);
                    failingListener(failure).onConnect(client);
                }
            });

            Throwable observed = null;
            try {
                fixture.client.connect();
            } catch (Throwable e) {
                observed = e;
            }

            assertSame(observed, failure);
            fixture.assertStopped();
            assertEquals(fixture.sockets.size(), 1);
            assertTrue(fixture.sockets.get(0).isClosed());
            assertEquals(fixture.connectionChecks.get(), 0, "startup failure must not run the keepalive loop");
            assertEquals(fixture.reconnectAttempts.availablePermits(), 1);
            // Once the prepared worker has terminated, its startup scheduling cannot affect this count.
            assertEquals(prepared.get().getTaskCount(), 0L, "no task should have been submitted");
        }
    }

    @Test(timeOut = 15000)
    public void preparedWorkerStartsItsLoopAfterTheCallbacksComplete() throws Exception {
        try (Fixture fixture = new Fixture()) {
            final AtomicReference<ThreadPoolExecutor> prepared = new AtomicReference<>();
            fixture.client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
                @Override
                public void onConnect(BinaryLogClient client) {
                    prepared.set(fixture.assertPreparedWorker());
                }
            });

            fixture.client.connect(5000);
            await(fixture.sockets.get(0).input.readWaiting, "the reader must finish startup");
            await(fixture.connectionCheckEntered, "the prepared worker must start its keepalive loop");

            assertSame(fixture.keepAliveExecutor(), prepared.get());
            assertEquals(prepared.get().getTaskCount(), 1L, "startup must submit one keepalive task");
            assertEquals(fixture.connectionChecks.get(), 1);
            fixture.client.disconnect();
            fixture.assertStopped();
        }
    }

    @Test(dataProvider = "keepAliveChanges", timeOut = 15000)
    public void callbackCanChangeKeepAliveBeforeMonitoringStarts(boolean initialKeepAlive,
                                                                boolean updatedKeepAlive) throws Exception {
        try (Fixture fixture = new Fixture()) {
            final AtomicReference<ThreadPoolExecutor> prepared = new AtomicReference<>();
            final AtomicReference<Thread> preparedThread = new AtomicReference<>();
            fixture.client.setKeepAlive(initialKeepAlive);
            fixture.client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
                @Override
                public void onConnect(BinaryLogClient client) {
                    if (initialKeepAlive) {
                        prepared.set(fixture.assertPreparedWorker());
                        preparedThread.set(fixture.keepAliveThread());
                    } else {
                        assertNull(fixture.keepAliveExecutor());
                        assertNull(fixture.keepAliveThread());
                    }
                    client.setKeepAlive(updatedKeepAlive);
                }
            });

            fixture.client.connect(5000);
            await(fixture.sockets.get(0).input.readWaiting, "the reader must apply the callback's keepalive setting");
            assertEquals(fixture.client.isKeepAlive(), updatedKeepAlive);
            assertTrue(fixture.client.isConnected());
            if (updatedKeepAlive) {
                await(fixture.connectionCheckEntered, "enabling keepalive in onConnect must start monitoring");
                assertTrue(fixture.client.isKeepAliveThreadRunning());
                assertEquals(fixture.connectionChecks.get(), 1);
                assertEquals(fixture.keepAliveExecutor().getTaskCount(), 1L);
            } else {
                assertNotNull(preparedThread.get());
                preparedThread.get().join(5000);
                assertFalse(preparedThread.get().isAlive(), "the disabled prepared worker must exit");
                assertFalse(fixture.client.isKeepAliveThreadRunning());
                assertEquals(fixture.connectionChecks.get(), 0);
                assertEquals(prepared.get().getTaskCount(), 0L);
            }
            fixture.client.disconnect();
            fixture.assertStopped();
        }
    }

    @Test(timeOut = 15000)
    public void enablingKeepAliveAfterCallbackDisconnectDoesNotPrepareAWorker() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.client.setKeepAlive(false);
            fixture.client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
                @Override
                public void onConnect(BinaryLogClient client) {
                    try {
                        client.disconnect();
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                    client.setKeepAlive(true);
                }
            });

            fixture.client.connect(5000);
            fixture.assertStopped();
            assertTrue(fixture.client.isKeepAlive());
            assertNull(fixture.keepAliveExecutor(), "a disconnected client must not prepare a keepalive worker");
            assertNull(fixture.keepAliveThread());
            assertEquals(fixture.connectionChecks.get(), 0);
            assertEquals(fixture.sockets.size(), 1);
            assertTrue(fixture.sockets.get(0).isClosed());
        }
    }

    @Test(timeOut = 15000)
    public void threadFactoryFailureWhilePreparingKeepAliveClosesTheConnection() throws Exception {
        try (Fixture fixture = new Fixture()) {
            final IllegalStateException failure = new IllegalStateException("keepalive thread creation failed");
            fixture.threadFactoryFailure.set(failure);
            Throwable observed = null;
            try {
                fixture.client.connect();
            } catch (Throwable e) {
                observed = e;
            }

            assertSame(observed, failure);
            fixture.assertStopped();
            assertEquals(fixture.sockets.size(), 1);
            assertTrue(fixture.sockets.get(0).isClosed());
            assertEquals(fixture.connectionChecks.get(), 0);
        }
    }

    @Test(timeOut = 15000)
    public void nullThreadWhilePreparingKeepAliveClosesTheConnection() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.returnNullThread.set(true);
            Throwable observed = null;
            try {
                fixture.client.connect();
            } catch (Throwable e) {
                observed = e;
            }

            assertTrue(observed instanceof RuntimeException, "a missing keepalive thread must fail startup");
            fixture.assertStopped();
            assertEquals(fixture.sockets.size(), 1);
            assertTrue(fixture.sockets.get(0).isClosed());
            assertEquals(fixture.connectionChecks.get(), 0);
        }
    }

    @Test(timeOut = 15000)
    public void terminatedThreadWhilePreparingKeepAlivePreservesThePreviousResources() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.client.connect(5000);
            await(fixture.sockets.get(0).input.readWaiting, "the initial reader must finish startup");
            fixture.client.disconnect();
            fixture.assertStopped();
            final ThreadPoolExecutor previousExecutor = fixture.keepAliveExecutor();
            final Thread previousThread = fixture.keepAliveThread();
            assertNotNull(previousExecutor);
            assertNotNull(previousThread);
            assertTrue(previousExecutor.isTerminated());
            assertFalse(previousThread.isAlive());

            fixture.returnTerminatedThread.set(true);
            // Avoid failing a connection-timeout watchdog before the keepalive worker is prepared.
            fixture.client.setConnectTimeout(0);
            Throwable observed = null;
            try {
                fixture.client.connect();
            } catch (Throwable e) {
                observed = e;
            }

            assertTrue(observed instanceof IllegalThreadStateException,
                "prestarting an already terminated thread must fail");
            assertSame(fixture.keepAliveExecutor(), previousExecutor);
            assertSame(fixture.keepAliveThread(), previousThread);
            assertEquals(fixture.sockets.size(), 2);
            assertTrue(fixture.sockets.get(1).isClosed(), "failed preparation must close the new connection");
            fixture.assertStopped();
        }
    }

    @Test(dataProvider = "callbackFailures", timeOut = 15000)
    public void disconnectCallbackFailureIsSuppressedByTheOriginalStartupFailure(Throwable failure) throws Exception {
        try (Fixture fixture = new Fixture()) {
            final IllegalArgumentException cleanupFailure = new IllegalArgumentException("onDisconnect failed");
            fixture.client.registerLifecycleListener(failingListener(failure));
            fixture.client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
                @Override
                public void onDisconnect(BinaryLogClient client) {
                    throw cleanupFailure;
                }
            });

            Throwable observed = null;
            try {
                fixture.client.connect();
            } catch (Throwable e) {
                observed = e;
            }

            assertSame(observed, failure);
            assertEquals(observed.getSuppressed().length, 1);
            assertSame(observed.getSuppressed()[0], cleanupFailure);
            fixture.assertStopped();
            assertTrue(fixture.sockets.get(0).isClosed());
        }
    }

    @Test(timeOut = 15000)
    public void socketCloseFailureIsSuppressedByTheOriginalStartupFailure() throws Exception {
        try (Fixture fixture = new Fixture()) {
            final IllegalStateException failure = new IllegalStateException("onConnect failed");
            final IOException cleanupFailure = new IOException("socket close failed");
            fixture.client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
                @Override
                public void onConnect(BinaryLogClient client) {
                    fixture.sockets.get(0).closeFailure.set(cleanupFailure);
                    throw failure;
                }
            });

            Throwable observed = null;
            try {
                fixture.client.connect();
            } catch (Throwable e) {
                observed = e;
            }

            assertSame(observed, failure);
            assertEquals(observed.getSuppressed().length, 1);
            assertSame(observed.getSuppressed()[0], cleanupFailure);
            fixture.assertStopped();
            assertTrue(fixture.sockets.get(0).isClosed());
        }
    }

    @Test(timeOut = 15000)
    public void reconnectCallbackFailureRetainsTheExistingRetryWorker() throws Exception {
        try (Fixture fixture = new Fixture()) {
            final AtomicInteger connections = new AtomicInteger();
            final CountDownLatch recovered = new CountDownLatch(1);
            fixture.client.setKeepAliveMaxReconnectAttempts(2);
            fixture.client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
                @Override
                public void onConnect(BinaryLogClient client) {
                    int attempt = connections.incrementAndGet();
                    if (attempt == 2) {
                        throw new IllegalStateException("first reconnect failed");
                    }
                    if (attempt == 3) {
                        recovered.countDown();
                    }
                }
            });

            fixture.client.connect(5000);
            fixture.allowReconnectAttempts(2);
            await(recovered, "a callback failure must not silently stop the existing retry worker");
            await(fixture.sockets.get(2).input.readWaiting, "the recovered reader must enter the receive loop");

            assertEquals(connections.get(), 3);
            assertTrue(fixture.client.isConnected());
            assertTrue(fixture.client.isKeepAliveThreadRunning());
            assertEquals(fixture.sockets.size(), 3);
            assertTrue(fixture.sockets.get(0).isClosed());
            assertTrue(fixture.sockets.get(1).isClosed(), "each failed reconnect must close its socket");
            fixture.client.disconnect();
            fixture.assertStopped();
            assertEquals(fixture.directConnectCalls.get(), 3,
                "the initial reader and both reconnect readers must retain public connect() dispatch");
            assertEquals(fixture.timedConnectCalls.get(), 3,
                "keepalive retries must retain public connect(timeout) dispatch");
        }
    }

    @Test(timeOut = 15000)
    public void reconnectCallbackFailuresRespectTheLimitAndReportAbandonment() throws Exception {
        try (Fixture fixture = new Fixture()) {
            final IllegalStateException failure = new IllegalStateException("reconnect callback failed");
            final AtomicInteger connections = new AtomicInteger();
            final AtomicInteger notifications = new AtomicInteger();
            final AtomicInteger failedAttempts = new AtomicInteger();
            final AtomicReference<Throwable> abandonedCause = new AtomicReference<>();
            final CountDownLatch abandoned = new CountDownLatch(1);
            fixture.client.setKeepAliveMaxReconnectAttempts(2);
            fixture.client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
                @Override
                public void onConnect(BinaryLogClient client) {
                    if (connections.incrementAndGet() > 1) {
                        throw failure;
                    }
                }

                @Override
                public void onReconnectAbandoned(BinaryLogClient client, Throwable cause, int attempts) {
                    abandonedCause.set(cause);
                    failedAttempts.set(attempts);
                    notifications.incrementAndGet();
                    abandoned.countDown();
                }
            });

            fixture.client.connect(5000);
            fixture.allowReconnectAttempts(2);
            await(abandoned, "exhausted callback failures must be reported to the consumer");
            fixture.assertStopped();

            assertEquals(connections.get(), 3, "initial connection plus two failed reconnects");
            assertEquals(failedAttempts.get(), 2);
            assertEquals(notifications.get(), 1);
            assertTrue(hasCause(abandonedCause.get(), failure));
            assertEquals(fixture.sockets.size(), 3);
            for (MemorySocket socket : fixture.sockets) {
                assertTrue(socket.isClosed(), "the abandoned client must not retain an open socket");
            }
        }
    }

    @Test(timeOut = 15000)
    public void disconnectRightAfterTimedConnectDoesNotLeaveKeepAliveRunning() throws Exception {
        try (Fixture fixture = new Fixture()) {
            final CountDownLatch callbackEntered = new CountDownLatch(1);
            final CountDownLatch allowCallbackReturn = new CountDownLatch(1);
            final CountDownLatch disconnected = new CountDownLatch(1);
            final AtomicReference<Throwable> disconnectFailure = new AtomicReference<>();
            fixture.afterConnectNotification.set(new BinaryLogClient.AbstractLifecycleListener() {
                @Override
                public void onConnect(BinaryLogClient client) {
                    callbackEntered.countDown();
                    try {
                        await(allowCallbackReturn, "the test must release the onConnect callback");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                }
            });

            try {
                fixture.client.connect(5000);
                await(callbackEntered, "connect(timeout) must return while the later callback is still running");
                Thread disconnector = new Thread(() -> {
                    try {
                        fixture.client.disconnect();
                    } catch (Throwable e) {
                        disconnectFailure.set(e);
                    } finally {
                        disconnected.countDown();
                    }
                }, "lifecycle-test-disconnect");
                disconnector.setDaemon(true);
                fixture.threads.add(disconnector);
                disconnector.start();
                // disconnect() has stopped the current keepalive and closed the socket before it waits for connect().
                await(fixture.sockets.get(0).closedLatch, "disconnect must close the connection before the callback returns");
            } finally {
                allowCallbackReturn.countDown();
            }
            await(disconnected, "disconnect must finish after the callback returns");
            assertNull(disconnectFailure.get());
            fixture.assertStopped();
            assertEquals(fixture.sockets.size(), 1);
            assertTrue(fixture.sockets.get(0).isClosed());
        }
    }

    @Test(timeOut = 15000)
    public void disconnectFromInitialOnConnectDoesNotLeaveKeepAliveRunning() throws Exception {
        try (Fixture fixture = new Fixture()) {
            final CountDownLatch disconnected = new CountDownLatch(1);
            fixture.client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
                @Override
                public void onConnect(BinaryLogClient client) {
                    try {
                        client.disconnect();
                        disconnected.countDown();
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                }
            });

            fixture.client.connect(5000);
            await(disconnected, "onConnect must be able to disconnect the initial connection");
            fixture.assertStopped();
            assertEquals(fixture.sockets.size(), 1);
            assertTrue(fixture.sockets.get(0).isClosed());
        }
    }

    @Test(timeOut = 15000)
    public void reconnectOverrideCanResetTheClientAndStartAFreshConnection() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.resetOnReconnect.set(true);
            final AtomicInteger connections = new AtomicInteger();
            fixture.client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
                @Override
                public void onConnect(BinaryLogClient client) {
                    connections.incrementAndGet();
                }
            });

            fixture.client.connect(5000);
            fixture.allowReconnectAttempts(1);
            await(fixture.resetAttemptDone, "the override must finish starting a fresh connection");
            assertNull(fixture.resetFailure.get());
            assertEquals(fixture.sockets.size(), 2);
            await(fixture.sockets.get(1).input.readWaiting, "the fresh reader must enter the receive loop");
            assertEquals(connections.get(), 2);
            assertTrue(fixture.client.isConnected());
            assertTrue(fixture.client.isKeepAliveThreadRunning());
            assertEquals(fixture.directConnectCalls.get(), 2);
            assertEquals(fixture.timedConnectCalls.get(), 2);
            fixture.client.disconnect();
            fixture.assertStopped();
            for (MemorySocket socket : fixture.sockets) {
                assertTrue(socket.isClosed());
            }
        }
    }

    private static BinaryLogClient.LifecycleListener failingListener(Throwable failure) {
        return new BinaryLogClient.AbstractLifecycleListener() {
            @Override
            public void onConnect(BinaryLogClient client) {
                if (failure instanceof RuntimeException) {
                    throw (RuntimeException) failure;
                }
                throw (Error) failure;
            }
        };
    }

    private static boolean hasCause(Throwable actual, Throwable expected) {
        for (Throwable cause = actual; cause != null; cause = cause.getCause()) {
            if (cause == expected) {
                return true;
            }
        }
        return false;
    }

    private static void await(CountDownLatch latch, String message) throws InterruptedException {
        assertTrue(latch.await(5, TimeUnit.SECONDS), message);
    }

    private static final class Fixture implements AutoCloseable {
        private final List<MemorySocket> sockets = new CopyOnWriteArrayList<>();
        private final List<Thread> threads = new CopyOnWriteArrayList<>();
        private final Semaphore reconnectAttempts = new Semaphore(0);
        private final AtomicInteger connectionChecks = new AtomicInteger();
        private final CountDownLatch connectionCheckEntered = new CountDownLatch(1);
        private final AtomicReference<RuntimeException> threadFactoryFailure = new AtomicReference<>();
        private final AtomicBoolean returnTerminatedThread = new AtomicBoolean();
        private final AtomicBoolean returnNullThread = new AtomicBoolean();
        private final AtomicBoolean resetOnReconnect = new AtomicBoolean();
        private final AtomicReference<Throwable> resetFailure = new AtomicReference<>();
        private final CountDownLatch resetAttemptDone = new CountDownLatch(1);
        private final AtomicReference<BinaryLogClient.LifecycleListener> afterConnectNotification = new AtomicReference<>();
        private final AtomicInteger directConnectCalls = new AtomicInteger();
        private final AtomicInteger timedConnectCalls = new AtomicInteger();
        private final BinaryLogClient client;

        private Fixture() {
            client = new BinaryLogClient("localhost", 3306, "fixture-user", "fixture-password") {
                @Override
                public void connect() throws IOException {
                    directConnectCalls.incrementAndGet();
                    super.connect();
                }

                @Override
                public void connect(long timeout) throws IOException, TimeoutException {
                    int invocation = timedConnectCalls.incrementAndGet();
                    if (resetOnReconnect.get() && invocation == 2) {
                        try {
                            disconnect();
                            // This override deliberately starts a fresh connection after stopping its old worker.
                            Thread.interrupted();
                            super.connect(timeout);
                        } catch (IOException | TimeoutException | RuntimeException e) {
                            resetFailure.set(e);
                            throw e;
                        } finally {
                            resetAttemptDone.countDown();
                        }
                        return;
                    }
                    super.connect(timeout);
                }

                @Override
                protected void setupConnection() {
                }

                @Override
                protected void requestBinaryLogStream() {
                    // connect(timeout) has registered its readiness listener before starting this reader.
                    BinaryLogClient.LifecycleListener listener = afterConnectNotification.getAndSet(null);
                    if (listener != null) {
                        registerLifecycleListener(listener);
                    }
                }

                @Override
                boolean isConnectionLost() {
                    connectionChecks.incrementAndGet();
                    connectionCheckEntered.countDown();
                    try {
                        reconnectAttempts.acquire();
                        return true;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
            };
            client.setBinlogFilename("binlog.000001");
            client.setConnectTimeout(0);
            // Reconnection advances only when the test grants a permit.
            client.setKeepAliveInterval(0);
            client.setSocketFactory(() -> {
                MemorySocket socket = new MemorySocket();
                sockets.add(socket);
                return socket;
            });
            client.setThreadFactory(runnable -> {
                RuntimeException failure = threadFactoryFailure.get();
                if (failure != null) {
                    throw failure;
                }
                if (returnNullThread.get()) {
                    return null;
                }
                boolean terminatedThread = returnTerminatedThread.get();
                Thread thread = new Thread(terminatedThread ? () -> { } : runnable);
                // A failing lifecycle test must not leave a non-daemon thread preventing the suite from exiting.
                thread.setDaemon(true);
                threads.add(thread);
                if (terminatedThread) {
                    thread.start();
                    try {
                        thread.join(5000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("interrupted while preparing the terminated fixture thread", e);
                    }
                    if (thread.isAlive()) {
                        throw new IllegalStateException("fixture thread did not terminate");
                    }
                }
                return thread;
            });
        }

        private void allowReconnectAttempts(int attempts) {
            reconnectAttempts.release(attempts);
        }

        private ThreadPoolExecutor keepAliveExecutor() {
            return (ThreadPoolExecutor) clientField("keepAliveThreadExecutor");
        }

        private Thread keepAliveThread() {
            return (Thread) clientField("keepAliveThread");
        }

        private Object clientField(String name) {
            try {
                Field field = BinaryLogClient.class.getDeclaredField(name);
                field.setAccessible(true);
                return field.get(client);
            } catch (ReflectiveOperationException e) {
                throw new AssertionError("cannot inspect client field " + name, e);
            }
        }

        private ThreadPoolExecutor assertPreparedWorker() {
            ThreadPoolExecutor executor = keepAliveExecutor();
            assertNotNull(executor, "the keepalive executor must be registered before onConnect");
            assertTrue(client.isKeepAliveThreadRunning());
            assertEquals(executor.getPoolSize(), 1, "one idle keepalive worker must be prestarted");
            assertTrue(executor.getQueue().isEmpty(), "startup must not queue the keepalive task yet");
            assertEquals(connectionChecks.get(), 0);
            assertTrue(threads.stream().anyMatch(thread -> thread.getName().startsWith("blc-keepalive-") &&
                thread.isAlive()), "the prepared keepalive thread must be alive during onConnect");
            return executor;
        }

        private void assertStopped() throws InterruptedException {
            long started = System.nanoTime();
            // A joined creator cannot register more children. Re-read size to include children created during join.
            for (int index = 0; index < threads.size(); index++) {
                Thread thread = threads.get(index);
                long remaining = TimeUnit.SECONDS.toNanos(5) - (System.nanoTime() - started);
                if (remaining > 0) {
                    TimeUnit.NANOSECONDS.timedJoin(thread, remaining);
                }
                assertFalse(thread.isAlive(), "client thread still running: " + thread.getName());
            }
            assertFalse(client.isConnected());
            assertFalse(client.isKeepAliveThreadRunning());
        }

        @Override
        public void close() throws InterruptedException {
            final List<Throwable> cleanupFailures = new CopyOnWriteArrayList<>();
            for (MemorySocket socket : sockets) {
                try {
                    socket.close();
                } catch (IOException | RuntimeException e) {
                    cleanupFailures.add(e);
                }
            }
            Thread cleanup = new Thread(() -> {
                try {
                    client.disconnect();
                } catch (Throwable e) {
                    cleanupFailures.add(e);
                }
            }, "lifecycle-test-cleanup");
            cleanup.setDaemon(true);
            cleanup.start();
            cleanup.join(5000);
            if (cleanup.isAlive()) {
                for (Thread thread : threads) {
                    thread.interrupt();
                }
                cleanup.interrupt();
                fail("fixture cleanup did not finish");
            }
            assertStopped();
            if (!cleanupFailures.isEmpty()) {
                AssertionError failure = new AssertionError("fixture cleanup failed");
                for (Throwable cleanupFailure : cleanupFailures) {
                    failure.addSuppressed(cleanupFailure);
                }
                throw failure;
            }
        }
    }

    private static final class MemorySocket extends Socket {
        private final BlockingInputStream input = new BlockingInputStream(greetingAndAuthentication());
        private final OutputStream output = new ByteArrayOutputStream();
        private final AtomicBoolean closed = new AtomicBoolean();
        private final CountDownLatch closedLatch = new CountDownLatch(1);
        private final AtomicReference<IOException> closeFailure = new AtomicReference<>();

        @Override
        public void connect(SocketAddress endpoint, int timeout) {
        }

        @Override
        public InputStream getInputStream() {
            return input;
        }

        @Override
        public OutputStream getOutputStream() {
            return output;
        }

        @Override
        public boolean isClosed() {
            return closed.get();
        }

        @Override
        public void shutdownInput() {
            input.close();
        }

        @Override
        public void shutdownOutput() {
        }

        @Override
        public void close() throws IOException {
            closed.set(true);
            input.close();
            closedLatch.countDown();
            IOException failure = closeFailure.getAndSet(null);
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static final class BlockingInputStream extends InputStream {
        private final ByteArrayInputStream bytes;
        private final CountDownLatch readWaiting = new CountDownLatch(1);
        private boolean closed;

        private BlockingInputStream(byte[] bytes) {
            this.bytes = new ByteArrayInputStream(bytes);
        }

        @Override
        public synchronized int read() throws IOException {
            awaitBytesOrClose();
            return closed ? -1 : bytes.read();
        }

        @Override
        public synchronized int read(byte[] buffer, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            awaitBytesOrClose();
            return closed ? -1 : bytes.read(buffer, offset, length);
        }

        private void awaitBytesOrClose() throws IOException {
            while (bytes.available() == 0 && !closed) {
                readWaiting.countDown();
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("fixture read interrupted", e);
                }
            }
        }

        @Override
        public synchronized void close() {
            closed = true;
            notifyAll();
        }
    }

    private static byte[] greetingAndAuthentication() {
        ByteArrayOutputStream greeting = new ByteArrayOutputStream();
        greeting.write(10);
        greeting.writeBytes("8.0.36".getBytes(StandardCharsets.US_ASCII));
        greeting.write(0);
        greeting.writeBytes(new byte[4]);
        greeting.writeBytes("12345678".getBytes(StandardCharsets.US_ASCII));
        greeting.write(0);
        greeting.writeBytes(new byte[2]);
        greeting.write(33);
        greeting.writeBytes(new byte[2]);
        greeting.writeBytes(new byte[13]);
        greeting.writeBytes("123456789012".getBytes(StandardCharsets.US_ASCII));
        greeting.write(0);
        ByteArrayOutputStream packets = new ByteArrayOutputStream();
        writePacket(packets, greeting.toByteArray(), 0);
        writePacket(packets, new byte[]{0}, 2);
        return packets.toByteArray();
    }

    private static void writePacket(ByteArrayOutputStream output, byte[] payload, int sequence) {
        output.write(payload.length);
        output.write(payload.length >> 8);
        output.write(payload.length >> 16);
        output.write(sequence);
        output.writeBytes(payload);
    }
}
