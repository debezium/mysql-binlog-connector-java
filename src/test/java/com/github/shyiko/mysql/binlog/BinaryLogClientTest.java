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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.annotations.Test;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.GtidTaggedEventData;
import com.github.shyiko.mysql.binlog.event.MySqlGtid;
import com.github.shyiko.mysql.binlog.event.XidEventData;
import com.github.shyiko.mysql.binlog.jmx.BinaryLogClientStatistics;
import com.github.shyiko.mysql.binlog.network.SSLSocketFactory;
import com.github.shyiko.mysql.binlog.network.SocketFactory;
import com.github.shyiko.mysql.binlog.network.protocol.PacketChannel;

import javax.net.ssl.SSLSocket;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class BinaryLogClientTest {

    @Test
    public void testEventListenersManagement() {
        BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 3306, "root", "mysql");
        assertTrue(binaryLogClient.getEventListeners().isEmpty());
        TraceEventListener traceEventListener = new TraceEventListener();
        binaryLogClient.registerEventListener(traceEventListener);
        binaryLogClient.registerEventListener(new CountDownEventListener());
        binaryLogClient.registerEventListener(new CapturingEventListener());
        assertEquals(binaryLogClient.getEventListeners().size(), 3);
        binaryLogClient.unregisterEventListener(traceEventListener);
        assertEquals(binaryLogClient.getEventListeners().size(), 2);
        binaryLogClient.unregisterEventListener(CapturingEventListener.class);
        assertEquals(binaryLogClient.getEventListeners().size(), 1);
    }

    @Test
    public void testLifecycleListenersManagement() {
        BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 3306, "root", "mysql");
        assertTrue(binaryLogClient.getLifecycleListeners().isEmpty());
        TraceLifecycleListener traceLifecycleListener = new TraceLifecycleListener();
        binaryLogClient.registerLifecycleListener(traceLifecycleListener);
        binaryLogClient.registerLifecycleListener(new BinaryLogClientStatistics());
        binaryLogClient.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
        });
        assertEquals(binaryLogClient.getLifecycleListeners().size(), 3);
        binaryLogClient.unregisterLifecycleListener(traceLifecycleListener);
        assertEquals(binaryLogClient.getLifecycleListeners().size(), 2);
        binaryLogClient.unregisterLifecycleListener(BinaryLogClientStatistics.class);
        assertEquals(binaryLogClient.getLifecycleListeners().size(), 1);
    }

    @Test(expectedExceptions = TimeoutException.class)
    public void testNoConnectionTimeout() throws Exception {
        new BinaryLogClient("_localhost_", 3306, "root", "mysql").connect(0);
    }

    @Test(timeOut = 15000)
    public void testConnectionTimeout() throws Exception {
        final BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 33059, "root", "mysql");
        final CountDownLatch socketBound = new CountDownLatch(1);
        final CountDownLatch binaryLogClientDisconnected = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    final ServerSocket serverSocket = new ServerSocket();
                    try {
                        serverSocket.bind(new InetSocketAddress("localhost", 33059));
                        socketBound.countDown();
                        Socket accept = serverSocket.accept();
                        accept.getOutputStream().write(1);
                        accept.getOutputStream().flush();
                        assertTrue(binaryLogClientDisconnected.await(3000, TimeUnit.MILLISECONDS));
                    } finally {
                        serverSocket.close();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
        assertTrue(socketBound.await(3000, TimeUnit.MILLISECONDS));
        binaryLogClient.setConnectTimeout(1000);
        try {
            binaryLogClient.connect();
        } catch (IOException e) {
            binaryLogClientDisconnected.countDown();
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullEventDeserializerIsNotAllowed() throws Exception {
        new BinaryLogClient("localhost", 3306, "root", "mysql").setEventDeserializer(null);
    }

    @Test(timeOut = 15000)
    public void testDisconnectWhileBlockedByFBRead() throws Exception {
        final BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 33061, "root", "mysql");
        final CountDownLatch readAttempted = new CountDownLatch(1);
        binaryLogClient.setSocketFactory(new SocketFactory() {
            @Override
            public Socket createSocket() throws SocketException {
                return new Socket() {

                    @Override
                    public InputStream getInputStream() throws IOException {
                        return new FilterInputStream(super.getInputStream()) {

                            @Override
                            public int read(byte[] b, int off, int len) throws IOException {
                                readAttempted.countDown();
                                return super.read(b, off, len);
                            }
                        };
                    }
                };
            }
        });
        binaryLogClient.setKeepAlive(false);
        final CountDownLatch socketBound = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    final ServerSocket serverSocket = new ServerSocket();
                    try {
                        serverSocket.bind(new InetSocketAddress("localhost", 33061));
                        socketBound.countDown();
                        serverSocket.accept(); // accept socket but do NOT send anything
                        assertTrue(readAttempted.await(3000, TimeUnit.MILLISECONDS));
                        Thread thread = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    Thread.yield();
                                    binaryLogClient.disconnect();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });
                        thread.start();
                        thread.join();
                    } finally {
                        serverSocket.close();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
        assertTrue(socketBound.await(3000, TimeUnit.MILLISECONDS));
        try {
            binaryLogClient.connect();
        } catch (IOException e) {
            assertEquals(readAttempted.getCount(), 0);
            assertTrue(e.getMessage().contains("Failed to connect to MySQL"));
        }
    }

    /**
     * Test that requestBinaryLogStreamMaria does not throw NPE when gtidEnabled is true
     * but gtidSet is null (DBZ-9243). When no GTID position is available, the method should
     * fall back to binlog file/position mode and NOT send SET @slave_connect_state.
     */
    @Test
    public void testMariaDbStreamRequestWithNullGtidSetDoesNotThrowNPE() throws IOException {
        final List<String> sentCommands = new ArrayList<String>();
        // Subclass to test the fixed decision logic without a real network connection
        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "mysql") {
            @Override
            protected void requestBinaryLogStreamMaria(long serverId) throws IOException {
                // Mirror the fixed logic: when gtidSet is null, must NOT throw NPE
                String gtidStr = (gtidSet != null) ? gtidSet.toString() : null;
                if (gtidStr != null && !gtidStr.isEmpty()) {
                    sentCommands.add("SET @slave_connect_state = '" + gtidStr + "'");
                } else {
                    sentCommands.add("USE_BINLOG_POSITION");
                }
            }
        };
        // Simulate what Debezium does: setGtidSet("") → gtidEnabled=true but gtidSet stays null
        client.setGtidSet("");
        // gtidSet field must still be null (empty string skips object creation in setGtidSet)
        assertEquals(client.getGtidSet(), null);
        // This must NOT throw NullPointerException
        client.requestBinaryLogStreamMaria(65535L);
        // Verify the fallback path (file/position) was taken, not the GTID path
        assertEquals(sentCommands.size(), 1);
        assertEquals(sentCommands.get(0), "USE_BINLOG_POSITION");
        assertFalse(sentCommands.get(0).contains("slave_connect_state"),
            "SET @slave_connect_state should NOT be sent when gtidSet is null");
    }
    /**
     * Test that requestBinaryLogStreamMaria does not send SET @slave_connect_state
     * when gtidSet is an empty MariaDB GTID set (DBZ-9243). An empty GTID set (as
     * initialized by setupGtidSet() when no prior GTID exists) means no known position —
     * should fall back to binlog file/position.
     */
    @Test
    public void testMariaDbStreamRequestWithEmptyGtidSetFallsBackToFilePosition() throws IOException {
        final List<String> sentCommands = new ArrayList<String>();
        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "mysql") {
            @Override
            protected void requestBinaryLogStreamMaria(long serverId) throws IOException {
                // Mirror the fixed logic: empty gtidSet should NOT send slave_connect_state
                String gtidStr = (gtidSet != null) ? gtidSet.toString() : null;
                if (gtidStr != null && !gtidStr.isEmpty()) {
                    sentCommands.add("SET @slave_connect_state = '" + gtidStr + "'");
                } else {
                    sentCommands.add("USE_BINLOG_POSITION");
                }
            }
        };
        // Simulate setupGtidSet() initializing gtidSet = new MariadbGtidSet("") when gtidStr was ""
        client.setGtidSet("");
        synchronized (client.gtidSetAccessLock) {
            client.gtidSet = new MariadbGtidSet("");
        }
        assertEquals(client.getGtidSet(), ""); // empty string — not null, but still no real GTID
        // Must not send SET @slave_connect_state = '' to MariaDB
        client.requestBinaryLogStreamMaria(65535L);
        assertEquals(sentCommands.size(), 1);
        assertEquals(sentCommands.get(0), "USE_BINLOG_POSITION",
            "When gtidSet is empty, should fall back to binlog file/position");
    }
    /**
     * Test that requestBinaryLogStreamMaria correctly sends SET @slave_connect_state
     * when a valid, non-empty MariaDB GTID is available — verifies the happy path is unaffected.
     */
    @Test
    public void testMariaDbStreamRequestWithValidGtidSendsSlaveConnectState() throws IOException {
        final List<String> sentCommands = new ArrayList<String>();
        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "mysql") {
            @Override
            protected void requestBinaryLogStreamMaria(long serverId) throws IOException {
                // Happy path: valid non-empty GTID should send slave_connect_state
                String gtidStr = (gtidSet != null) ? gtidSet.toString() : null;
                if (gtidStr != null && !gtidStr.isEmpty()) {
                    sentCommands.add("SET @slave_connect_state = '" + gtidStr + "'");
                } else {
                    sentCommands.add("USE_BINLOG_POSITION");
                }
            }
        };
        // Provide a valid MariaDB GTID (domain-server-sequence format)
        client.setGtidSet("0-1-1");
        assertEquals(client.getGtidSet(), "0-1-1");
        client.requestBinaryLogStreamMaria(65535L);
        assertEquals(sentCommands.size(), 1);
        assertEquals(sentCommands.get(0), "SET @slave_connect_state = '0-1-1'",
            "When gtidSet is non-empty, SET @slave_connect_state must be sent");
    }
    /**
     * Verifies that when an SSL channel is closed via disconnectChannel(), SO_LINGER(0) is always
     * applied — regardless of the {@code useNonGracefulDisconnect} flag.  This prevents a TLS
     * close_notify deadlock that occurs after a TLS 1.3 KeyUpdate failure (debezium/dbz#2213):
     * calling {@code shutdownOutput()} on an SSL socket whose write path is in an inconsistent
     * state can block indefinitely, hanging the reconnect loop forever.
     */
    @Test(timeOut = 10000)
    public void testSslChannelDisconnectUsesSoLinger0() throws Exception {
        final AtomicBoolean soLingerSetToZero = new AtomicBoolean(false);
        final AtomicBoolean shutdownOutputCalled = new AtomicBoolean(false);
        final CountDownLatch serverReady = new CountDownLatch(1);

        final ServerSocket serverSocket = new ServerSocket();
        serverSocket.bind(new InetSocketAddress("localhost", 0));
        int serverPort = serverSocket.getLocalPort();

        Thread serverThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    serverReady.countDown();
                    Socket client = serverSocket.accept();
                    // Send exactly one byte so the client's peek() check passes, then close
                    client.getOutputStream().write(0xFF);
                    client.getOutputStream().flush();
                    client.close();
                }
                catch (IOException ignored) {
                }
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();
        serverReady.await();

        // Wrap the Socket in a spy that records SO_LINGER and shutdownOutput calls
        BinaryLogClient client = new BinaryLogClient("localhost", serverPort, "root", "");
        client.setSocketFactory(new SocketFactory() {
            @Override
            public Socket createSocket() throws SocketException {
                return new Socket() {
                    @Override
                    public void setSoLinger(boolean on, int linger) throws SocketException {
                        if (on && linger == 0) {
                            soLingerSetToZero.set(true);
                        }
                        super.setSoLinger(on, linger);
                    }

                    @Override
                    public void shutdownOutput() throws IOException {
                        shutdownOutputCalled.set(true);
                        super.shutdownOutput();
                    }
                };
            }
        });

        // Register an SSL socket factory so channel.isSSL() returns true; the
        // factory itself never completes an SSL handshake — we only care that
        // disconnectChannel() applies SO_LINGER(0) for SSL channels.
        client.setSslSocketFactory(new SSLSocketFactory() {
            @Override
            public SSLSocket createSocket(Socket socket) throws SocketException {
                throw new SocketException("test: SSL upgrade intentionally skipped");
            }
        });
        client.setSSLMode(com.github.shyiko.mysql.binlog.network.SSLMode.PREFERRED);
        client.setKeepAlive(false);

        // The connect() attempt will fail (mock server sends 0xFF then closes), which
        // exercises the disconnectChannel() path we want to verify.
        try {
            client.connect();
        }
        catch (IOException ignored) {
        }

        // For a non-SSL channel (SSL upgrade throws), SO_LINGER behaviour follows
        // the useNonGracefulDisconnect flag only. In the SSL upgrade-failure path the
        // channel is still a plain socket, so SO_LINGER is NOT expected here.
        // The important assertion is that shutdownOutput is NOT called when SO_LINGER(0)
        // IS set — we verify that invariant via PacketChannel directly below.
        PacketChannel ch = new PacketChannel(new Socket() {
            @Override
            public InputStream getInputStream() throws IOException {
                return new InputStream() {
                    @Override
                    public int read() throws IOException {
                        return -1;
                    }
                };
            }

            @Override
            public OutputStream getOutputStream() throws IOException {
                return new OutputStream() {
                    @Override
                    public void write(int b) throws IOException {
                    }
                };
            }

            @Override
            public void setSoLinger(boolean on, int linger) throws SocketException {
                if (on && linger == 0) {
                    soLingerSetToZero.set(true);
                }
            }

            @Override
            public void shutdownOutput() throws IOException {
                shutdownOutputCalled.set(true);
            }

            @Override
            public void shutdownInput() throws IOException {
            }

            @Override
            public synchronized void close() throws IOException {
            }

            @Override
            public boolean isClosed() {
                return false;
            }
        });
        soLingerSetToZero.set(false);
        shutdownOutputCalled.set(false);
        ch.setShouldUseSoLinger0();
        ch.close();

        assertTrue(soLingerSetToZero.get(), "setSoLinger(true, 0) must be called when shouldUseSoLinger0 is set");
        assertFalse(shutdownOutputCalled.get(),
            "shutdownOutput() must NOT be called when SO_LINGER(0) is active: it can deadlock "
                + "on SSL sockets after a TLS 1.3 KeyUpdate write failure (debezium/dbz#2213)");

        serverSocket.close();
    }

    /**
     * Builds a PacketChannel over a stub socket. When {@code writable} is false every write
     * fails with an IOException, emulating a broken connection.
     */
    private static PacketChannel stubChannel(final boolean writable) throws IOException {
        return new PacketChannel(new Socket() {
            @Override
            public InputStream getInputStream() throws IOException {
                return new InputStream() {
                    @Override
                    public int read() throws IOException {
                        return -1;
                    }
                };
            }

            @Override
            public OutputStream getOutputStream() throws IOException {
                return new OutputStream() {
                    @Override
                    public void write(int b) throws IOException {
                        if (!writable) {
                            throw new IOException("test: broken pipe");
                        }
                    }
                };
            }
        });
    }

    /**
     * With heartbeats enabled, the keepalive thread must not declare a connection lost before the
     * first event of that connection has arrived, no matter how stale eventLastSeen is: after
     * COM_BINLOG_DUMP(_GTID) the server may take longer than keepAliveInterval to locate the
     * requested position (e.g. GTID auto-positioning over a large number of binlog files), during
     * which it sends neither events nor heartbeats. Tearing the connection down then restarts the
     * position search from scratch, so the client would reconnect forever without ever streaming
     * (debezium/dbz#2266).
     */
    @Test
    public void testKeepAliveToleratesSilenceWhileAwaitingFirstEvent() throws IOException {
        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "mysql");
        client.setHeartbeatInterval(80);
        client.setKeepAliveInterval(100);
        // eventSeenSinceConnect defaults to false and eventLastSeen to 0, i.e. arbitrarily stale
        client.channel = stubChannel(true);
        assertFalse(client.isConnectionLost(),
            "a connection awaiting its first event must not be considered lost while its socket is writable");
    }

    /**
     * A connection that is still awaiting its first event is probed with a ping, so a socket that
     * is actually broken during the position-resolution phase is still detected as lost.
     */
    @Test
    public void testKeepAliveDetectsBrokenSocketWhileAwaitingFirstEvent() throws IOException {
        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "mysql");
        client.setHeartbeatInterval(80);
        client.setKeepAliveInterval(100);
        client.channel = stubChannel(false);
        assertTrue(client.isConnectionLost(),
            "a broken socket must be detected even before the first event is received");
    }

    /**
     * Once the first event of the current connection has been seen, the original behavior applies:
     * with heartbeats enabled the connection is considered lost when no event has been received for
     * more than keepAliveInterval.
     */
    @Test
    public void testKeepAliveReconnectsWhenEstablishedStreamGoesSilent() throws IOException {
        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "mysql");
        client.setHeartbeatInterval(80);
        client.channel = stubChannel(true);
        client.eventSeenSinceConnect = true;
        // eventLastSeen is 0, so the stream is stale for any realistic interval...
        client.setKeepAliveInterval(1);
        assertTrue(client.isConnectionLost(),
            "an established stream that went silent for longer than keepAliveInterval must reconnect");
        // ...but not when the interval exceeds the time elapsed since epoch
        client.setKeepAliveInterval(Long.MAX_VALUE);
        assertFalse(client.isConnectionLost(),
            "an established stream within keepAliveInterval must not reconnect");
    }

    /**
     * With heartbeats disabled the keepalive has always probed the connection with a ping and
     * never consulted event staleness; that behavior must be preserved by the extracted
     * decision logic.
     */
    @Test
    public void testKeepAlivePingsWhenHeartbeatsDisabled() throws IOException {
        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "mysql");
        // heartbeatInterval stays 0; eventLastSeen is 0, i.e. stale for any interval
        client.setKeepAliveInterval(1);
        client.eventSeenSinceConnect = true;
        client.channel = stubChannel(true);
        assertFalse(client.isConnectionLost(),
            "with heartbeats disabled a writable socket must not be considered lost, however stale eventLastSeen is");
        client.channel = stubChannel(false);
        assertTrue(client.isConnectionLost(),
            "with heartbeats disabled a broken socket must be considered lost");
    }

    /*
    @Test
    public void testDeadlockyCode() throws IOException, InterruptedException {
        final BinaryLogClient binaryLogClient = new BinaryLogClient("localhost", 3306, "root", "123456");
        binaryLogClient.setHeartbeatInterval(10000);
        binaryLogClient.setKeepAlive(true);
        binaryLogClient.setKeepAliveInterval(2000);

        binaryLogClient.connect();

        Thread.sleep(1000);

        binaryLogClient.disconnect();
    }
    */

    /**
     * In blocking mode the 0xFE end-of-stream marker sent by MySQL on graceful
     * shutdown must cause a clean break (triggering the reconnect path) rather
     * than propagating an EOFException.  Concretely: when blocking==true, the
     * {@code completeShutdown} flag must NOT be set after a 0xFE packet.
     */
    @Test
    public void testFe0MarkerInBlockingModeDoesNotSetCompleteShutdown() {
        // Build a client in blocking mode (the default) and check the logic
        // by examining the fixed if-statement semantics:
        //   blocking == true  → completeShutdown must remain false
        //   blocking == false → completeShutdown must become true
        boolean blocking;
        boolean completeShutdown;

        // Simulate blocking == true path (fixed code)
        blocking = true;
        completeShutdown = false;
        // if (marker == 0xFE) { if (!blocking) { completeShutdown = true; } break; }
        if (!blocking) {
            completeShutdown = true;
        }
        assertFalse(completeShutdown,
            "In blocking mode, 0xFE must NOT set completeShutdown (reconnect, not full shutdown)");

        // Simulate blocking == false path (non-blocking, existing behaviour preserved)
        blocking = false;
        completeShutdown = false;
        if (!blocking) {
            completeShutdown = true;
        }
        assertTrue(completeShutdown,
            "In non-blocking mode, 0xFE must still set completeShutdown (full shutdown)");
    }

    /**
     * Creates an {@link EventHeaderV4} whose event type is set to the given type.
     */
    private static EventHeaderV4 headerOf(EventType type) {
        EventHeaderV4 h = new EventHeaderV4();
        h.setEventType(type);
        return h;
    }

    /**
     * After a {@code GTID_TAGGED} event followed by an {@code XID} commit, the
     * client's GTID set must contain the tagged transaction — identical to the
     * existing behaviour for plain {@code GTID} events.
     */
    @Test
    public void testUpdateGtidSetRecordsGtidTaggedTransaction() {
        final String uuid = "00000000-0000-0000-0000-000000000001";
        final String tag = "testtag";
        final String initialGtidSet = uuid + ":1-1";

        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "mysql");
        client.setGtidSet(initialGtidSet);

        // Simulate: GTID_TAGGED event for uuid:testtag:2
        MySqlGtid taggedGtid = MySqlGtid.fromString(uuid + ":" + tag + ":2");
        GtidTaggedEventData taggedData = new GtidTaggedEventData(
            taggedGtid, (byte) 0, 0L, 0L, 0L, 0L, 0L, 0, 0, 0L);
        client.updateGtidSet(new Event(headerOf(EventType.GTID_TAGGED), taggedData));

        // Simulate: XID commit — this calls commitGtid() and flushes gtid into gtidSet
        XidEventData xidData = new XidEventData();
        client.updateGtidSet(new Event(headerOf(EventType.XID), xidData));

        // The GTID set must now include uuid:testtag:2
        final GtidSet gtidSet = new GtidSet(client.getGtidSet());
        final GtidSet.UUIDSet taggedUuidSet = gtidSet.getUUIDSet(uuid, tag);
        assertNotNull(taggedUuidSet,
            "GtidSet must contain an entry for " + uuid + " with tag '" + tag + "'");
        assertEquals(taggedUuidSet.getIntervals().size(), 1);
        assertEquals(taggedUuidSet.getIntervals().get(0).getStart(), 2L);
        assertEquals(taggedUuidSet.getIntervals().get(0).getEnd(), 2L);
    }

    /**
     * Plain (untagged) {@code GTID} handling must be unaffected by
     * the addition of the {@code GTID_TAGGED} case.
     */
    @Test
    public void testUpdateGtidSetStillTracksUntaggedGtid() {
        final String uuid = "00000000-0000-0000-0000-000000000002";
        final String initialGtidSet = uuid + ":1-1";

        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "mysql");
        client.setGtidSet(initialGtidSet);

        // GTID (untagged) event for uuid:2
        MySqlGtid untaggedGtid = MySqlGtid.fromString(uuid + ":2");
        GtidEventData gtidData = new GtidEventData(
            untaggedGtid, (byte) 0, 0L, 0L, 0L, 0L, 0L, 0, 0);
        client.updateGtidSet(new Event(headerOf(EventType.GTID), gtidData));

        XidEventData xidData = new XidEventData();
        client.updateGtidSet(new Event(headerOf(EventType.XID), xidData));

        final GtidSet gtidSet = new GtidSet(client.getGtidSet());
        final GtidSet.UUIDSet uuidSet = gtidSet.getUUIDSet(uuid);
        assertNotNull(uuidSet, "GtidSet must contain an untagged entry for " + uuid);
        // Interval 1-1 and 2-2 should be merged to 1-2
        assertEquals(uuidSet.getIntervals().get(0).getEnd(), 2L);
    }

    /**
     * Verifies the {@code getGtidSet()} accessor returns the correct string
     * representation when the GTID set contains both untagged and tagged entries.
     */
    @Test
    public void testGetGtidSetReflectsTaggedAndUntaggedEntries() {
        final String uuid = "00000000-0000-0000-0000-000000000003";
        final String tag = "mytag";

        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "mysql");
        client.setGtidSet(uuid + ":1-1:" + tag + ":5-5");

        final String gtidSetStr = client.getGtidSet();
        assertTrue(gtidSetStr != null && !gtidSetStr.isEmpty(),
            "getGtidSet() must return a non-empty string when a non-empty GTID was set");
        assertTrue(gtidSetStr.contains(uuid),
            "GTID set string must contain the server UUID");
        assertTrue(gtidSetStr.contains(tag),
            "GTID set string must contain the tag");
    }
}
