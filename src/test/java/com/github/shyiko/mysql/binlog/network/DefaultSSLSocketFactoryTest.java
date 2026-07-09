/*
 * Copyright 2026 Safwan Khan
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
package com.github.shyiko.mysql.binlog.network;

import org.testng.annotations.Test;

import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 * Unit tests for {@link DefaultSSLSocketFactory}, verifying that it correctly pins the TLS
 * protocol version on the created socket to prevent unwanted TLS 1.3 negotiation.
 *
 * <p>TLS 1.3 introduces a post-handshake KeyUpdate mechanism which, on high-throughput
 * connections under Java 17+, can trigger concurrent SSL writes that conflict with keepalive
 * pings and cause "Connection reset by peer" failures (debezium/dbz#2213).
 *
 * @author Safwan Khan
 */
public class DefaultSSLSocketFactoryTest {

    /**
     * Verifies that {@code DefaultSSLSocketFactory} with a "TLSv1.2" protocol explicitly sets
     * the enabled protocols on the resulting SSLSocket to only "TLSv1.2", preventing TLS 1.3
     * from being negotiated even on Java 17+ where TLS 1.3 is the JVM default.
     */
    @Test(timeOut = 10000)
    public void testTls12FactoryPinsProtocolOnSocket() throws Exception {
        final CountDownLatch serverReady = new CountDownLatch(1);
        final AtomicReference<Exception> serverError = new AtomicReference<>();

        final ServerSocket plainServer = new ServerSocket();
        plainServer.bind(new InetSocketAddress("localhost", 0));
        int port = plainServer.getLocalPort();

        Thread serverThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    serverReady.countDown();
                    Socket client = plainServer.accept();
                    client.close();
                }
                catch (IOException e) {
                    serverError.set(e);
                }
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();

        serverReady.await();

        Socket plainSocket = new Socket("localhost", port);
        try {
            DefaultSSLSocketFactory factory = new DefaultSSLSocketFactory("TLSv1.2");
            SSLSocket sslSocket = factory.createSocket(plainSocket);
            try {
                String[] enabledProtocols = sslSocket.getEnabledProtocols();
                assertEquals(enabledProtocols.length, 1,
                    "Only one protocol should be enabled; got: " + Arrays.toString(enabledProtocols));
                assertEquals(enabledProtocols[0], "TLSv1.2",
                    "The single enabled protocol must be TLSv1.2 to prevent TLS 1.3 KeyUpdate");
                assertFalse(Arrays.asList(enabledProtocols).contains("TLSv1.3"),
                    "TLS 1.3 must NOT be in the enabled protocols (would trigger KeyUpdate)");
            }
            finally {
                try {
                    sslSocket.close();
                }
                catch (IOException ignored) {
                }
            }
        }
        finally {
            try {
                plainSocket.close();
            }
            catch (IOException ignored) {
            }
            plainServer.close();
        }

        if (serverError.get() != null) {
            throw serverError.get();
        }
    }

    /**
     * Verifies the default (no-arg) constructor produces a socket limited to TLSv1.2.
     * The default was pinned to TLSv1.2 in line with JDK 11.0.11 changes and must remain
     * so to avoid TLS 1.3 KeyUpdate regressions on Java 17.
     */
    @Test(timeOut = 10000)
    public void testDefaultConstructorUsesTls12() throws Exception {
        final CountDownLatch serverReady = new CountDownLatch(1);
        final ServerSocket plainServer = new ServerSocket();
        plainServer.bind(new InetSocketAddress("localhost", 0));
        int port = plainServer.getLocalPort();

        Thread serverThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    serverReady.countDown();
                    Socket client = plainServer.accept();
                    client.close();
                }
                catch (IOException ignored) {
                }
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();

        serverReady.await();

        Socket plainSocket = new Socket("localhost", port);
        try {
            DefaultSSLSocketFactory factory = new DefaultSSLSocketFactory();
            SSLSocket sslSocket = factory.createSocket(plainSocket);
            try {
                String[] enabledProtocols = sslSocket.getEnabledProtocols();
                assertFalse(Arrays.asList(enabledProtocols).contains("TLSv1.3"),
                    "Default factory must not enable TLS 1.3 (causes KeyUpdate on Java 17+)");
            }
            finally {
                try {
                    sslSocket.close();
                }
                catch (IOException ignored) {
                }
            }
        }
        finally {
            try {
                plainSocket.close();
            }
            catch (IOException ignored) {
            }
            plainServer.close();
        }
    }
}
