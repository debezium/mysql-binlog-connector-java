package com.github.shyiko.mysql.binlog;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.testng.SkipException;
import org.testng.annotations.Test;

/**
 * @author <a href="https://github.com/abhayprakashtiwari">Abhay Prakash Tiwari</a>
 * <p>
 * A {@link BinaryLogClient} that has been disconnected must not stay reachable.
 * <p>
 * The keepalive executor is created with {@code Executors.newSingleThreadExecutor(ThreadFactory)}. The
 * {@code ThreadFactory} is an inner class and therefore references the client, while the client references the
 * executor through {@code keepAliveThreadExecutor}. Since JDK 21 the wrapper returned by
 * {@code newSingleThreadExecutor} registers a {@code Cleaner} whose action holds the underlying
 * {@code ThreadPoolExecutor} (and with it the factory and the client). Because the cleaner list is a GC root, the
 * wrapper never becomes unreachable, the cleaner never runs and every disconnected client is retained together
 * with its channel and any TLS buffers.
 * <p>
 * No MySQL server is needed: the keepalive thread is started directly and {@link BinaryLogClient#disconnect()}
 * is called without a channel. The test is meaningful on JDK 21+ only, the reference to the client can only be pinned
 * this way on JDK 21+, so the tests are skipped.
 */
public class BinaryLogClientKeepAliveLeakTest {

    @Test(timeOut = 60000)
    public void disconnectedClientIsGarbageCollectable() throws Exception {
        assumeJdk21(); // only JDK 21+ pins the client through the executor's Cleaner
        WeakReference<BinaryLogClient> ref = startAndDisconnectClient(false);
        assertCollected(ref);
    }

    @Test(timeOut = 60000)
    public void disconnectedClientWithCustomThreadFactoryIsGarbageCollectable() throws Exception {
        assumeJdk21();
        // Debezium installs its own ThreadFactory via setThreadFactory()
        WeakReference<BinaryLogClient> ref = startAndDisconnectClient(true);
        assertCollected(ref);
    }

    private static void assumeJdk21() {
        if (javaMajorVersion() < 21) {
            throw new SkipException("The executor's Cleaner pins the client only on JDK 21+");
        }
    }

    private static WeakReference<BinaryLogClient> startAndDisconnectClient(boolean customThreadFactory)
        throws Exception {
        final CountDownLatch keepAliveStarted = new CountDownLatch(1);
        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "") {
            @Override
            void runKeepAlive(ExecutorService executor) {
                keepAliveStarted.countDown();
                super.runKeepAlive(executor);
            }
        };
        if (customThreadFactory) {
            client.setThreadFactory(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r);
                    t.setDaemon(true);
                    return t;
                }
            });
        }
        client.setKeepAliveInterval(60000L);
        Method prepare = BinaryLogClient.class.getDeclaredMethod("prepareKeepAliveThread");
        prepare.setAccessible(true);
        Method start = BinaryLogClient.class.getDeclaredMethod("startKeepAliveThread", ThreadPoolExecutor.class);
        start.setAccessible(true);
        Thread keepAliveThread = null;
        try {
            ThreadPoolExecutor executor = (ThreadPoolExecutor) prepare.invoke(client);
            Field threadField = BinaryLogClient.class.getDeclaredField("keepAliveThread");
            threadField.setAccessible(true);
            keepAliveThread = (Thread) threadField.get(client);
            start.invoke(client, executor);
            assertTrue(keepAliveStarted.await(10, TimeUnit.SECONDS), "the keepalive monitoring task must start");
            assertTrue(client.isKeepAliveThreadRunning(), "keepalive thread should be running");
        } finally {
            try {
                client.disconnect();
            } finally {
                if (keepAliveThread != null) {
                    // Executor termination can precede the actual thread's exit and release of its stack.
                    keepAliveThread.join(10000);
                    assertFalse(keepAliveThread.isAlive(), "the keepalive thread must exit before checking reachability");
                }
            }
        }
        assertFalse(client.isKeepAliveThreadRunning(), "keepalive thread should be stopped after disconnect()");
        return new WeakReference<BinaryLogClient>(client);
    }

    private static void assertCollected(WeakReference<BinaryLogClient> ref) throws InterruptedException {
        for (int i = 0; i < 50 && ref.get() != null; i++) {
            System.gc();
            Thread.sleep(100);
        }
        assertNull(ref.get(), "BinaryLogClient is still reachable after disconnect(): the keepalive executor's " +
            "Cleaner (JDK 21+) keeps it alive through ThreadPoolExecutor -> ThreadFactory -> BinaryLogClient");
    }

    private static int javaMajorVersion() {
        String v = System.getProperty("java.specification.version");
        return v.startsWith("1.") ? Integer.parseInt(v.substring(2)) : Integer.parseInt(v);
    }

}
