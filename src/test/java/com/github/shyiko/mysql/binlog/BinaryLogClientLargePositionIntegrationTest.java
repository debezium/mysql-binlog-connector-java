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
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

/**
 * Opt-in test against an isolated, externally managed MySQL 8.0.28+ server.
 * Enable with binlog.large.enabled=true and configure binlog.large.host, port,
 * username, password and optionally jdbcUrl. The account needs replication,
 * privileges to create and drop its fixture schema, flush binary logs and set SESSION sql_log_bin.
 *
 * The fixture writes one transaction containing 1100 server-generated 4 MiB
 * BLOBs. Allow at least 20 GiB of database disk space and several minutes. It
 * requires GTID ON, ROW/FULL, CRC32, transaction compression OFF, and an 8 GiB
 * max_binlog_cache_size. No BLOB or row event is retained by the test. The
 * fixture is deliberately retained for running the same tests against an older
 * client; removing the isolated server's data directory cleans it up.
 */
public class BinaryLogClientLargePositionIntegrationTest {

    private static final long UINT32_RANGE = 1L << 32;
    private static final String SCHEMA = "dbz_large_positions";
    private static final int PAYLOAD_ROWS = 1100;
    private static final int PAYLOAD_BYTES = 4 * 1024 * 1024;
    private static final long TIMEOUT_SECONDS = 600;

    private Connection database;
    private Fixture fixture;
    private final Map<String, Map<Long, PhysicalEvent>> physicalEvents = new HashMap<>();

    @BeforeClass
    public void prepareFixture() throws Exception {
        if (!Boolean.getBoolean("binlog.large.enabled")) {
            throw new SkipException("Enable binlog.large.enabled explicitly to write and stream a >4 GiB binlog");
        }
        database = openDatabase();
        verifyServer();
        if (tableExists("fixture_meta")) {
            loadFixture();
            return;
        }

        boolean createdSchema = createSchemaIfMissing();
        List<String> createdTables = new ArrayList<>();
        try {
            createFixture(createdTables);
            loadFixture();
        } catch (Exception | AssertionError failure) {
            // The writer connection may have failed partway through the large transaction.
            try (Connection cleanup = openDatabase(); Statement statement = cleanup.createStatement()) {
                if (createdSchema) {
                    statement.execute("DROP DATABASE " + SCHEMA);
                } else if (!createdTables.isEmpty()) {
                    statement.execute("DROP TABLE " + String.join(", ", createdTables));
                }
            } catch (SQLException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private static Connection openDatabase() throws SQLException {
        String defaultUrl = "jdbc:mysql://" + property("host", "127.0.0.1") + ":" + property("port", "3306") +
            "/?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=10000&socketTimeout=900000";
        return DriverManager.getConnection(property("jdbcUrl", defaultUrl),
            property("username", "root"), property("password", ""));
    }

    private void loadFixture() throws Exception {
        fixture = readFixture();
        assertEquals(fixture.payloadRows, PAYLOAD_ROWS);
        assertEquals(fixture.payloadBytes, PAYLOAD_BYTES);
        assertNotEquals(fixture.bigFile, fixture.barrierFile, "The oversized transaction must rotate its binlog");
        assertEquals(queryLong("SELECT COUNT(*) FROM " + SCHEMA + ".payload_rows"), (long) PAYLOAD_ROWS);
        loadPhysicalEvents(fixture.bigFile);
        loadPhysicalEvents(fixture.barrierFile);
        assertTrue(fixture.bigTransactionEnd > UINT32_RANGE,
            "The server's physical XID position must exceed 4 GiB: " + fixture.bigTransactionEnd);
        assertTrue(fixture.tailTableMapPosition > UINT32_RANGE,
            "The multi-row resume fixture must be physically beyond 4 GiB");
        System.out.println("Large-binlog fixture: file=" + fixture.bigFile + ", size=" + fixture.bigFileSize +
            ", start=" + fixture.startPosition + ", xidEnd=" + fixture.bigTransactionEnd +
            ", tailTableMap=" + fixture.tailTableMapPosition + ", tailRows=" + fixture.tailRowsPosition +
            ", barrier=" + fixture.barrierFile + ":" + fixture.barrierEndPosition +
            ", gtidBefore=" + fixture.gtidBefore + ", gtidAfter=" + fixture.gtidAfter);
    }

    @AfterClass(alwaysRun = true)
    public void closeDatabase() throws Exception {
        if (database != null) {
            database.close();
        }
    }

    @Test(timeOut = 900000)
    public void testPhysicalPositionsBeyondFourGiB() throws Exception {
        StreamObserver observer = new StreamObserver(fixture.bigFile);
        BinaryLogClient client = client(fixture.bigFile, fixture.startPosition, null);
        stream(client, event -> {
            observer.accept(event);
            return observer.isBarrierCommit(event);
        });

        assertEquals(observer.payloadRows, PAYLOAD_ROWS);
        assertEquals(observer.tailIds, Arrays.asList(3001, 3002, 3003));
        assertEquals(observer.barrierIds, Arrays.asList(9001));
        assertTrue(observer.positionsAboveFourGiB > 0, "Real file events must cross the 32-bit boundary");
        assertTrue(observer.sawLargeFileRotate, "The final ROTATE must report the end of the large file");
        assertEquals(client.getBinlogFilename(), fixture.barrierFile);
        assertEquals(client.getBinlogPosition(), fixture.barrierEndPosition);
    }

    @Test(timeOut = 900000)
    public void testGtidResumeSkipsMoreThanFourGiB() throws Exception {
        StreamObserver observer = new StreamObserver(fixture.bigFile);
        List<Long> skippedRangeAnchors = new ArrayList<>();
        // An explicit filename prevents GTID auto-positioning from opening the
        // newer file directly and bypassing the >4 GiB filtered transaction.
        BinaryLogClient client = client(fixture.bigFile, 4, fixture.gtidAfter);
        stream(client, event -> {
            observer.accept(event);
            EventHeaderV4 header = event.getHeader();
            if (header.getEventType() == EventType.HEARTBEAT_V2 && header.getNextPosition() > UINT32_RANGE) {
                assertEquals(header.getNextPosition(), fixture.bigTransactionEnd,
                    "The skipped transaction's anchor must match the SQL physical end position");
                skippedRangeAnchors.add(header.getNextPosition());
            }
            return observer.isBarrierCommit(event);
        });

        assertEquals(observer.payloadRows, 0, "GTID filtering must omit the large transaction's row payloads");
        assertTrue(observer.tailIds.isEmpty());
        assertFalse(skippedRangeAnchors.isEmpty(), "The server must send a full-width skipped-range heartbeat");
        assertEquals(observer.barrierIds, Arrays.asList(9001));
        System.out.println("Large-binlog GTID skip anchors: " + skippedRangeAnchors);
    }

    @Test(timeOut = 900000)
    public void testPartialRowResumeBeyondFourGiB() throws Exception {
        List<Integer> firstPass = new ArrayList<>();
        AtomicReference<String> savedGtid = new AtomicReference<>();
        long[] savedPositions = new long[2];
        StreamObserver first = new StreamObserver(fixture.bigFile);
        BinaryLogClient interrupted = client(fixture.bigFile, fixture.tailTableMapPosition, fixture.gtidBefore);
        stream(interrupted, event -> {
            first.accept(event);
            if (event.getData() instanceof WriteRowsEventData && "tail_rows".equals(first.table(event))) {
                WriteRowsEventData rows = event.getData();
                assertEquals(rows.getRows().size(), 3, "The resume case must stop inside one multi-row event");
                firstPass.add(((Number) rows.getRows().get(0)[0]).intValue());
                firstPass.add(((Number) rows.getRows().get(1)[0]).intValue());
                savedGtid.set(interrupted.getGtidSet());
                savedPositions[0] = interrupted.getBinlogPosition();
                savedPositions[1] = ((EventHeaderV4) event.getHeader()).getPosition();
                // stream() disconnects on this same reader thread before it
                // advances past this multi-row event to the transaction's XID.
                // This file-position resume starts after the GTID/BEGIN events.
                return true;
            }
            return false;
        });
        assertEquals(firstPass, Arrays.asList(3001, 3002));
        assertEquals(savedGtid.get(), fixture.gtidBefore);
        assertEquals(savedPositions[0], fixture.tailTableMapPosition, "Retain the table map for a fresh client");
        assertEquals(savedPositions[1], fixture.tailRowsPosition);

        List<Integer> resumed = new ArrayList<>();
        StreamObserver second = new StreamObserver(fixture.bigFile);
        BinaryLogClient restarted = client(fixture.bigFile, savedPositions[0], savedGtid.get());
        stream(restarted, event -> {
            second.accept(event);
            if (event.getData() instanceof WriteRowsEventData && "tail_rows".equals(second.table(event))) {
                assertEquals(((EventHeaderV4) event.getHeader()).getPosition(), savedPositions[1],
                    "The replayed multi-row event must retain its original full-width position");
                WriteRowsEventData rows = event.getData();
                for (int row = firstPass.size(); row < rows.getRows().size(); row++) {
                    resumed.add(((Number) rows.getRows().get(row)[0]).intValue());
                }
            }
            return second.isBarrierCommit(event);
        });
        assertEquals(resumed, Arrays.asList(3003));
        assertEquals(second.barrierIds, Arrays.asList(9001));
        assertEquals(second.payloadRows, 0, "A partial-row resume must not re-stream the preceding BLOBs");
    }

    private BinaryLogClient client(String file, long position, String gtids) {
        BinaryLogClient client = new BinaryLogClient(property("host", "127.0.0.1"),
            Integer.parseInt(property("port", "3306")), property("username", "root"), property("password", ""));
        client.setServerId(99002508L);
        client.setKeepAlive(false);
        client.setHeartbeatInterval(0);
        client.setConnectTimeout(10000);
        client.setBinlogFilename(file);
        client.setBinlogPosition(position);
        if (gtids != null) {
            client.setGtidSet(gtids);
            client.setUseBinlogFilenamePositionInGtidMode(true);
        }
        return client;
    }

    static void stream(BinaryLogClient client, EventConsumer consumer) throws Exception {
        CountDownLatch completed = new CountDownLatch(1);
        AtomicBoolean goalReached = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        client.registerEventListener(event -> {
            boolean stop;
            try {
                stop = consumer.accept(event);
                if (stop) {
                    goalReached.set(true);
                }
            } catch (Throwable error) {
                recordFailure(failure, error);
                stop = true;
            }
            if (stop) {
                try {
                    client.disconnect();
                } catch (Throwable cleanup) {
                    recordFailure(failure, cleanup);
                } finally {
                    completed.countDown();
                }
            }
        });
        client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
            @Override
            public void onCommunicationFailure(BinaryLogClient source, Exception exception) {
                recordFailure(failure, exception);
                completed.countDown();
            }

            @Override
            public void onEventDeserializationFailure(BinaryLogClient source, Exception exception) {
                recordFailure(failure, exception);
                completed.countDown();
            }
        });
        Thread reader = new Thread(() -> {
            try {
                client.connect();
            } catch (Throwable error) {
                recordFailure(failure, error);
            } finally {
                completed.countDown();
            }
        }, "large-binlog-position-reader");
        reader.setDaemon(true);
        boolean interrupted = false;
        try {
            reader.start();
            assertTrue(completed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "Replication did not reach its expected stopping event");
            if (failure.get() == null) {
                assertTrue(goalReached.get(), "Replication reader terminated before reaching its expected stopping event");
            }
        } catch (Throwable error) {
            recordFailure(failure, error);
            interrupted = error instanceof InterruptedException;
        } finally {
            interrupted |= Thread.interrupted();
            try {
                client.disconnect();
            } catch (Throwable cleanup) {
                recordFailure(failure, cleanup);
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (reader.isAlive()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    recordFailure(failure, new AssertionError("The replication reader must terminate before the next scenario"));
                    break;
                }
                try {
                    TimeUnit.NANOSECONDS.timedJoin(reader, remaining);
                } catch (InterruptedException cleanup) {
                    recordFailure(failure, cleanup);
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        Throwable error = failure.get();
        if (error instanceof Error fatal) {
            throw fatal;
        }
        if (error instanceof Exception exception) {
            throw exception;
        }
        if (error != null) {
            throw new AssertionError("Replication failed", error);
        }
    }

    private static void recordFailure(AtomicReference<Throwable> failure, Throwable error) {
        Throwable firstFailure = failure.compareAndExchange(null, error);
        if (firstFailure != null && firstFailure != error) {
            firstFailure.addSuppressed(error);
        }
    }

    private void verifyServer() throws Exception {
        try (Statement statement = database.createStatement(); ResultSet result = statement.executeQuery(
                "SELECT VERSION(), @@GLOBAL.gtid_mode, @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image, " +
                    "@@GLOBAL.binlog_checksum, @@GLOBAL.binlog_transaction_compression, @@GLOBAL.max_binlog_cache_size")) {
            assertTrue(result.next());
            BinaryLogDatabaseVersion version = BinaryLogDatabaseVersion.parse(result.getString(1));
            assertFalse(version.isMariaDb(), "This opt-in fixture exercises MySQL's wire format");
            assertTrue(version.isGreaterThanOrEqualTo(8, 0, 28), "Requires MySQL 8.0.28+");
            assertEquals(result.getString(2), "ON");
            assertEquals(result.getString(3), "ROW");
            assertEquals(result.getString(4), "FULL");
            assertEquals(result.getString(5), "CRC32");
            assertEquals(result.getInt(6), 0);
            assertTrue(result.getLong(7) > (long) PAYLOAD_ROWS * PAYLOAD_BYTES,
                "max_binlog_cache_size must accommodate the entire >4 GiB transaction");
        }
    }

    private boolean tableExists(String table) throws Exception {
        try (PreparedStatement statement = database.prepareStatement(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=? AND table_name=?")) {
            statement.setString(1, SCHEMA);
            statement.setString(2, table);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                return result.getInt(1) != 0;
            }
        }
    }

    private boolean createSchemaIfMissing() throws SQLException {
        try (Statement statement = database.createStatement()) {
            statement.execute("CREATE DATABASE " + SCHEMA);
            return true;
        } catch (SQLException e) {
            if (e.getErrorCode() == 1007) { // ER_DB_CREATE_EXISTS
                return false;
            }
            throw e;
        }
    }

    private void createFixture(List<String> createdTables) throws Exception {
        try (Statement statement = database.createStatement()) {
            createTable(statement, createdTables, "payload_rows", "id INT PRIMARY KEY, payload LONGBLOB NOT NULL");
            createTable(statement, createdTables, "tail_rows", "id INT PRIMARY KEY");
            createTable(statement, createdTables, "barrier_rows", "id INT PRIMARY KEY");
            createTable(statement, createdTables, "fixture_meta", "id INT PRIMARY KEY, big_file VARCHAR(255) NOT NULL, " +
                "start_position BIGINT NOT NULL, gtid_before LONGTEXT NOT NULL, gtid_after LONGTEXT NOT NULL, " +
                "barrier_file VARCHAR(255) NOT NULL, barrier_end_position BIGINT NOT NULL, " +
                "payload_rows INT NOT NULL, payload_bytes INT NOT NULL");
            statement.execute("FLUSH BINARY LOGS");
        }
        Status before = status();
        String gtidBefore = queryString("SELECT @@GLOBAL.gtid_executed");
        database.setAutoCommit(false);
        long start = System.nanoTime();
        try (PreparedStatement insert = database.prepareStatement(
                "INSERT INTO " + SCHEMA + ".payload_rows VALUES (?, REPEAT(_binary'x', ?))")) {
            insert.setQueryTimeout((int) TIMEOUT_SECONDS);
            for (int row = 1; row <= PAYLOAD_ROWS; row++) {
                assertTrue(System.nanoTime() - start < TimeUnit.MINUTES.toNanos(20), "Fixture generation exceeded 20 minutes");
                insert.setInt(1, row);
                insert.setInt(2, PAYLOAD_BYTES);
                assertEquals(insert.executeUpdate(), 1);
                if (row % 100 == 0) {
                    System.out.println("Large-binlog fixture: generated " + row + "/" + PAYLOAD_ROWS + " rows");
                }
            }
            try (Statement statement = database.createStatement()) {
                statement.executeUpdate("INSERT INTO " + SCHEMA + ".tail_rows VALUES (3001), (3002), (3003)");
            }
            database.commit();
        } catch (Throwable failure) {
            database.rollback();
            throw failure;
        } finally {
            database.setAutoCommit(true);
        }
        String gtidAfter = queryString("SELECT @@GLOBAL.gtid_executed");
        try (Statement statement = database.createStatement()) {
            statement.executeUpdate("INSERT INTO " + SCHEMA + ".barrier_rows VALUES (9001)");
        }
        Status barrier = status();
        try (Statement statement = database.createStatement()) {
            statement.execute("SET SESSION sql_log_bin=0");
        }
        try (PreparedStatement insert = database.prepareStatement(
                "INSERT INTO " + SCHEMA + ".fixture_meta VALUES (1,?,?,?,?,?,?,?,?)")) {
            insert.setString(1, before.file);
            insert.setLong(2, before.position);
            insert.setString(3, gtidBefore);
            insert.setString(4, gtidAfter);
            insert.setString(5, barrier.file);
            insert.setLong(6, barrier.position);
            insert.setInt(7, PAYLOAD_ROWS);
            insert.setInt(8, PAYLOAD_BYTES);
            insert.executeUpdate();
        }
    }

    private static void createTable(Statement statement, List<String> createdTables, String table, String columns) throws SQLException {
        String qualifiedName = SCHEMA + "." + table;
        statement.execute("CREATE TABLE " + qualifiedName + " (" + columns + ") ENGINE=InnoDB");
        createdTables.add(qualifiedName);
    }

    private Fixture readFixture() throws Exception {
        try (Statement statement = database.createStatement(); ResultSet result = statement.executeQuery(
                "SELECT * FROM " + SCHEMA + ".fixture_meta WHERE id=1")) {
            assertTrue(result.next(), "An incomplete fixture must be removed before generating a new one");
            Fixture value = new Fixture();
            value.bigFile = result.getString("big_file");
            value.startPosition = result.getLong("start_position");
            value.gtidBefore = result.getString("gtid_before");
            value.gtidAfter = result.getString("gtid_after");
            value.barrierFile = result.getString("barrier_file");
            value.barrierEndPosition = result.getLong("barrier_end_position");
            value.payloadRows = result.getInt("payload_rows");
            value.payloadBytes = result.getInt("payload_bytes");
            return value;
        }
    }

    private void loadPhysicalEvents(String file) throws Exception {
        long fileSize = -1;
        try (Statement statement = database.createStatement(); ResultSet result = statement.executeQuery("SHOW BINARY LOGS")) {
            while (result.next()) {
                if (file.equals(result.getString("Log_name"))) {
                    fileSize = result.getLong("File_size");
                }
            }
        }
        assertTrue(fileSize > 0, "The server must retain the fixture's on-disk binlog " + file);
        List<PhysicalEvent> events = new ArrayList<>();
        try (Statement statement = database.createStatement()) {
            statement.setQueryTimeout((int) TIMEOUT_SECONDS);
            try (ResultSet result = statement.executeQuery("SHOW BINLOG EVENTS IN '" + file + "' FROM 4")) {
                while (result.next()) {
                    PhysicalEvent event = new PhysicalEvent();
                    event.position = result.getLong("Pos");
                    event.type = result.getString("Event_type");
                    events.add(event);
                }
            }
        }
        Map<Long, PhysicalEvent> index = new HashMap<>();
        for (int i = 0; i < events.size(); i++) {
            PhysicalEvent event = events.get(i);
            event.endPosition = i + 1 < events.size() ? events.get(i + 1).position : fileSize;
            index.put(event.position, event);
            if (file.equals(fixture.bigFile)) {
                if ("Table_map".equals(event.type)) {
                    fixture.tailTableMapPosition = event.position;
                } else if ("Write_rows".equals(event.type)) {
                    fixture.tailRowsPosition = event.position;
                } else if ("Xid".equals(event.type)) {
                    fixture.bigTransactionEnd = event.endPosition;
                }
            }
        }
        if (file.equals(fixture.bigFile)) {
            fixture.bigFileSize = fileSize;
        }
        physicalEvents.put(file, index);
    }

    private Status status() throws Exception {
        String sql = MysqlVersion.capture(database).atLeast(8, 4) ? "SHOW BINARY LOG STATUS" : "SHOW MASTER STATUS";
        try (Statement statement = database.createStatement(); ResultSet result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            Status status = new Status();
            status.file = result.getString("File");
            status.position = result.getLong("Position");
            return status;
        }
    }

    private String queryString(String sql) throws Exception {
        try (Statement statement = database.createStatement(); ResultSet result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            return result.getString(1);
        }
    }

    private long queryLong(String sql) throws Exception {
        return Long.parseLong(queryString(sql));
    }

    private static String property(String name, String defaultValue) {
        return System.getProperty("binlog.large." + name, defaultValue);
    }

    private final class StreamObserver {
        private String file;
        private final Map<Long, String> tables = new HashMap<>();
        private int payloadRows;
        private int positionsAboveFourGiB;
        private boolean sawLargeFileRotate;
        private final List<Integer> tailIds = new ArrayList<>();
        private final List<Integer> barrierIds = new ArrayList<>();

        private StreamObserver(String file) {
            this.file = file;
        }

        private void accept(Event event) {
            EventHeaderV4 header = event.getHeader();
            EventType type = header.getEventType();
            if (type == EventType.ROTATE) {
                if (header.getTimestamp() != 0) {
                    // The header describes this file; the payload names the next one.
                    assertPhysicalPosition(header);
                    if (file.equals(fixture.bigFile)) {
                        assertEquals(header.getNextPosition(), fixture.bigFileSize);
                        sawLargeFileRotate = true;
                    }
                } else {
                    assertEquals(header.getNextPosition(), 0L, "The sender's artificial ROTATE has no file position");
                }
                RotateEventData rotate = event.getData();
                file = rotate.getBinlogFilename();
                tables.clear();
                return;
            }
            if (type == EventType.HEARTBEAT || type == EventType.HEARTBEAT_V2 ||
                    (header.getFlags() & 0x20) != 0 || header.getNextPosition() == 0) {
                return;
            }
            assertPhysicalPosition(header);
            if (header.getNextPosition() > UINT32_RANGE) {
                positionsAboveFourGiB++;
            }
            if (event.getData() instanceof TableMapEventData) {
                TableMapEventData table = event.getData();
                assertEquals(table.getDatabase(), SCHEMA);
                tables.put(table.getTableId(), table.getTable());
            } else if (event.getData() instanceof WriteRowsEventData) {
                WriteRowsEventData rows = event.getData();
                String table = table(event);
                for (Serializable[] row : rows.getRows()) {
                    int id = ((Number) row[0]).intValue();
                    if ("payload_rows".equals(table)) {
                        assertEquals(((byte[]) row[1]).length, PAYLOAD_BYTES);
                        payloadRows++;
                    } else if ("tail_rows".equals(table)) {
                        tailIds.add(id);
                    } else if ("barrier_rows".equals(table)) {
                        barrierIds.add(id);
                    } else {
                        throw new AssertionError("Unexpected row table " + table);
                    }
                }
            }
        }

        private void assertPhysicalPosition(EventHeaderV4 header) {
            Map<Long, PhysicalEvent> index = physicalEvents.get(file);
            assertNotNull(index, "Unexpected binlog file " + file);
            PhysicalEvent physical = index.get(header.getPosition());
            assertNotNull(physical, "No SQL physical event at streamed " + file + ":" + header.getPosition() + " (" + header.getEventType() + ")");
            assertEquals(header.getNextPosition(), physical.endPosition,
                "Streamed event end must match the next physical SQL position in " + file);
        }

        private String table(Event event) {
            WriteRowsEventData rows = event.getData();
            return tables.get(rows.getTableId());
        }

        private boolean isBarrierCommit(Event event) {
            EventHeaderV4 header = event.getHeader();
            return fixture.barrierFile.equals(file) && header.getEventType() == EventType.XID &&
                header.getNextPosition() == fixture.barrierEndPosition;
        }
    }

    interface EventConsumer {
        boolean accept(Event event) throws Exception;
    }

    private static class Status {
        private String file;
        private long position;
    }

    private static final class Fixture {
        private String bigFile;
        private long startPosition;
        private String gtidBefore;
        private String gtidAfter;
        private String barrierFile;
        private long barrierEndPosition;
        private int payloadRows;
        private int payloadBytes;
        private long bigFileSize;
        private long bigTransactionEnd;
        private long tailTableMapPosition;
        private long tailRowsPosition;
    }

    private static final class PhysicalEvent {
        private long position;
        private long endPosition;
        private String type;
    }
}
