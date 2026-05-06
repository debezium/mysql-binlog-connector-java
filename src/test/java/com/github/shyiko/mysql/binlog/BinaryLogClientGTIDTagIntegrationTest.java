/*
 * Copyright 2024 Debezium Authors
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
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.GtidTaggedEventData;
import com.github.shyiko.mysql.binlog.event.MySqlGtid;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Integration tests for MySQL 8.3+ GTID tag support using SET gtid_next.
 *
 * @author Debezium Authors
 */
public class BinaryLogClientGTIDTagIntegrationTest extends BinaryLogClientIntegrationTest {

    private static final String TEST_TAG = "testtag";

    @Override
    protected TestDatabaseContainerOptions getOptions() {
        if (!this.mysqlVersion.atLeast(8, 3)) {
            throw new SkipException("GTID tags require MySQL 8.3+");
        }

        TestDatabaseContainerOptions options = new TestDatabaseContainerOptions();
        options.gtid = true;
        return options;
    }

    @Test
    public void testGtidTagWithSetGtidNext() throws Exception {
        master.execute("CREATE TABLE if not exists gtid_tag_test (id int primary key, value varchar(50))");

        final String initialGTIDSet = getExecutedGtidSet();
        assertNotNull(initialGTIDSet, "Initial GTID set is null");

        final AtomicReference<String> capturedTag = new AtomicReference<>();
        final AtomicReference<MySqlGtid> capturedGtid = new AtomicReference<>();

        try {
            client.disconnect();
            final BinaryLogClient taggedClient = new BinaryLogClient(master.hostname(), master.port(),
                master.username(), master.password());

            taggedClient.setGtidSet(initialGTIDSet);
            taggedClient.registerEventListener(new BinaryLogClient.EventListener() {
                @Override
                public void onEvent(Event event) {
                    final EventType eventType = event.getHeader().getEventType();
                    if (eventType == EventType.GTID) {
                        final GtidEventData gtidData = (GtidEventData) event.getData();
                        final MySqlGtid gtid = gtidData.getMySqlGtid();
                        capturedGtid.set(gtid);
                        capturedTag.set(gtid.getTag());
                    } else if (eventType == EventType.GTID_TAGGED) {
                        final GtidTaggedEventData gtidData = (GtidTaggedEventData) event.getData();
                        final MySqlGtid gtid = gtidData.getMySqlGtid();
                        capturedGtid.set(gtid);
                        capturedTag.set(gtid.getTag());
                    }
                }
            });
            taggedClient.registerEventListener(new TraceEventListener());
            taggedClient.registerEventListener(eventListener);

            try {
                eventListener.reset();
                taggedClient.connect(DEFAULT_TIMEOUT);

                // Execute a transaction with a tagged GTID using SET gtid_next.
                master.execute(new Callback<Statement>() {
                    @Override
                    public void execute(Statement statement) throws java.sql.SQLException {
                        statement.execute("SET SESSION gtid_next = 'AUTOMATIC:" + TEST_TAG + "'");
                        statement.execute("INSERT INTO gtid_tag_test (id, value) VALUES (1, 'tagged')");
                    }
                });
                master.execute("SET SESSION gtid_next = 'AUTOMATIC'");

                // Wait for the tagged GTID event.
                eventListener.waitForAtLeast(EventType.GTID_TAGGED, 1, TimeUnit.SECONDS.toMillis(4));

                // Verify the tag was captured
                assertNotNull(capturedTag.get(), "GTID tag was not captured");
                assertEquals(capturedTag.get(), TEST_TAG, "GTID tag does not match expected value");

                // Verify the full GTID includes the tag
                assertNotNull(capturedGtid.get(), "GTID was not captured");
                String gtidString = capturedGtid.get().toString();
                assertNotNull(gtidString, "GTID toString returned null");
                assertEquals(gtidString.split(":")[1], TEST_TAG, "GTID string does not include expected tag");

            } finally {
                taggedClient.disconnect();
            }
        } finally {
            client.connect(DEFAULT_TIMEOUT);
            master.execute("DROP TABLE IF EXISTS gtid_tag_test");
        }
    }

    @Test
    public void testMixedTaggedAndNonTaggedGtids() throws Exception {
        master.execute("CREATE TABLE if not exists mixed_gtid_test (id int primary key, value varchar(50))");

        final String initialGTIDSet = getExecutedGtidSet();
        assertNotNull(initialGTIDSet, "Initial GTID set is null");

        final AtomicReference<String> firstTag = new AtomicReference<>();
        final AtomicReference<String> secondTag = new AtomicReference<>();
        final CountDownLatch mixedRows = new CountDownLatch(2);

        try {
            client.disconnect();
            final BinaryLogClient taggedClient = new BinaryLogClient(master.hostname(), master.port(),
                master.username(), master.password());

            taggedClient.setGtidSet(initialGTIDSet);

            taggedClient.registerEventListener(new BinaryLogClient.EventListener() {
                private final AtomicReference<String> transactionTag = new AtomicReference<>();
                private long mixedTableId = -1;

                @Override
                public void onEvent(Event event) {
                    final EventType eventType = event.getHeader().getEventType();
                    if (eventType == EventType.GTID) {
                        final GtidEventData gtidData = (GtidEventData) event.getData();
                        transactionTag.set(gtidData.getMySqlGtid().getTag());
                    } else if (eventType == EventType.GTID_TAGGED) {
                        final GtidTaggedEventData gtidData = (GtidTaggedEventData) event.getData();
                        transactionTag.set(gtidData.getMySqlGtid().getTag());
                    } else if (eventType == EventType.TABLE_MAP) {
                        final TableMapEventData tableMap = (TableMapEventData) event.getData();
                        if ("mixed_gtid_test".equals(tableMap.getTable())) {
                            mixedTableId = tableMap.getTableId();
                        }
                    } else if (EventType.isWrite(eventType)) {
                        final WriteRowsEventData writeRows = (WriteRowsEventData) event.getData();
                        if (writeRows.getTableId() == mixedTableId) {
                            for (Object[] row : writeRows.getRows()) {
                                if (Integer.valueOf(1).equals(row[0])) {
                                    firstTag.set(transactionTag.get());
                                    mixedRows.countDown();
                                } else if (Integer.valueOf(2).equals(row[0])) {
                                    secondTag.set(transactionTag.get());
                                    mixedRows.countDown();
                                }
                            }
                        }
                    }
                }
            });
            taggedClient.registerEventListener(eventListener);

            try {
                eventListener.reset();
                taggedClient.connect(DEFAULT_TIMEOUT);

                // Execute one tagged transaction and one untagged transaction.
                master.execute(new Callback<Statement>() {
                    @Override
                    public void execute(Statement statement) throws java.sql.SQLException {
                        statement.execute("SET SESSION gtid_next = 'AUTOMATIC:" + TEST_TAG + "'");
                        statement.execute("INSERT INTO mixed_gtid_test (id, value) VALUES (1, 'tagged')");
                    }
                });
                master.execute("SET SESSION gtid_next = 'AUTOMATIC'");
                master.execute("INSERT INTO mixed_gtid_test (id, value) VALUES (2, 'not-tagged')");

                // Wait for both GTID event types and the rows that belong to this test.
                eventListener.waitForAtLeast(EventType.GTID_TAGGED, 1, TimeUnit.SECONDS.toMillis(4));
                eventListener.waitForAtLeast(EventType.GTID, 1, TimeUnit.SECONDS.toMillis(4));
                assertTrue(mixedRows.await(4, TimeUnit.SECONDS), "Timed out waiting for mixed GTID test rows");

                // Verify the tagged insert was associated with the tag.
                assertNotNull(firstTag.get(), "First GTID tag was not captured");
                assertEquals(firstTag.get(), TEST_TAG, "First GTID tag does not match");

                // Verify the untagged insert was not associated with a tag.
                assertNull(secondTag.get(), "Second GTID should not have a tag");

            } finally {
                taggedClient.disconnect();
            }
        } finally {
            client.connect(DEFAULT_TIMEOUT);
            master.execute("DROP TABLE IF EXISTS mixed_gtid_test");
        }
    }

    private String getExecutedGtidSet() throws Exception {
        final String[] gtidSet = new String[1];
        // MySQL 8.4+ renamed SHOW MASTER STATUS to SHOW BINARY LOG STATUS
        String version = System.getProperty("mysql.image", "mysql:8.0");
        boolean isMySQL84Plus = version.contains("8.4") || version.contains("8.5") || version.contains("9.");
        String statusQuery = isMySQL84Plus ? "SHOW BINARY LOG STATUS" : "SHOW MASTER STATUS";

        master.query(statusQuery, new Callback<java.sql.ResultSet>() {
            @Override
            public void execute(java.sql.ResultSet rs) throws java.sql.SQLException {
                if (rs.next()) {
                    gtidSet[0] = rs.getString("Executed_Gtid_Set");
                }
            }
        });
        return gtidSet[0];
    }
}
