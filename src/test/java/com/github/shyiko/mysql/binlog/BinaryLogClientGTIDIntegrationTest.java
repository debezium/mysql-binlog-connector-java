/*
 * Copyright 2018 Stanley Shyiko
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

import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * @author <a href="https://github.com/osheroff">Ben Osheroff</a>
 */
public class BinaryLogClientGTIDIntegrationTest extends BinaryLogClientIntegrationTest {
    @Override
    protected TestDatabaseContainerOptions getOptions() {
        if ( !this.mysqlVersion.atLeast(5,7) )  {
            throw new SkipException("skipping gtid on 5.5");
        }

        TestDatabaseContainerOptions options = new TestDatabaseContainerOptions();
        options.gtid = true;
        return options;
    }

    @Test
    public void testGTIDAdvancesStatementBased() throws Exception {
        try {
            master.execute("set global binlog_format=statement");
            slave.execute("stop slave", "set global binlog_format=statement", "start slave");
            master.reconnect();
            master.execute("use test");
            testGTIDAdvances();
        } finally {
            master.execute("set global binlog_format=row");
            slave.execute("stop slave", "set global binlog_format=row", "start slave");
            master.reconnect();
            master.execute("use test");
        }
    }

    @Test
    public void testGTIDAdvances() throws Exception {
        master.execute("CREATE TABLE if not exists foo (i int)");

        String initialGTIDSet = getExecutedGtidSet(master);
        assertNotNull("Initial GTID set is null", initialGTIDSet);

        EventDeserializer eventDeserializer = new EventDeserializer();
        try {
            client.disconnect();
            final BinaryLogClient clientWithKeepAlive = new BinaryLogClient(slave.hostname(), slave.port(),
                slave.username(), slave.password());

            clientWithKeepAlive.setGtidSet(initialGTIDSet);
            clientWithKeepAlive.registerEventListener(eventListener);
            clientWithKeepAlive.setEventDeserializer(eventDeserializer);
            try {
                eventListener.reset();
                clientWithKeepAlive.connect(DEFAULT_TIMEOUT);

                master.execute(new Callback<Statement>() {
                    @Override
                    public void execute(Statement statement) throws SQLException {
                        statement.execute("INSERT INTO foo set i = 2");
                        statement.execute("INSERT INTO foo set i = 3");
                    }
                });

                // Wait for GTID to advance instead of counting events to avoid race conditions in CI
                final String initialGtid = initialGTIDSet;
                org.awaitility.Awaitility.await()
                    .atMost(4, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .until(() -> !initialGtid.equals(clientWithKeepAlive.getGtidSet()));
                String gtidSet = clientWithKeepAlive.getGtidSet();
                assertNotNull(gtidSet);
                assertNotEquals(initialGTIDSet, gtidSet, "Initial GTID set and current GTID set are the same");

                eventListener.reset();

                master.execute(new Callback<Statement>() {
                    @Override
                    public void execute(Statement statement) throws SQLException {
                        statement.execute("INSERT INTO foo set i = 4");
                        statement.execute("INSERT INTO foo set i = 5");
                    }
                });

                // Wait for GTID to advance instead of counting events
                final String gtidAfterFirst = gtidSet;
                org.awaitility.Awaitility.await()
                    .atMost(4, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .until(() -> !gtidAfterFirst.equals(clientWithKeepAlive.getGtidSet()));
                assertNotEquals(clientWithKeepAlive.getGtidSet(), gtidSet, "GTID set before and after INSERT operation are the same");

                gtidSet = clientWithKeepAlive.getGtidSet();

                eventListener.reset();
                final String gtidBeforeDrop = clientWithKeepAlive.getGtidSet();
                master.execute("DROP TABLE IF EXISTS test.bar");
                // Use Awaitility to poll for GTID advancement instead of counting events
                // This avoids race conditions and works for both row-based and statement-based modes
                org.awaitility.Awaitility.await()
                    .atMost(4, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .until(() -> !gtidBeforeDrop.equals(clientWithKeepAlive.getGtidSet()));
                assertNotEquals(clientWithKeepAlive.getGtidSet(), gtidBeforeDrop, "GTID set before and after DROP TABLE operation are the same");
            } finally {
                clientWithKeepAlive.disconnect();
            }
        } finally {
            client.connect(DEFAULT_TIMEOUT);
        }
    }


    @Test
    public void testGtidServerId() throws Exception {
        master.execute("CREATE TABLE if not exists foo (i int)");

        String initialGTIDSet = getExecutedGtidSet(master);

        final String[] expectedServerId = new String[1];
        master.query("select @@server_uuid", new Callback<ResultSet>() {
            @Override
            public void execute(ResultSet rs) throws SQLException {
                rs.next();
                expectedServerId[0] = rs.getString(1);
            }
        });

        final String[] actualServerId = new String[1];

        EventDeserializer eventDeserializer = new EventDeserializer();
        try {
            client.disconnect();
            final BinaryLogClient clientWithKeepAlive = new BinaryLogClient(master.hostname(), master.port(),
                master.username(), master.password());

            clientWithKeepAlive.setGtidSet(initialGTIDSet);

            clientWithKeepAlive.registerEventListener(new BinaryLogClient.EventListener() {
                @Override
                public void onEvent(Event event) {
                    if (event.getHeader().getEventType() == EventType.GTID) {
                        actualServerId[0] = ((GtidEventData) event.getData()).getMySqlGtid().getServerId().toString();
                    }
                }
            });
            clientWithKeepAlive.registerEventListener(eventListener);
            clientWithKeepAlive.setEventDeserializer(eventDeserializer);
            try {
                eventListener.reset();
                clientWithKeepAlive.connect(DEFAULT_TIMEOUT);

                master.execute(new Callback<Statement>() {
                    @Override
                    public void execute(Statement statement) throws SQLException {
                        statement.execute("INSERT INTO foo set i = 2");
                    }
                });

                eventListener.waitForAtLeast(EventType.GTID, 1, TimeUnit.SECONDS.toMillis(4));
                assertEquals(actualServerId[0], expectedServerId[0]);


            } finally {
                clientWithKeepAlive.disconnect();
            }
        } finally {
            client.connect(DEFAULT_TIMEOUT);
        }
    }

    private String getExecutedGtidSet(MySQLConnection master) throws SQLException {
        final String[] initialGTIDSet = new String[1];
        master.query("show master status", new Callback<ResultSet>() {
            @Override
            public void execute(ResultSet rs) throws SQLException {
                rs.next();
                initialGTIDSet[0] = rs.getString("Executed_Gtid_Set");
            }
        });
        return initialGTIDSet[0];
    }

}
