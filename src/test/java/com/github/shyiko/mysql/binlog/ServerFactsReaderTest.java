package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.network.ServerException;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class ServerFactsReaderTest {

    private static final BinaryLogDatabaseVersion MYSQL_84 = BinaryLogDatabaseVersion.parse("8.4.6");
    private static final BinaryLogDatabaseVersion MYSQL_80 = BinaryLogDatabaseVersion.parse("8.0.43");
    private static final BinaryLogDatabaseVersion MARIADB = BinaryLogDatabaseVersion.parse("12.3.3-MariaDB-log");

    @Test
    public void testMySqlReplica() throws Exception {
        FakeServer server = new FakeServer()
            .variables("read_only", "ON", "super_read_only", "ON", "innodb_read_only", "OFF",
                "group_replication_group_name", "")
            .rows("show replica status",
                row("Channel_Name", "", "Replica_IO_Running", "Yes", "Source_Host", "primary", "Source_Port", "3306"),
                row("Channel_Name", "second", "Replica_IO_Running", "Connecting", "Source_Host", "other",
                    "Source_Port", "3307"),
                row("Channel_Name", "stopped", "Replica_IO_Running", "No", "Source_Host", "old", "Source_Port", "3306"),
                row("Channel_Name", "group_replication_applier", "Replica_IO_Running", "Yes", "Source_Host", "<NULL>",
                    "Source_Port", "0"));

        ServerFacts facts = new ServerFactsReader(server, MYSQL_84).read();

        assertEquals(facts.getReadOnly(), Boolean.TRUE);
        assertEquals(facts.getSuperReadOnly(), Boolean.TRUE);
        assertEquals(facts.getInnodbReadOnly(), Boolean.FALSE);
        assertNull(facts.getAuroraVersion());
        assertNull(facts.getGroupReplicationMemberRole());
        assertEquals(facts.getReplicationSources(), Arrays.asList(
            new ServerFacts.ReplicationSource("", "primary", 3306),
            new ServerFacts.ReplicationSource("second", "other", 3307)));
        assertFalse(server.queries.contains(ServerFactsReader.GROUP_REPLICATION_MEMBER_ROLE_QUERY));
    }

    @Test
    public void testMySqlSourceBefore84() throws Exception {
        FakeServer server = new FakeServer()
            .variables("read_only", "OFF", "super_read_only", "OFF", "innodb_read_only", "OFF")
            .rows("show slave status");

        ServerFacts facts = new ServerFactsReader(server, MYSQL_80).read();

        assertEquals(facts.getReadOnly(), Boolean.FALSE);
        assertEquals(facts.getReplicationSources(), Collections.emptyList());
    }

    @Test
    public void testMySqlReplicaBefore84() throws Exception {
        FakeServer server = new FakeServer()
            .variables("read_only", "ON")
            .rows("show slave status",
                row("Channel_Name", "", "Slave_IO_Running", "Yes", "Master_Host", "primary", "Master_Port", "3306"));

        ServerFacts facts = new ServerFactsReader(server, MYSQL_80).read();

        assertEquals(facts.getReplicationSources(),
            Collections.singletonList(new ServerFacts.ReplicationSource("", "primary", 3306)));
    }

    @Test
    public void testAurora() throws Exception {
        // Aurora serves binary logs only from the writer, whose innodb_read_only is OFF
        FakeServer writer = new FakeServer()
            .variables("read_only", "OFF", "super_read_only", "OFF", "innodb_read_only", "OFF",
                "aurora_version", "8.4.7")
            .rows("show replica status");

        ServerFacts facts = new ServerFactsReader(writer, BinaryLogDatabaseVersion.parse("8.4.7")).read();

        assertEquals(facts.getAuroraVersion(), "8.4.7");
        assertEquals(facts.getInnodbReadOnly(), Boolean.FALSE);
        assertEquals(facts.getReplicationSources(), Collections.emptyList());

        FakeServer reader = new FakeServer()
            .variables("read_only", "OFF", "super_read_only", "OFF", "innodb_read_only", "ON",
                "aurora_version", "3.05.2")
            .rows("show slave status");

        facts = new ServerFactsReader(reader, BinaryLogDatabaseVersion.parse("8.0.32")).read();

        assertEquals(facts.getAuroraVersion(), "3.05.2");
        assertEquals(facts.getInnodbReadOnly(), Boolean.TRUE);
    }

    @Test
    public void testGroupReplicationMember() throws Exception {
        FakeServer server = new FakeServer()
            .variables("read_only", "ON", "super_read_only", "ON", "innodb_read_only", "OFF",
                "group_replication_group_name", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
            .rows(ServerFactsReader.GROUP_REPLICATION_MEMBER_ROLE_QUERY, row("MEMBER_ROLE", "SECONDARY"))
            .rows("show replica status",
                row("Channel_Name", "group_replication_applier", "Replica_IO_Running", "Yes"));

        ServerFacts facts = new ServerFactsReader(server, MYSQL_84).read();

        assertEquals(facts.getGroupReplicationMemberRole(), "SECONDARY");
        assertEquals(facts.getReplicationSources(), Collections.emptyList());
    }

    @Test
    public void testMariaDbReplica() throws Exception {
        FakeServer server = new FakeServer()
            .variables("read_only", "NO_LOCK", "innodb_read_only", "OFF",
                // not read on MariaDB, even if a server reported them
                "super_read_only", "ON", "aurora_version", "x", "group_replication_group_name", "x")
            .rows("show all slaves status",
                row("Connection_name", "", "Slave_IO_Running", "Yes", "Master_Host", "primary", "Master_Port", "3306",
                    "Last_IO_Error", null),
                row("Connection_name", "group_replication_x", "Slave_IO_Running", "Preparing",
                    "Master_Host", "other", "Master_Port", "3307"));

        ServerFacts facts = new ServerFactsReader(server, MARIADB).read();

        assertEquals(facts.getReadOnly(), Boolean.TRUE);
        assertNull(facts.getSuperReadOnly());
        assertNull(facts.getAuroraVersion());
        assertNull(facts.getGroupReplicationMemberRole());
        assertEquals(facts.getReplicationSources(), Arrays.asList(
            new ServerFacts.ReplicationSource("", "primary", 3306),
            new ServerFacts.ReplicationSource("group_replication_x", "other", 3307)));
        assertFalse(server.queries.contains(ServerFactsReader.GROUP_REPLICATION_MEMBER_ROLE_QUERY));
    }

    @Test
    public void testRejectedStatementsLeaveTheirFactsNull() throws Exception {
        FakeServer server = new FakeServer()
            .variables("read_only", "OFF", "super_read_only", "OFF", "innodb_read_only", "OFF")
            .rejects("show replica status");

        ServerFacts facts = new ServerFactsReader(server, MYSQL_84).read();

        assertEquals(facts.getReadOnly(), Boolean.FALSE);
        assertNull(facts.getReplicationSources());

        facts = new ServerFactsReader(new FakeServer().rejects(ServerFactsReader.VARIABLES_QUERY)
            .rows("show replica status"), MYSQL_84).read();

        assertNull(facts.getReadOnly());
        assertNull(facts.getSuperReadOnly());
        assertNull(facts.getInnodbReadOnly());
        assertEquals(facts.getReplicationSources(), Collections.emptyList());
    }

    @Test
    public void testIoErrorFails() throws Exception {
        FakeServer server = new FakeServer().variables("read_only", "OFF").breaks("show replica status");
        try {
            new ServerFactsReader(server, MYSQL_84).read();
            fail("Expected the I/O error to be thrown");
        } catch (IOException e) {
            assertFalse(e instanceof ServerException);
        }
    }

    @Test
    public void testReplicaStatusQuery() {
        assertEquals(ServerFactsReader.replicaStatusQuery(MYSQL_84), "show replica status");
        assertEquals(ServerFactsReader.replicaStatusQuery(BinaryLogDatabaseVersion.parse("9.5.0")),
            "show replica status");
        assertEquals(ServerFactsReader.replicaStatusQuery(MYSQL_80), "show slave status");
        assertEquals(ServerFactsReader.replicaStatusQuery(MARIADB), "show all slaves status");
    }

    @Test
    public void testReadOnlyValues() {
        assertNull(ServerFactsReader.readOnly(null));
        assertEquals(ServerFactsReader.readOnly("OFF"), Boolean.FALSE);
        assertEquals(ServerFactsReader.readOnly("off"), Boolean.FALSE);
        assertEquals(ServerFactsReader.readOnly("0"), Boolean.FALSE);
        assertEquals(ServerFactsReader.readOnly("ON"), Boolean.TRUE);
        assertEquals(ServerFactsReader.readOnly("1"), Boolean.TRUE);
        assertEquals(ServerFactsReader.readOnly("NO_LOCK_NO_ADMIN"), Boolean.TRUE);
    }

    @Test
    public void testFactsAreImmutable() throws Exception {
        ServerFacts facts = new ServerFactsReader(new FakeServer().rows("show replica status"), MYSQL_84).read();
        try {
            facts.getReplicationSources().add(new ServerFacts.ReplicationSource("", "x", 1));
            fail("Expected the sources to be unmodifiable");
        } catch (UnsupportedOperationException expected) {
            assertTrue(facts.getReplicationSources().isEmpty());
        }
    }

    private static Map<String, String> row(String... namesAndValues) {
        Map<String, String> row = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < namesAndValues.length; i += 2) {
            row.put(namesAndValues[i], namesAndValues[i + 1]);
        }
        return row;
    }

    /**
     * Answers the statements of {@link ServerFactsReader} with the configured rows, a rejection or an I/O error. Any
     * other statement is rejected, as by a server that does not know it.
     */
    private static class FakeServer implements ServerFactsReader.QueryRunner {

        private final Map<String, List<Map<String, String>>> results = new HashMap<>();
        private final List<String> broken = new ArrayList<>();
        private final List<String> queries = new ArrayList<>();

        FakeServer variables(String... namesAndValues) {
            List<Map<String, String>> rows = new ArrayList<>();
            for (int i = 0; i < namesAndValues.length; i += 2) {
                rows.add(row("Variable_name", namesAndValues[i], "Value", namesAndValues[i + 1]));
            }
            results.put(ServerFactsReader.VARIABLES_QUERY, rows);
            return this;
        }

        @SafeVarargs
        final FakeServer rows(String sql, Map<String, String>... rows) {
            results.put(sql, Arrays.asList(rows));
            return this;
        }

        FakeServer rejects(String sql) {
            results.remove(sql);
            return this;
        }

        FakeServer breaks(String sql) {
            broken.add(sql);
            return this;
        }

        @Override
        public List<Map<String, String>> query(String sql) throws IOException {
            queries.add(sql);
            if (broken.contains(sql)) {
                throw new IOException("Connection reset");
            }
            List<Map<String, String>> rows = results.get(sql);
            if (rows == null) {
                throw new ServerException("Access denied", 1227, "42000");
            }
            return rows;
        }
    }
}
