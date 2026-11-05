package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.network.ServerException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reads the {@link ServerFacts} on a connection that has not requested the binary log stream yet.
 * <p>
 * Every statement needs at most the {@code REPLICATION CLIENT} privilege, except the one for the role of a Group
 * Replication member, which reads {@code performance_schema}. A statement that the server rejects leaves its facts
 * null, while an I/O error is thrown, as the connection can no longer be used.
 */
class ServerFactsReader {

    /**
     * Runs a statement and returns its rows, each of which maps the column names, in any case, to the values.
     */
    interface QueryRunner {
        List<Map<String, String>> query(String sql) throws IOException;
    }

    static final String VARIABLES_QUERY = "show global variables where Variable_name in " +
        "('read_only', 'super_read_only', 'innodb_read_only', 'aurora_version', 'group_replication_group_name')";
    static final String GROUP_REPLICATION_MEMBER_ROLE_QUERY = "select MEMBER_ROLE " +
        "from performance_schema.replication_group_members " +
        "where MEMBER_ID = @@server_uuid and MEMBER_STATE = 'ONLINE'";

    private static final String GROUP_REPLICATION_CHANNEL_PREFIX = "group_replication_";

    private final Logger logger = Logger.getLogger(getClass().getName());

    private final QueryRunner queryRunner;
    private final BinaryLogDatabaseVersion databaseVersion;

    ServerFactsReader(QueryRunner queryRunner, BinaryLogDatabaseVersion databaseVersion) {
        this.queryRunner = queryRunner;
        this.databaseVersion = databaseVersion;
    }

    ServerFacts read() throws IOException {
        boolean mariaDb = databaseVersion.isMariaDb();
        Map<String, String> variables = readVariables();
        String groupName = variables.get("group_replication_group_name");
        return new ServerFacts(
            readOnly(variables.get("read_only")),
            mariaDb ? null : readOnly(variables.get("super_read_only")),
            readOnly(variables.get("innodb_read_only")),
            mariaDb ? null : variables.get("aurora_version"),
            mariaDb || groupName == null || groupName.isEmpty() ? null : readGroupReplicationMemberRole(),
            readReplicationSources(mariaDb));
    }

    // visible for testing
    static String replicaStatusQuery(BinaryLogDatabaseVersion databaseVersion) {
        if (databaseVersion.isMariaDb()) {
            return "show all slaves status";
        }
        return databaseVersion.isGreaterThanOrEqualTo(8, 4) ? "show replica status" : "show slave status";
    }

    private Map<String, String> readVariables() throws IOException {
        Map<String, String> variables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        List<Map<String, String>> rows = query(VARIABLES_QUERY);
        if (rows != null) {
            for (Map<String, String> row : rows) {
                variables.put(row.get("Variable_name"), row.get("Value"));
            }
        }
        return variables;
    }

    private String readGroupReplicationMemberRole() throws IOException {
        List<Map<String, String>> rows = query(GROUP_REPLICATION_MEMBER_ROLE_QUERY);
        return rows == null || rows.isEmpty() ? null : rows.get(0).get("MEMBER_ROLE");
    }

    private List<ServerFacts.ReplicationSource> readReplicationSources(boolean mariaDb) throws IOException {
        List<Map<String, String>> rows = query(replicaStatusQuery(databaseVersion));
        if (rows == null) {
            return null;
        }
        List<ServerFacts.ReplicationSource> sources = new ArrayList<>();
        for (Map<String, String> row : rows) {
            String channel = first(row, "Channel_Name", "Connection_name");
            if (channel == null) {
                channel = "";
            }
            String receiver = first(row, "Replica_IO_Running", "Slave_IO_Running");
            if (receiver == null || "No".equalsIgnoreCase(receiver) ||
                    !mariaDb && channel.startsWith(GROUP_REPLICATION_CHANNEL_PREFIX)) {
                continue;
            }
            sources.add(new ServerFacts.ReplicationSource(channel, first(row, "Source_Host", "Master_Host"),
                port(first(row, "Source_Port", "Master_Port"))));
        }
        return sources;
    }

    private List<Map<String, String>> query(String sql) throws IOException {
        try {
            return queryRunner.query(sql);
        } catch (ServerException e) {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Server facts not read with \"" + sql + "\": " + e.getMessage());
            }
            return null;
        }
    }

    // visible for testing
    static Boolean readOnly(String value) {
        if (value == null) {
            return null;
        }
        return !("OFF".equalsIgnoreCase(value) || "0".equals(value));
    }

    private static String first(Map<String, String> row, String name, String alternative) {
        return row.containsKey(name) ? row.get(name) : row.get(alternative);
    }

    private static int port(String value) {
        try {
            return value == null ? -1 : Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
