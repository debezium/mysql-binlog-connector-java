package com.github.shyiko.mysql.binlog;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Facts about the server that provides the binary log stream, which {@link BinaryLogClient} reads on the binary log
 * connection before it requests the stream (see {@link BinaryLogClient#setServerFactsEnabled(boolean)}).
 * <p>
 * The facts are reported as the server states them. They do not tell whether the server is a primary or a replica,
 * which depends on the topology. A fact is null when the server does not have it, such as {@code super_read_only} on
 * MariaDB, or when it could not be read, for example because the user may not run the statement that reports it.
 */
public class ServerFacts {

    private final Boolean readOnly;
    private final Boolean superReadOnly;
    private final Boolean innodbReadOnly;
    private final String auroraVersion;
    private final String groupReplicationMemberRole;
    private final List<ReplicationSource> replicationSources;

    public ServerFacts(Boolean readOnly, Boolean superReadOnly, Boolean innodbReadOnly, String auroraVersion,
                       String groupReplicationMemberRole, List<ReplicationSource> replicationSources) {
        this.readOnly = readOnly;
        this.superReadOnly = superReadOnly;
        this.innodbReadOnly = innodbReadOnly;
        this.auroraVersion = auroraVersion;
        this.groupReplicationMemberRole = groupReplicationMemberRole;
        this.replicationSources = replicationSources == null ? null :
            Collections.unmodifiableList(replicationSources);
    }

    /**
     * @return {@code read_only}; on MariaDB, where it can also be {@code NO_LOCK} or {@code NO_LOCK_NO_ADMIN}, every
     * value other than {@code OFF} is read-only
     */
    public Boolean getReadOnly() {
        return readOnly;
    }

    /**
     * @return {@code super_read_only}, which MySQL has and MariaDB does not
     */
    public Boolean getSuperReadOnly() {
        return superReadOnly;
    }

    /**
     * @return {@code innodb_read_only}, which Amazon Aurora MySQL sets to {@code ON} on its reader instances and to
     * {@code OFF} on the writer instance. As Aurora serves binary logs only from the writer, it is {@code OFF} on a
     * binary log connection to Aurora.
     * @see <a href="https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/AuroraMySQL.BestPractices.html">
     *     Determining which DB instance you are connected to</a>
     */
    public Boolean getInnodbReadOnly() {
        return innodbReadOnly;
    }

    /**
     * @return the {@code aurora_version} system variable, such as {@code 3.05.2} or {@code 8.4.7}, on Amazon Aurora
     * MySQL, null on other servers
     * @see <a href="https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraMySQL.Updates.Versions.html">
     *     Checking Aurora MySQL version numbers</a>
     */
    public String getAuroraVersion() {
        return auroraVersion;
    }

    /**
     * @return {@code MEMBER_ROLE} of the server, such as {@code PRIMARY} or {@code SECONDARY}, if it is an online
     * member of a MySQL Group Replication group; null if it is not, or if that could not be read
     */
    public String getGroupReplicationMemberRole() {
        return groupReplicationMemberRole;
    }

    /**
     * @return the sources that the server replicates from over a running (or connecting) replication channel, or a
     * MariaDB replication connection, other than the channels of Group Replication; empty if there is none, null if
     * the replica status could not be read
     */
    public List<ReplicationSource> getReplicationSources() {
        return replicationSources;
    }

    @Override
    public String toString() {
        return "ServerFacts{readOnly=" + readOnly +
            ", superReadOnly=" + superReadOnly +
            ", innodbReadOnly=" + innodbReadOnly +
            ", auroraVersion=" + auroraVersion +
            ", groupReplicationMemberRole=" + groupReplicationMemberRole +
            ", replicationSources=" + replicationSources +
            '}';
    }

    /**
     * A source that the server replicates from.
     */
    public static class ReplicationSource {

        private final String channel;
        private final String host;
        private final int port;

        public ReplicationSource(String channel, String host, int port) {
            this.channel = channel;
            this.host = host;
            this.port = port;
        }

        /**
         * @return name of the replication channel (MySQL) or connection (MariaDB), empty for the default one
         */
        public String getChannel() {
            return channel;
        }

        public String getHost() {
            return host;
        }

        /**
         * @return port of the source, -1 if it is not known
         */
        public int getPort() {
            return port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ReplicationSource)) {
                return false;
            }
            ReplicationSource that = (ReplicationSource) o;
            return port == that.port && Objects.equals(channel, that.channel) && Objects.equals(host, that.host);
        }

        @Override
        public int hashCode() {
            return Objects.hash(channel, host, port);
        }

        @Override
        public String toString() {
            return (channel == null || channel.isEmpty() ? "" : channel + "=") + host + ":" + port;
        }
    }
}
