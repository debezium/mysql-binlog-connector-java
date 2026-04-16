package com.github.shyiko.mysql.binlog;

import org.testcontainers.containers.Network;

/**
 * Configuration options for TestDatabaseContainer.
 * Replaces MysqlOnetimeServerOptions.
 */
public class TestDatabaseContainerOptions {
    /**
     * Database version to use (e.g., "5.7", "8.0", "mariadb", "mariadb-10.6").
     * If null, uses MYSQL_VERSION environment variable or defaults to "5.7".
     */
    public String version;

    /**
     * Server ID for replication.
     */
    public int serverID = 1;

    /**
     * Enable GTID mode.
     */
    public boolean gtid = false;

    /**
     * Enable full row metadata (MySQL 8.0+).
     */
    public boolean fullRowMetaData = false;

    /**
     * Extra MySQL/MariaDB parameters to pass to the container.
     */
    public String extraParams;

    /**
     * Master server for replication setup (optional).
     */
    public TestDatabaseContainer masterServer;

    /**
     * Docker network for container-to-container communication (optional).
     * Required for master-slave replication setups.
     */
    public Network network;

    /**
     * Network alias for this container (optional).
     * Used to allow other containers to connect by name instead of IP.
     */
    public String networkAlias;

    public TestDatabaseContainerOptions() {
    }
}
