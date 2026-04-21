package com.github.shyiko.mysql.binlog;

import org.awaitility.Awaitility;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * Manages MySQL or MariaDB test containers for integration testing.
 * Replaces the legacy MysqlOnetimeServer implementation.
 */
public class TestDatabaseContainer {
    private final Logger logger = Logger.getLogger(getClass().getName());
    private final JdbcDatabaseContainer<?> container;
    private final DatabaseType databaseType;
    private Connection connection;

    public enum DatabaseType {
        MYSQL,
        MARIADB
    }

    /**
     * Creates a new test database container based on the mysql.image system property.
     * Defaults to MySQL 8.0 if not specified.
     * Use Maven profiles: -Pmysql-8.0 or -Pmariadb
     */
    public TestDatabaseContainer() {
        this(new TestDatabaseContainerOptions());
    }

    /**
     * Creates a new test database container with specific options.
     */
    public TestDatabaseContainer(TestDatabaseContainerOptions options) {
        String version = options.version != null ? options.version : getImageFromSystemProperty();

        // Parse image name (e.g., "mysql:8.0" or "mariadb:10.6")
        if (version.toLowerCase().contains("mariadb")) {
            this.databaseType = DatabaseType.MARIADB;
            DockerImageName imageName = DockerImageName.parse(version).asCompatibleSubstituteFor("mariadb");
            this.container = new MariaDBContainer<>(imageName)
                    .withDatabaseName("mysql")
                    .withUsername("root")
                    .withPassword("");
        } else {
            this.databaseType = DatabaseType.MYSQL;
            DockerImageName imageName = DockerImageName.parse(version).asCompatibleSubstituteFor("mysql");
            this.container = new MySQLContainer<>(imageName)
                    .withDatabaseName("mysql")
                    .withUsername("root")
                    .withPassword("")
                    .withCommand("--default-time-zone=+00:00");
        }

        // Configure network if provided (for master-slave setups)
        if (options.network != null) {
            container.withNetwork(options.network);
            if (options.networkAlias != null) {
                container.withNetworkAliases(options.networkAlias);
            }
        }

        // Build all command parameters together
        List<String> commands = new java.util.ArrayList<>();

        // Configure container based on options
        if (options.gtid) {
            logger.info("Enabling GTID mode");
            commands.add("--gtid-mode=ON");
            commands.add("--log-slave-updates=ON");
            commands.add("--enforce-gtid-consistency=true");
        }

        commands.add("--log-bin=master");
        commands.add("--binlog_format=row");
        commands.add("--server-id=" + options.serverID);
        commands.add("--default-time-zone=+00:00");

        // Add full row metadata for MySQL 8.0+
        if (options.fullRowMetaData && !version.toLowerCase().startsWith("mariadb")) {
            MysqlVersion mysqlVersion = parseVersion(version);
            if (mysqlVersion.atLeast(8, 0)) {
                commands.add("--binlog-row-metadata=FULL");
            }
        }

        // Allow larger data packets for testing
        commands.add("--max_allowed_packet=16M");

        // Add extra parameters if provided
        if (options.extraParams != null && !options.extraParams.isEmpty()) {
            String[] extraParamArray = options.extraParams.trim().split("\\s+");
            commands.addAll(Arrays.asList(extraParamArray));
        }

        // Apply all commands at once
        container.withCommand(commands.toArray(new String[0]));
    }

    private static String getImageFromSystemProperty() {
        String mysqlImage = System.getProperty("mysql.image");
        return mysqlImage == null ? "mysql:8.0" : mysqlImage;
    }

    private static MysqlVersion parseVersion(String imageTag) {
        if (imageTag.toLowerCase().contains("mariadb")) {
            return new MysqlVersion(0, 0, true);
        } else {
            // Extract version from image tag, handling registry prefixes
            // e.g., "container-registry.oracle.com/mysql/community-server:8.0" -> "8.0"
            // or "mysql:8.0" -> "8.0"
            String version = "8.0"; // default
            if (imageTag.contains(":")) {
                // Get the part after the last colon (the version tag)
                version = imageTag.substring(imageTag.lastIndexOf(':') + 1);
            }
            String[] parts = version.split("\\.");
            return new MysqlVersion(Integer.valueOf(parts[0]), Integer.valueOf(parts[1]), false);
        }
    }

    /**
     * Starts the container and initializes the database.
     */
    public void start() throws Exception {
        logger.info("Starting " + databaseType + " container...");
        container.start();
        logger.info("Container started on port " + container.getMappedPort(3306));

        // Initialize connection and create test user
        resetConnection();

        // Create maxwell user with replication privileges
        // Use mysql_native_password for MySQL 8.0+ to avoid SSL requirement for replication
        // Drop user first to ensure clean state
        try {
            this.connection.createStatement().executeUpdate("DROP USER IF EXISTS 'maxwell'@'%'");
        } catch (SQLException e) {
            // Ignore if user doesn't exist
        }

        if (databaseType == DatabaseType.MYSQL) {
            this.connection.createStatement().executeUpdate(
                "CREATE USER 'maxwell'@'%' IDENTIFIED WITH mysql_native_password BY 'maxwell'");
        } else {
            this.connection.createStatement().executeUpdate(
                "CREATE USER 'maxwell'@'%' IDENTIFIED BY 'maxwell'");
        }
        this.connection.createStatement().executeUpdate("GRANT REPLICATION SLAVE ON *.* TO 'maxwell'@'%'");
        this.connection.createStatement().executeUpdate("GRANT REPLICATION CLIENT ON *.* TO 'maxwell'@'%'");
        this.connection.createStatement().executeUpdate("GRANT ALL ON *.* TO 'maxwell'@'%'");
        this.connection.createStatement().executeUpdate("FLUSH PRIVILEGES");
        this.connection.createStatement().executeUpdate("CREATE DATABASE IF NOT EXISTS test");

        logger.info("Container initialized successfully");
    }

    /**
     * Stops and removes the container.
     */
    public void stop() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.warning("Error closing connection: " + e.getMessage());
            }
        }
        if (container != null && container.isRunning()) {
            logger.info("Stopping container...");
            container.stop();
        }
    }

    /**
     * Gets the mapped port for MySQL/MariaDB.
     */
    public int getPort() {
        return container.getMappedPort(3306);
    }

    /**
     * Gets the host (always localhost for Testcontainers).
     */
    public String getHost() {
        return container.getHost();
    }

    /**
     * Resets the connection to the database.
     */
    public void resetConnection() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        this.connection = getNewConnection();
    }

    /**
     * Creates a new connection to the database.
     */
    public Connection getNewConnection() throws SQLException {
        String jdbcUrl = container.getJdbcUrl() + "?zeroDateTimeBehavior=convertToNull&useSSL=false";
        return DriverManager.getConnection(jdbcUrl, "root", "");
    }

    /**
     * Gets the current connection.
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Gets a connection to a specific database.
     */
    public Connection getConnection(String defaultDB) throws SQLException {
        Connection conn = getNewConnection();
        conn.setCatalog(defaultDB);
        return conn;
    }

    /**
     * Executes a SQL statement.
     */
    public void execute(String query) throws SQLException {
        Statement s = getConnection().createStatement();
        s.executeUpdate(query);
        s.close();
    }

    /**
     * Executes a list of SQL statements.
     */
    public void executeList(List<String> queries) throws SQLException {
        for (String q : queries) {
            if (q.matches("^\\s*$")) {
                continue;
            }
            execute(q);
        }
    }

    /**
     * Executes a list of SQL statements.
     */
    public void executeList(String[] schemaSQL) throws SQLException {
        executeList(Arrays.asList(schemaSQL));
    }

    /**
     * Executes a query and returns the result set.
     */
    public ResultSet query(String sql) throws SQLException {
        return getConnection().createStatement().executeQuery(sql);
    }

    /**
     * Gets the database type (MySQL or MariaDB).
     */
    public DatabaseType getDatabaseType() {
        return databaseType;
    }

    /**
     * Gets the MySQL version information.
     */
    public static MysqlVersion getVersion() {
        String imageTag = getImageFromSystemProperty();
        return parseVersion(imageTag);
    }

    /**
     * Dumps a query result as a string (for debugging).
     */
    public String dumpQuery(String query) throws SQLException {
        String result = "";
        ResultSet rs = getConnection().createStatement().executeQuery(query);
        rs.next();
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            Object val = rs.getObject(i);
            String asString = val == null ? "null" : val.toString();
            result = result + rs.getMetaData().getColumnName(i) + ": " + asString + "\n";
        }
        rs.close();
        return result;
    }

    /**
     * Sets up a slave replication from a master container.
     */
    public void setupSlave(TestDatabaseContainer master) throws SQLException {
        Connection masterConn = master.getConnection();
        ResultSet rs = masterConn.createStatement().executeQuery("SHOW MASTER STATUS");
        if (!rs.next()) {
            throw new RuntimeException("Could not get master status");
        }

        String file = rs.getString("File");
        Long position = rs.getLong("Position");
        rs.close();

        // Use the container's internal network IP address for container-to-container communication
        // This works reliably across all Testcontainers versions
        String masterHost = master.container.getContainerInfo().getNetworkSettings()
            .getNetworks().values().iterator().next().getIpAddress();

        logger.info("Master container IP: " + masterHost);

        String changeSQL = String.format(
            "CHANGE MASTER TO master_host = '%s', master_user='maxwell', master_password='maxwell', " +
            "master_log_file = '%s', master_log_pos = %d, master_port = %d",
            masterHost, file, position, 3306
        );

        logger.info("Starting up slave: " + changeSQL);
        getConnection().createStatement().execute(changeSQL);
        getConnection().createStatement().execute("START SLAVE");

        ResultSet status = query("SHOW SLAVE STATUS");
        if (!status.next()) {
            throw new RuntimeException("Could not get slave status");
        }

        if (status.getString("Slave_IO_Running").equals("No") ||
            status.getString("Slave_SQL_Running").equals("No")) {
            throw new RuntimeException("Could not start slave: " + dumpQuery("SHOW SLAVE STATUS"));
        }
        status.close();
    }

    /**
     * Waits for the slave to catch up with the master using Awaitility.
     */
    public void waitForSlaveToBeCurrent(TestDatabaseContainer master) throws Exception {
        ResultSet ms = master.query("SHOW MASTER STATUS");
        ms.next();
        final String masterFile = ms.getString("File");
        final Long masterPos = ms.getLong("Position");
        ms.close();

        logger.info("Waiting for slave to catch up: master file=" + masterFile + ", position=" + masterPos);

        Awaitility.await()
            .atMost(Duration.ofSeconds(30))
            .pollInterval(Duration.ofMillis(200))
            .pollDelay(Duration.ZERO)
            .ignoreExceptions()
            .until(() -> {
                try (ResultSet rs = query("SHOW SLAVE STATUS")) {
                    if (!rs.next()) {
                        logger.warning("Slave status not available");
                        return false;
                    }

                    String slaveIORunning = rs.getString("Slave_IO_Running");
                    String slaveSQLRunning = rs.getString("Slave_SQL_Running");
                    String relayMasterLogFile = rs.getString("Relay_Master_Log_File");
                    Long execMasterLogPos = rs.getLong("Exec_Master_Log_Pos");

                    logger.info("Slave status: IO=" + slaveIORunning + ", SQL=" + slaveSQLRunning +
                               ", file=" + relayMasterLogFile + ", pos=" + execMasterLogPos);

                    if (!"Yes".equals(slaveIORunning) || !"Yes".equals(slaveSQLRunning)) {
                        throw new RuntimeException("Slave replication not running: " + dumpQuery("SHOW SLAVE STATUS"));
                    }

                    boolean caughtUp = relayMasterLogFile.equals(masterFile) && execMasterLogPos >= masterPos;
                    if (caughtUp) {
                        logger.info("Slave caught up with master");
                    }
                    return caughtUp;
                }
            });
    }
}
