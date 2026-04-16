package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import org.testcontainers.containers.Network;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.TimeZone;

public abstract class AbstractIntegrationTest {
    protected MySQLConnection master;
    protected MySQLConnection slave;
    protected BinaryLogClient client;
    protected CountDownEventListener eventListener;
    protected MysqlVersion mysqlVersion;
    protected TestDatabaseContainer masterContainer;
    protected TestDatabaseContainer slaveContainer;
    protected Network network;

    protected TestDatabaseContainerOptions getOptions() {
        TestDatabaseContainerOptions options = new TestDatabaseContainerOptions();
        options.fullRowMetaData = true;
        return options;
    }

    @BeforeClass
    public void setUp() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        mysqlVersion = TestDatabaseContainer.getVersion();

        // Create a shared network for master-slave communication
        network = Network.newNetwork();

        // Configure master with network and alias
        TestDatabaseContainerOptions masterOptions = getOptions();
        masterOptions.network = network;
        masterOptions.networkAlias = "mysql-master";
        masterOptions.serverID = 1;
        masterContainer = new TestDatabaseContainer(masterOptions);

        // Configure slave with same network but different server ID
        TestDatabaseContainerOptions slaveOptions = getOptions();
        slaveOptions.network = network;
        slaveOptions.networkAlias = "mysql-slave";
        slaveOptions.serverID = 2;  // Different server ID for slave
        slaveContainer = new TestDatabaseContainer(slaveOptions);

        masterContainer.start();
        slaveContainer.start();
        slaveContainer.setupSlave(masterContainer);

        master = new MySQLConnection(masterContainer.getHost(), masterContainer.getPort(), "root", "");
        slave = new MySQLConnection(slaveContainer.getHost(), slaveContainer.getPort(), "root", "");

        client = new BinaryLogClient(slave.hostname, slave.port, slave.username, slave.password);
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY,
            EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG);
        client.setEventDeserializer(eventDeserializer);
        client.setServerId(client.getServerId() - 1); // avoid clashes between BinaryLogClient instances
        client.setKeepAlive(false);
        client.registerEventListener(new TraceEventListener());
        client.registerEventListener(eventListener = new CountDownEventListener());
        client.registerLifecycleListener(new TraceLifecycleListener());
        client.connect(BinaryLogClientIntegrationTest.DEFAULT_TIMEOUT);
        master.execute(new BinaryLogClientIntegrationTest.Callback<Statement>() {
            @Override
            public void execute(Statement statement) throws SQLException {
                statement.execute("drop database if exists mbcj_test");
                statement.execute("create database mbcj_test");
                statement.execute("use mbcj_test");
            }
        });
        // Wait for slave to replicate the commands from master
        slaveContainer.waitForSlaveToBeCurrent(masterContainer);
        eventListener.waitFor(EventType.QUERY, 2, BinaryLogClientIntegrationTest.DEFAULT_TIMEOUT);

        if ( mysqlVersion.atLeast(8, 0) ) {
            setupMysql8Login(master);
            // Wait for slave to replicate the MySQL 8 login setup
            slaveContainer.waitForSlaveToBeCurrent(masterContainer);
            eventListener.waitFor(EventType.QUERY, 2, BinaryLogClientIntegrationTest.DEFAULT_TIMEOUT);
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDownContainers() throws Exception {
        if (slaveContainer != null) {
            slaveContainer.stop();
        }
        if (masterContainer != null) {
            masterContainer.stop();
        }
    }

    protected void setupMysql8Login(MySQLConnection server) throws Exception {
        server.execute("create user 'mysql8' IDENTIFIED WITH caching_sha2_password BY 'testpass'");
        server.execute("grant replication slave, replication client on *.* to 'mysql8'");
    }
}
