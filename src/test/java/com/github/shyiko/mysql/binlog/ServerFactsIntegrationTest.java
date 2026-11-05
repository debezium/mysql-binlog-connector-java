package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class ServerFactsIntegrationTest extends AbstractIntegrationTest {

    @Test
    public void testFactsAreNotReadByDefault() {
        assertTrue(client.isConnected());
        assertNull(client.getServerFacts());
    }

    @Test
    public void testFactsOfReplica() throws Exception {
        ServerFacts facts = connectAndReadFacts(slave, "root", "");

        List<ServerFacts.ReplicationSource> sources = facts.getReplicationSources();
        assertEquals(sources.size(), 1);
        assertEquals(sources.get(0).getChannel(), "");
        assertNotNull(sources.get(0).getHost());
        assertEquals(sources.get(0).getPort(), 3306);
        assertFactsOfServer(facts);
    }

    @Test
    public void testFactsOfSource() throws Exception {
        ServerFacts facts = connectAndReadFacts(master, "root", "");

        assertEquals(facts.getReplicationSources().size(), 0);
        assertEquals(facts.getReadOnly(), Boolean.FALSE);
        assertFactsOfServer(facts);
    }

    @Test
    public void testFactsAreReadOnEveryConnect() throws Exception {
        BinaryLogClient factsClient = newFactsClient(master, "root", "");
        try {
            factsClient.connect(BinaryLogClientIntegrationTest.DEFAULT_TIMEOUT);
            assertEquals(factsClient.getServerFacts().getReadOnly(), Boolean.FALSE);
            factsClient.disconnect();

            master.execute("set global read_only = 1");
            factsClient.connect(BinaryLogClientIntegrationTest.DEFAULT_TIMEOUT);
            assertEquals(factsClient.getServerFacts().getReadOnly(), Boolean.TRUE);
        } finally {
            master.execute("set global read_only = 0");
            factsClient.disconnect();
        }
        // kept while disconnected
        assertEquals(factsClient.getServerFacts().getReadOnly(), Boolean.TRUE);
    }

    @Test
    public void testRejectedStatementsDoNotFailTheConnect() throws Exception {
        master.execute("drop user if exists 'nofacts'");
        master.execute("create user 'nofacts' identified by 'testpass'");
        master.execute("grant replication slave on *.* to 'nofacts'");

        BinaryLogClient factsClient = newFactsClient(master, "nofacts", "testpass");
        // without a position, the client reads the binary log status, which needs REPLICATION CLIENT too
        startAtCurrentPosition(factsClient);
        ServerFacts facts = connectAndReadFacts(factsClient);

        // the replica status needs REPLICATION CLIENT, while the variables need no privilege
        assertNull(facts.getReplicationSources());
        assertEquals(facts.getReadOnly(), Boolean.FALSE);
    }

    private void startAtCurrentPosition(final BinaryLogClient factsClient) throws Exception {
        String status = !mysqlVersion.isMaria && mysqlVersion.atLeast(8, 4) ? "show binary log status" :
            "show master status";
        master.query(status, rs -> {
            rs.next();
            factsClient.setBinlogFilename(rs.getString("File"));
            factsClient.setBinlogPosition(rs.getLong("Position"));
        });
    }

    private void assertFactsOfServer(ServerFacts facts) {
        assertNotNull(facts.getReadOnly());
        assertNotNull(facts.getInnodbReadOnly());
        assertNull(facts.getAuroraVersion());
        assertNull(facts.getGroupReplicationMemberRole());
        if (mysqlVersion.isMaria) {
            assertNull(facts.getSuperReadOnly());
        } else if (mysqlVersion.atLeast(5, 7)) {
            assertNotNull(facts.getSuperReadOnly());
        }
    }

    /**
     * Connects with server facts on, checks that the stream starts after they are read, and returns the facts that
     * were available to {@link BinaryLogClient.LifecycleListener#onConnect(BinaryLogClient)}.
     */
    private ServerFacts connectAndReadFacts(MySQLConnection server, String username, String password)
            throws Exception {
        return connectAndReadFacts(newFactsClient(server, username, password));
    }

    private ServerFacts connectAndReadFacts(BinaryLogClient factsClient) throws Exception {
        final AtomicReference<ServerFacts> factsOnConnect = new AtomicReference<>();
        factsClient.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
            @Override
            public void onConnect(BinaryLogClient client) {
                factsOnConnect.set(client.getServerFacts());
            }
        });
        CountDownEventListener events = new CountDownEventListener();
        factsClient.registerEventListener(events);
        try {
            factsClient.connect(BinaryLogClientIntegrationTest.DEFAULT_TIMEOUT);
            events.waitFor(EventType.FORMAT_DESCRIPTION, 1, BinaryLogClientIntegrationTest.DEFAULT_TIMEOUT);
        } finally {
            factsClient.disconnect();
        }
        assertNotNull(factsOnConnect.get());
        return factsOnConnect.get();
    }

    private BinaryLogClient newFactsClient(MySQLConnection server, String username, String password) {
        BinaryLogClient factsClient = new BinaryLogClient(server.hostname, server.port, username, password);
        factsClient.setServerId(client.getServerId() - 1);
        factsClient.setKeepAlive(false);
        factsClient.setSSLMode(SSLMode.PREFERRED);
        factsClient.setServerFactsEnabled(true);
        return factsClient;
    }
}
