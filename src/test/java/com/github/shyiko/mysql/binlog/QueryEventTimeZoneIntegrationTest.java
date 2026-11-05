package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.QueryEventData;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

public class QueryEventTimeZoneIntegrationTest extends AbstractIntegrationTest {

    @Test
    public void testTimeZoneOfTheSession() throws Exception {
        CapturingEventListener capturingEventListener = new CapturingEventListener();
        client.registerEventListener(capturingEventListener);
        try {
            // the client reads the binary log of the slave, which records the statements again in the same time zone
            master.execute(
                "set time_zone = '+09:00'",
                "create table time_zone_test (id int primary key, ts timestamp null default '2026-09-28 12:34:56')",
                "set time_zone = 'SYSTEM'",
                "alter table time_zone_test alter column ts set default '2026-09-28 12:34:56'",
                "set time_zone = '-05:00'",
                // MariaDB 11.2 and later record the collations of the charsets before the time zone
                "create table time_zone_charset_test (id int primary key, c varchar(10) character set utf8mb4, " +
                    "ts timestamp null default '2026-09-28 12:34:56') default charset = utf8mb4",
                "drop table time_zone_test, time_zone_charset_test");
            capturingEventListener.waitFor(QueryEventData.class, 4, BinaryLogClientIntegrationTest.DEFAULT_TIMEOUT);
        } finally {
            client.unregisterEventListener(capturingEventListener);
        }
        List<QueryEventData> events = capturingEventListener.getEvents(QueryEventData.class);
        assertEquals(find(events, "create table time_zone_test").getTimeZone(), "+09:00");
        assertEquals(find(events, "alter table").getTimeZone(), "SYSTEM");
        assertEquals(find(events, "create table time_zone_charset_test").getTimeZone(), "-05:00");
        // the statement does not depend on the time zone
        assertNull(find(events, "drop table").getTimeZone());
    }

    private static QueryEventData find(List<QueryEventData> events, String statement) {
        for (QueryEventData event : events) {
            if (event.getSql().toLowerCase().startsWith(statement)) {
                return event;
            }
        }
        fail("No query event for " + statement + " in " + events);
        return null;
    }
}
