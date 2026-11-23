package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.MariadbGtidListEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

/**
 * @author <a href="mailto:winger2049@gmail.com">Winger</a>
 */
public class MariadbGtidListEventDataDeserializerTest {

    private static final byte[] DATA = {1, 0, 0, 0, 0, 0, 0, 0, 102, 0, 0, 0, 87, 28, 0, 0, 0, 0, 0, 0, 77};

    private static final String GTID_SET_LIST = "MariadbGtidListEventData{mariaGTIDSet=0-102-7255}";

    @Test
    public void deserialize() throws IOException {
        MariadbGtidListEventDataDeserializer deserializer = new MariadbGtidListEventDataDeserializer();
        MariadbGtidListEventData eventData = deserializer.deserialize(new ByteArrayInputStream(DATA));
        assertEquals(eventData.toString(), GTID_SET_LIST);
    }

    @Test
    public void deserializeUnsignedDomainIdAndServerId() throws IOException {
        // count = 1, domainId = 0xFFFFFFFF, serverId = 0xFFFFFFFE (uint4 values), sequence = 7255
        byte[] data = {1, 0, 0, 0, -1, -1, -1, -1, -2, -1, -1, -1, 87, 28, 0, 0, 0, 0, 0, 0, 77};
        MariadbGtidListEventDataDeserializer deserializer = new MariadbGtidListEventDataDeserializer();
        MariadbGtidListEventData eventData = deserializer.deserialize(new ByteArrayInputStream(data));
        assertEquals(eventData.toString(), "MariadbGtidListEventData{mariaGTIDSet=4294967295-4294967294-7255}");
    }
}
