package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.MariadbGtidEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;

/**
 * @author <a href="mailto:winger2049@gmail.com">Winger</a>
 */
public class MariadbGtidEventDataDeserializerTest {

    private static final byte[] DATA = {-20, 28, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 121};

    private static final String GTID_SET = "0-0-7404";

    @Test
    public void deserialize() throws IOException {
        MariadbGtidEventDataDeserializer deserializer = new MariadbGtidEventDataDeserializer();
        MariadbGtidEventData eventData = deserializer.deserialize(new ByteArrayInputStream(DATA));
        assertEquals(eventData.toString(), GTID_SET);
    }

    @Test
    public void deserializeUnsignedDomainId() throws IOException {
        // sequence = 7404, domainId = 0xFFFFFFFF (uint4 max), flags = 12
        byte[] data = {-20, 28, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1, 12, 0, 0, 0, 0, 0, 0, 121};
        MariadbGtidEventDataDeserializer deserializer = new MariadbGtidEventDataDeserializer();
        MariadbGtidEventData eventData = deserializer.deserialize(new ByteArrayInputStream(data));
        assertEquals(eventData.getDomainId(), 4294967295L);
        assertEquals(eventData.getSequence(), 7404L);
    }
}
