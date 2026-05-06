package com.github.shyiko.mysql.binlog.event;

import org.testng.annotations.Test;

import java.util.UUID;

import static org.testng.Assert.*;

/**
 * Unit tests for MySqlGtid class, including MySQL 8.3+ tagged GTID support.
 */
public class MySqlGtidTest {

    @Test
    public void testParseLegacyFormat() {
        // Test backward compatibility with non-tagged GTIDs
        final MySqlGtid gtid = MySqlGtid.fromString("24bc7850-2c16-11e6-a073-0242ac110002:11");

        assertNull(gtid.getTag());
        assertEquals(gtid.getServerId().toString(), "24bc7850-2c16-11e6-a073-0242ac110002");
        assertEquals(gtid.getTransactionId(), 11L);
        assertEquals(gtid.toString(), "24bc7850-2c16-11e6-a073-0242ac110002:11");
    }

    @Test
    public void testParseTaggedFormat() {
        // Test MySQL 8.3+ tagged GTID format
        final MySqlGtid gtid = MySqlGtid.fromString("mytag:24bc7850-2c16-11e6-a073-0242ac110002:11");

        assertEquals(gtid.getTag(), "mytag");
        assertEquals(gtid.getServerId().toString(), "24bc7850-2c16-11e6-a073-0242ac110002");
        assertEquals(gtid.getTransactionId(), 11L);
        assertEquals(gtid.toString(), "mytag:24bc7850-2c16-11e6-a073-0242ac110002:11");
    }

    @Test
    public void testParseTaggedFormatWithComplexTag() {
        // Test with more complex tag names
        final MySqlGtid gtid = MySqlGtid.fromString("prod-db-01:aae57b2f-8e44-11ee-a3d6-a036bcda1a41:42");

        assertEquals(gtid.getTag(), "prod-db-01");
        assertEquals(gtid.getServerId().toString(), "aae57b2f-8e44-11ee-a3d6-a036bcda1a41");
        assertEquals(gtid.getTransactionId(), 42L);
        assertEquals(gtid.toString(), "prod-db-01:aae57b2f-8e44-11ee-a3d6-a036bcda1a41:42");
    }

    @Test
    public void testParseTaggedFormatWithNumericTag() {
        // Test with numeric tag
        final MySqlGtid gtid = MySqlGtid.fromString("12345:994ab859-8ea8-11ee-a568-a036bcda1a41:3");

        assertEquals(gtid.getTag(), "12345");
        assertEquals(gtid.getServerId().toString(), "994ab859-8ea8-11ee-a568-a036bcda1a41");
        assertEquals(gtid.getTransactionId(), 3L);
        assertEquals(gtid.toString(), "12345:994ab859-8ea8-11ee-a568-a036bcda1a41:3");
    }

    @Test
    public void testParseTaggedFormatWithUnderscoreTag() {
        // Test with underscore in tag
        final MySqlGtid gtid = MySqlGtid.fromString("my_tag_123:bd9794e0-1d65-11ed-a7e7-0adb305b3a12:9");

        assertEquals(gtid.getTag(), "my_tag_123");
        assertEquals(gtid.getServerId().toString(), "bd9794e0-1d65-11ed-a7e7-0adb305b3a12");
        assertEquals(gtid.getTransactionId(), 9L);
        assertEquals(gtid.toString(), "my_tag_123:bd9794e0-1d65-11ed-a7e7-0adb305b3a12:9");
    }

    @Test
    public void testConstructorWithoutTag() {
        // Test backward compatible constructor
        final UUID uuid = UUID.fromString("24bc7850-2c16-11e6-a073-0242ac110002");
        final MySqlGtid gtid = new MySqlGtid(uuid, 11L);

        assertNull(gtid.getTag());
        assertEquals(gtid.getServerId(), uuid);
        assertEquals(gtid.getTransactionId(), 11L);
        assertEquals(gtid.toString(), "24bc7850-2c16-11e6-a073-0242ac110002:11");
    }

    @Test
    public void testConstructorWithTag() {
        // Test new constructor with tag
        final UUID uuid = UUID.fromString("24bc7850-2c16-11e6-a073-0242ac110002");
        final MySqlGtid gtid = new MySqlGtid("mytag", uuid, 11L);

        assertEquals(gtid.getTag(), "mytag");
        assertEquals(gtid.getServerId(), uuid);
        assertEquals(gtid.getTransactionId(), 11L);
        assertEquals(gtid.toString(), "mytag:24bc7850-2c16-11e6-a073-0242ac110002:11");
    }

    @Test
    public void testConstructorWithEmptyTag() {
        // Test that empty tag is treated as no tag in toString
        final UUID uuid = UUID.fromString("24bc7850-2c16-11e6-a073-0242ac110002");
        final MySqlGtid gtid = new MySqlGtid("", uuid, 11L);

        assertEquals(gtid.getTag(), "");
        assertEquals(gtid.toString(), "24bc7850-2c16-11e6-a073-0242ac110002:11");
    }

    @Test
    public void testConstructorWithNullTag() {
        // Test that null tag works correctly
        final UUID uuid = UUID.fromString("24bc7850-2c16-11e6-a073-0242ac110002");
        final MySqlGtid gtid = new MySqlGtid(null, uuid, 11L);

        assertNull(gtid.getTag());
        assertEquals(gtid.toString(), "24bc7850-2c16-11e6-a073-0242ac110002:11");
    }

    @Test
    public void testParseLegacyFormatWithLargeTransactionId() {
        // Test with large transaction ID
        final MySqlGtid gtid = MySqlGtid.fromString("24bc7850-2c16-11e6-a073-0242ac110002:9223372036854775807");

        assertNull(gtid.getTag());
        assertEquals(gtid.getServerId().toString(), "24bc7850-2c16-11e6-a073-0242ac110002");
        assertEquals(gtid.getTransactionId(), Long.MAX_VALUE);
    }

    @Test
    public void testParseTaggedFormatWithLargeTransactionId() {
        // Test tagged format with large transaction ID
        final MySqlGtid gtid = MySqlGtid.fromString("tag:24bc7850-2c16-11e6-a073-0242ac110002:9223372036854775807");

        assertEquals(gtid.getTag(), "tag");
        assertEquals(gtid.getServerId().toString(), "24bc7850-2c16-11e6-a073-0242ac110002");
        assertEquals(gtid.getTransactionId(), Long.MAX_VALUE);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParseInvalidFormatTooFewParts() {
        // Test error handling for invalid format (only UUID, no transaction ID)
        MySqlGtid.fromString("24bc7850-2c16-11e6-a073-0242ac110002");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParseInvalidFormatTooManyParts() {
        // Test error handling for too many parts
        MySqlGtid.fromString("tag:uuid:123:extra");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParseInvalidUuidInLegacyFormat() {
        // Test error handling for invalid UUID in legacy format
        MySqlGtid.fromString("invalid-uuid:11");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParseInvalidUuidInTaggedFormat() {
        // Test error handling for invalid UUID in tagged format
        MySqlGtid.fromString("mytag:invalid-uuid:11");
    }

    @Test(expectedExceptions = NumberFormatException.class)
    public void testParseInvalidTransactionIdInLegacyFormat() {
        // Test error handling for invalid transaction ID in legacy format
        MySqlGtid.fromString("24bc7850-2c16-11e6-a073-0242ac110002:notanumber");
    }

    @Test(expectedExceptions = NumberFormatException.class)
    public void testParseInvalidTransactionIdInTaggedFormat() {
        // Test error handling for invalid transaction ID in tagged format
        MySqlGtid.fromString("mytag:24bc7850-2c16-11e6-a073-0242ac110002:notanumber");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testParseEmptyString() {
        // Test error handling for empty string
        MySqlGtid.fromString("");
    }

    @Test
    public void testRoundTripLegacyFormat() {
        // Test that parsing and toString are inverse operations for legacy format
        final String original = "24bc7850-2c16-11e6-a073-0242ac110002:11";
        final MySqlGtid gtid = MySqlGtid.fromString(original);
        assertEquals(gtid.toString(), original);
    }

    @Test
    public void testRoundTripTaggedFormat() {
        // Test that parsing and toString are inverse operations for tagged format
        final String original = "mytag:24bc7850-2c16-11e6-a073-0242ac110002:11";
        final MySqlGtid gtid = MySqlGtid.fromString(original);
        assertEquals(gtid.toString(), original);
    }
}
