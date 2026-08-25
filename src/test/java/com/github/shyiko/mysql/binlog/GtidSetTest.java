/*
 * Copyright 2015 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.GtidSet.Interval;
import com.github.shyiko.mysql.binlog.GtidSet.UUIDSet;
import com.github.shyiko.mysql.binlog.event.MySqlGtid;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.LinkedList;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class GtidSetTest {

    private static final String UUID = "24bc7850-2c16-11e6-a073-0242ac110002";

    @Test
    public void testAdd() throws Exception {
        GtidSet gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:3-5");
        gtidSet.add("00000000-0000-0000-0000-000000000000:2");
        gtidSet.add("00000000-0000-0000-0000-000000000000:4");
        gtidSet.add("00000000-0000-0000-0000-000000000000:5");
        gtidSet.add("00000000-0000-0000-0000-000000000000:7");
        gtidSet.add("00000000-0000-0000-0000-000000000001:9");
        gtidSet.add("00000000-0000-0000-0000-000000000000:0");
        assertEquals(gtidSet.toString(),
            "00000000-0000-0000-0000-000000000000:0-0:2-5:7-7,00000000-0000-0000-0000-000000000001:9-9");
    }

    @Test
    public void testJoin() throws Exception {
        GtidSet gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:3-4:6-7");
        gtidSet.add("00000000-0000-0000-0000-000000000000:5");
        assertEquals(gtidSet.getUUIDSets().iterator().next().getIntervals().iterator().next().getEnd(), 7);
        assertEquals(gtidSet.toString(), "00000000-0000-0000-0000-000000000000:3-7");
    }

    @Test
    public void testEmptySet() throws Exception {
        assertEquals(new GtidSet("").toString(), "");
    }

    @Test
    public void testEquals() {
        assertEquals(new GtidSet(""), new GtidSet(null));
        assertEquals(new GtidSet(""), new GtidSet(""));
        assertEquals(new GtidSet(UUID + ":1-191"), new GtidSet(UUID + ":1-191"));
        assertEquals(new GtidSet(UUID + ":1-191:192-199"), new GtidSet(UUID + ":1-191:192-199"));
        assertEquals(new GtidSet(UUID + ":1-191:192-199"), new GtidSet(UUID + ":1-199"));
        assertEquals(new GtidSet(UUID + ":1-191:193-199"), new GtidSet(UUID + ":1-191:193-199"));
        assertNotEquals(new GtidSet(UUID + ":1-191:193-199"), new GtidSet(UUID + ":1-199"));
    }

    @Test
    public void testSubsetOf() {
        GtidSet[] set = {
            new GtidSet(""),
            new GtidSet(UUID + ":1-191"),
            new GtidSet(UUID + ":192-199"),
            new GtidSet(UUID + ":1-191:192-199"),
            new GtidSet(UUID + ":1-191:193-199"),
            new GtidSet(UUID + ":2-199"),
            new GtidSet(UUID + ":1-200")
        };
        byte[][] subsetMatrix = {
            {1, 1, 1, 1, 1, 1, 1},
            {0, 1, 0, 1, 1, 0, 1},
            {0, 0, 1, 1, 0, 1, 1},
            {0, 0, 0, 1, 0, 0, 1},
            {0, 0, 0, 1, 1, 0, 1},
            {0, 0, 0, 1, 0, 1, 1},
            {0, 0, 0, 0, 0, 0, 1},
        };
        for (int i = 0; i < subsetMatrix.length; i++) {
            byte[] subset = subsetMatrix[i];
            for (int j = 0; j < subset.length; j++) {
                assertEquals(set[i].isContainedWithin(set[j]), subset[j] == 1,
                    "\"" + set[i] + "\" was expected to be a subset of \"" + set[j] +  "\"" +
                        " (" + i + "," + j + ")");
            }
        }
    }

    @Test
    public void testSingleInterval() {
        GtidSet gtidSet = new GtidSet(UUID + ":1-191");
        UUIDSet uuidSet = gtidSet.getUUIDSet(UUID);
        assertEquals(uuidSet.getIntervals().size(), 1);
        assertTrue(uuidSet.getIntervals().contains(new Interval(1, 191)));
        assertEquals(uuidSet.getIntervals().iterator().next(), new Interval(1, 191));
        assertEquals(new LinkedList<Interval>(uuidSet.getIntervals()).getLast(), new Interval(1, 191));
        assertEquals(gtidSet.toString(), UUID + ":1-191");
    }

    @Test
    public void testCollapseAdjacentIntervals() {
        GtidSet gtidSet = new GtidSet(UUID + ":1-191:192-199");
        UUIDSet uuidSet = gtidSet.getUUIDSet(UUID);
        assertEquals(uuidSet.getIntervals().size(), 1);
        assertTrue(uuidSet.getIntervals().contains(new Interval(1, 199)));
        assertEquals(uuidSet.getIntervals().iterator().next(), new Interval(1, 199));
        assertEquals(new LinkedList<Interval>(uuidSet.getIntervals()).getLast(), new Interval(1, 199));
        assertEquals(gtidSet.toString(), UUID + ":1-199");
    }

    @Test
    public void testNotCollapseNonAdjacentIntervals() {
        GtidSet gtidSet = new GtidSet(UUID + ":1-191:193-199");
        UUIDSet uuidSet = gtidSet.getUUIDSet(UUID);
        assertEquals(uuidSet.getIntervals().size(), 2);
        assertEquals(uuidSet.getIntervals().iterator().next(), new Interval(1, 191));
        assertEquals(new LinkedList<Interval>(uuidSet.getIntervals()).getLast(), new Interval(193, 199));
        assertEquals(gtidSet.toString(), UUID + ":1-191:193-199");
    }

    @Test
    public void testMultipleIntervals() {
        GtidSet set = new GtidSet(UUID + ":1-191:193-199:1000-1033");
        UUIDSet uuidSet = set.getUUIDSet(UUID);
        assertEquals(uuidSet.getIntervals().size(), 3);
        assertTrue(uuidSet.getIntervals().contains(new Interval(193, 199)));
        assertEquals(uuidSet.getIntervals().iterator().next(), new Interval(1, 191));
        assertEquals(new LinkedList<Interval>(uuidSet.getIntervals()).getLast(), new Interval(1000, 1033));
        assertEquals(set.toString(), UUID + ":1-191:193-199:1000-1033");
    }

    @Test
    public void testMultipleIntervalsThatMayBeAdjacent() {
        GtidSet gtidSet = new GtidSet(UUID + ":1-191:192-199:1000-1033:1035-1036:1038-1039");
        UUIDSet uuidSet = gtidSet.getUUIDSet(UUID);
        assertEquals(uuidSet.getIntervals().size(), 4);
        assertTrue(uuidSet.getIntervals().contains(new Interval(1000, 1033)));
        assertTrue(uuidSet.getIntervals().contains(new Interval(1035, 1036)));
        assertEquals(uuidSet.getIntervals().iterator().next(), new Interval(1, 199));
        assertEquals(new LinkedList<Interval>(uuidSet.getIntervals()).getLast(), new Interval(1038, 1039));
        assertEquals(gtidSet.toString(), UUID + ":1-199:1000-1033:1035-1036:1038-1039");
    }

    @Test
    public void testPutUUIDSet() {
        GtidSet gtidSet = new GtidSet(UUID + ":1-191");
        UUIDSet uuidSet = gtidSet.getUUIDSet(UUID);
        GtidSet gtidSet2 = new GtidSet(UUID + ":1-190");
        UUIDSet uuidSet2 = gtidSet2.getUUIDSet(UUID);
        gtidSet.putUUIDSet(uuidSet2);
        assertEquals(gtidSet, gtidSet2);
    }

    @Test
    public void testAddStringGtid() {
        GtidSet gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:1");
        gtidSet.addGtid("00000000-0000-0000-0000-000000000000:2");
        assertEquals("00000000-0000-0000-0000-000000000000:1-2", gtidSet.toString());
    }

    @Test
    public void testAddMySqlGtid() {
        GtidSet gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:1");
        gtidSet.addGtid(MySqlGtid.fromString("00000000-0000-0000-0000-000000000000:2"));
        assertEquals("00000000-0000-0000-0000-000000000000:1-2", gtidSet.toString());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testAddAnotherObjectAsGtidFails() {
        GtidSet gtidSet = new GtidSet("");
        gtidSet.addGtid(MariadbGtidSet.MariaGtid.parse("1-2-3"));
    }

    @Test
    public void testParseTaggedGtidSet() {
        // Test parsing GTID set with tagged GTIDs (MySQL 8.3+)
        final GtidSet gtidSet = new GtidSet("24bc7850-2c16-11e6-a073-0242ac110002:mytag:1-5");

        final Collection<GtidSet.UUIDSet> uuidSets = gtidSet.getUUIDSets();
        assertEquals(uuidSets.size(), 1);

        final GtidSet.UUIDSet uuidSet = uuidSets.iterator().next();
        assertEquals(uuidSet.getServerId().toString(), "24bc7850-2c16-11e6-a073-0242ac110002");
        assertEquals(uuidSet.getTag(), "mytag");
        assertEquals(uuidSet.getIntervals().size(), 1);
        assertEquals(uuidSet.getIntervals().get(0).getStart(), 1L);
        assertEquals(uuidSet.getIntervals().get(0).getEnd(), 5L);
        assertEquals(gtidSet.toString(), "24bc7850-2c16-11e6-a073-0242ac110002:mytag:1-5");
    }

    @Test
    public void testParseTaggedGtidSetWithMultipleIntervals() {
        // Test parsing tagged GTID set with multiple intervals
        final GtidSet gtidSet = new GtidSet("24bc7850-2c16-11e6-a073-0242ac110002:prod:1-5:10-15:20");

        final Collection<GtidSet.UUIDSet> uuidSets = gtidSet.getUUIDSets();
        assertEquals(uuidSets.size(), 1);

        final GtidSet.UUIDSet uuidSet = uuidSets.iterator().next();
        assertEquals(uuidSet.getServerId().toString(), "24bc7850-2c16-11e6-a073-0242ac110002");
        assertEquals(uuidSet.getTag(), "prod");
        assertEquals(uuidSet.getIntervals().size(), 3);
        assertEquals(uuidSet.getIntervals().get(0).getStart(), 1L);
        assertEquals(uuidSet.getIntervals().get(0).getEnd(), 5L);
        assertEquals(uuidSet.getIntervals().get(1).getStart(), 10L);
        assertEquals(uuidSet.getIntervals().get(1).getEnd(), 15L);
        assertEquals(uuidSet.getIntervals().get(2).getStart(), 20L);
        assertEquals(uuidSet.getIntervals().get(2).getEnd(), 20L);
    }

    @Test
    public void testParseMixedGtidSet() {
        // Test parsing GTID set with both tagged and non-tagged GTIDs
        final GtidSet gtidSet = new GtidSet(
            "24bc7850-2c16-11e6-a073-0242ac110002:1-5," +
            "aae57b2f-8e44-11ee-a3d6-a036bcda1a41:mytag:1-10"
        );

        final Collection<GtidSet.UUIDSet> uuidSets = gtidSet.getUUIDSets();
        assertEquals(uuidSets.size(), 2);

        // Verify both UUIDs are present
        assertNotNull(gtidSet.getUUIDSet("24bc7850-2c16-11e6-a073-0242ac110002"));
        assertNotNull(gtidSet.getUUIDSet("aae57b2f-8e44-11ee-a3d6-a036bcda1a41", "mytag"));
    }

    @Test
    public void testParseExecutedGtidSetWithTaggedIntervals() {
        final GtidSet gtidSet = new GtidSet(
            "24bc7850-2c16-11e6-a073-0242ac110002:1-20:testtag:1-3"
        );

        assertEquals(gtidSet.toString(), "24bc7850-2c16-11e6-a073-0242ac110002:1-20:testtag:1-3");
    }

    @Test
    public void testParseExecutedGtidSetWithSeparatedTaggedIntervals() {
        final GtidSet gtidSet = new GtidSet(
            "24bc7850-2c16-11e6-a073-0242ac110002:1-5:testtag:7-9:othertag:6"
        );

        // Tags are emitted in alphabetical order (othertag < testtag), untagged first.
        assertEquals(gtidSet.toString(), "24bc7850-2c16-11e6-a073-0242ac110002:1-5:othertag:6-6:testtag:7-9");
    }

    @Test
    public void testParseMixedGtidSetMultipleServers() {
        // Test parsing complex GTID set with multiple servers, some tagged
        final GtidSet gtidSet = new GtidSet(
            "24bc7850-2c16-11e6-a073-0242ac110002:1-5:10," +
            "aae57b2f-8e44-11ee-a3d6-a036bcda1a41:tag1:1-10:15-20," +
            "994ab859-8ea8-11ee-a568-a036bcda1a41:1-3," +
            "bd9794e0-1d65-11ed-a7e7-0adb305b3a12:tag2:5-9"
        );

        final Collection<GtidSet.UUIDSet> uuidSets = gtidSet.getUUIDSets();
        assertEquals(uuidSets.size(), 4);
    }

    @Test
    public void testAddTaggedMySqlGtid() {
        // Test adding tagged GTID to set
        final GtidSet gtidSet = new GtidSet("");
        gtidSet.addGtid(MySqlGtid.fromString("00000000-0000-0000-0000-000000000000:mytag:2"));

        assertEquals(gtidSet.toString(), "00000000-0000-0000-0000-000000000000:mytag:2-2");
    }

    @Test
    public void testAddTaggedMySqlGtidToExistingSet() {
        // Test adding tagged GTID to existing set
        final GtidSet gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:1");
        gtidSet.addGtid(MySqlGtid.fromString("00000000-0000-0000-0000-000000000000:mytag:2"));

        assertEquals(gtidSet.toString(), "00000000-0000-0000-0000-000000000000:1-1:mytag:2-2");
    }

    @Test
    public void testParseTaggedGtidSetWithComplexTag() {
        // Test parsing GTID set with complex tag name
        final GtidSet gtidSet = new GtidSet("24bc7850-2c16-11e6-a073-0242ac110002:prod_db_01:1-100");

        final Collection<GtidSet.UUIDSet> uuidSets = gtidSet.getUUIDSets();
        assertEquals(uuidSets.size(), 1);

        final GtidSet.UUIDSet uuidSet = uuidSets.iterator().next();
        assertEquals(uuidSet.getServerId().toString(), "24bc7850-2c16-11e6-a073-0242ac110002");
        assertEquals(uuidSet.getTag(), "prod_db_01");
        assertEquals(uuidSet.getIntervals().size(), 1);
        assertEquals(uuidSet.getIntervals().get(0).getStart(), 1L);
        assertEquals(uuidSet.getIntervals().get(0).getEnd(), 100L);
    }

    @Test
    public void testParseTaggedAndUntaggedGtidSetsAreDistinct() {
        final GtidSet legacySet = new GtidSet("24bc7850-2c16-11e6-a073-0242ac110002:1-5");
        final GtidSet taggedSet = new GtidSet("24bc7850-2c16-11e6-a073-0242ac110002:tag:1-5");

        // Tagged and untagged GTIDs from the same UUID are different TSIDs.
        final GtidSet.UUIDSet legacyUuidSet = legacySet.getUUIDSet("24bc7850-2c16-11e6-a073-0242ac110002");

        assertNotNull(legacyUuidSet);
        assertEquals(legacyUuidSet.getTag(), null);
        assertEquals(taggedSet.toString(), "24bc7850-2c16-11e6-a073-0242ac110002:tag:1-5");
        assertNotEquals(legacySet, taggedSet);
    }

    @Test
    public void testParsePreviousTagFirstGtidSetFormat() {
        final GtidSet gtidSet = new GtidSet("tag:24bc7850-2c16-11e6-a073-0242ac110002:1-5");

        final GtidSet.UUIDSet uuidSet = gtidSet.getUUIDSet("24bc7850-2c16-11e6-a073-0242ac110002", "tag");
        assertNotNull(uuidSet);
        assertEquals(uuidSet.getIntervals().get(0), new Interval(1, 5));
        assertEquals(gtidSet.toString(), "24bc7850-2c16-11e6-a073-0242ac110002:tag:1-5");
    }

    /**
     * When a tagged interval is added before an untagged interval for the same UUID,
     * {@code toString()} must still emit the untagged interval first. MySQL's
     * {@code SET @@GLOBAL.gtid_purged} and {@code CHANGE MASTER TO} commands require
     * this ordering.
     */
    @Test
    public void testToStringAlwaysEmitsUntaggedIntervalBeforeTaggedForSameUuid() {
        final GtidSet gtidSet = new GtidSet("");
        // Add the tagged entry FIRST (the degenerate / adversarial order)
        gtidSet.addGtid(MySqlGtid.fromString(UUID + ":prod:3"));
        // Then add the untagged entry
        gtidSet.addGtid(MySqlGtid.fromString(UUID + ":1"));

        // toString() groups all entries for the same UUID into one string:
        //   uuid:untagged_intervals:tag:tagged_intervals
        // e.g. 24bc7850-...:1-1:prod:3-3
        final String result = gtidSet.toString();
        final int untaggedIndex = result.indexOf(":1-1");
        final int taggedIndex = result.indexOf(":prod:");
        assertTrue(untaggedIndex >= 0, "toString() must contain the untagged interval: " + result);
        assertTrue(taggedIndex >= 0, "toString() must contain the tagged interval: " + result);
        assertTrue(untaggedIndex < taggedIndex,
            "Untagged interval must appear before tagged interval in toString() output: " + result);
    }

    /**
     * When the natural insertion order already puts the untagged entry first (the common case),
     * {@code toString()} must also be correct and stable.
     */
    @Test
    public void testToStringNaturalOrderUntaggedBeforeTagged() {
        // Untagged added first — the common path; ordering must be preserved.
        final GtidSet gtidSet = new GtidSet(UUID + ":1-5," + UUID + ":mytag:10-15");

        final String result = gtidSet.toString();
        final int untaggedIndex = result.indexOf(UUID + ":1-5");
        final int taggedIndex = result.indexOf(":mytag:");
        assertTrue(untaggedIndex >= 0, "toString() must contain the untagged interval");
        assertTrue(taggedIndex >= 0, "toString() must contain the tagged interval");
        assertTrue(untaggedIndex < taggedIndex,
            "Untagged interval must appear before tagged interval: " + result);
    }

    /**
     * When multiple tags are present for the same UUID, untagged must still come first,
     * followed by tagged entries in their natural sort order.
     */
    @Test
    public void testToStringMultipleTagsUntaggedFirst() {
        final GtidSet gtidSet = new GtidSet("");
        // Insert in adversarial order: z-tag, then a-tag, then untagged
        gtidSet.addGtid(MySqlGtid.fromString(UUID + ":ztag:5"));
        gtidSet.addGtid(MySqlGtid.fromString(UUID + ":atag:3"));
        gtidSet.addGtid(MySqlGtid.fromString(UUID + ":1"));

        final String result = gtidSet.toString();
        final int untaggedIndex = result.indexOf(UUID + ":1-1");
        assertTrue(untaggedIndex >= 0, "toString() must contain the untagged interval: " + result);
        // Both tagged entries must come after the untagged one within the UUID group
        final int atagIndex = result.indexOf(":atag:");
        final int ztagIndex = result.indexOf(":ztag:");
        assertTrue(untaggedIndex < atagIndex,
            "Untagged must precede :atag: in: " + result);
        assertTrue(untaggedIndex < ztagIndex,
            "Untagged must precede :ztag: in: " + result);
    }
}
