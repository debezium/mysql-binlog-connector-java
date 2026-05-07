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
        final GtidSet gtidSet = new GtidSet("mytag:24bc7850-2c16-11e6-a073-0242ac110002:1-5");

        final Collection<GtidSet.UUIDSet> uuidSets = gtidSet.getUUIDSets();
        assertEquals(uuidSets.size(), 1);

        final GtidSet.UUIDSet uuidSet = uuidSets.iterator().next();
        assertEquals(uuidSet.getServerId().toString(), "24bc7850-2c16-11e6-a073-0242ac110002");
        assertEquals(uuidSet.getIntervals().size(), 1);
        assertEquals(uuidSet.getIntervals().get(0).getStart(), 1L);
        assertEquals(uuidSet.getIntervals().get(0).getEnd(), 5L);
    }

    @Test
    public void testParseTaggedGtidSetWithMultipleIntervals() {
        // Test parsing tagged GTID set with multiple intervals
        final GtidSet gtidSet = new GtidSet("prod:24bc7850-2c16-11e6-a073-0242ac110002:1-5:10-15:20");

        final Collection<GtidSet.UUIDSet> uuidSets = gtidSet.getUUIDSets();
        assertEquals(uuidSets.size(), 1);

        final GtidSet.UUIDSet uuidSet = uuidSets.iterator().next();
        assertEquals(uuidSet.getServerId().toString(), "24bc7850-2c16-11e6-a073-0242ac110002");
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
            "mytag:aae57b2f-8e44-11ee-a3d6-a036bcda1a41:1-10"
        );

        final Collection<GtidSet.UUIDSet> uuidSets = gtidSet.getUUIDSets();
        assertEquals(uuidSets.size(), 2);

        // Verify both UUIDs are present
        assertNotNull(gtidSet.getUUIDSet("24bc7850-2c16-11e6-a073-0242ac110002"));
        assertNotNull(gtidSet.getUUIDSet("aae57b2f-8e44-11ee-a3d6-a036bcda1a41"));
    }

    @Test
    public void testParseMixedGtidSetMultipleServers() {
        // Test parsing complex GTID set with multiple servers, some tagged
        final GtidSet gtidSet = new GtidSet(
            "24bc7850-2c16-11e6-a073-0242ac110002:1-5:10," +
            "tag1:aae57b2f-8e44-11ee-a3d6-a036bcda1a41:1-10:15-20," +
            "994ab859-8ea8-11ee-a568-a036bcda1a41:1-3," +
            "tag2:bd9794e0-1d65-11ed-a7e7-0adb305b3a12:5-9"
        );

        final Collection<GtidSet.UUIDSet> uuidSets = gtidSet.getUUIDSets();
        assertEquals(uuidSets.size(), 4);
    }

    @Test
    public void testAddTaggedMySqlGtid() {
        // Test adding tagged GTID to set
        final GtidSet gtidSet = new GtidSet("");
        gtidSet.addGtid(MySqlGtid.fromString("mytag:00000000-0000-0000-0000-000000000000:2"));

        // Note: GtidSet toString doesn't include tags, only UUID and intervals
        assertEquals(gtidSet.toString(), "00000000-0000-0000-0000-000000000000:2-2");
    }

    @Test
    public void testAddTaggedMySqlGtidToExistingSet() {
        // Test adding tagged GTID to existing set
        final GtidSet gtidSet = new GtidSet("00000000-0000-0000-0000-000000000000:1");
        gtidSet.addGtid(MySqlGtid.fromString("mytag:00000000-0000-0000-0000-000000000000:2"));

        assertEquals(gtidSet.toString(), "00000000-0000-0000-0000-000000000000:1-2");
    }

    @Test
    public void testParseTaggedGtidSetWithComplexTag() {
        // Test parsing GTID set with complex tag name
        final GtidSet gtidSet = new GtidSet("prod-db-01:24bc7850-2c16-11e6-a073-0242ac110002:1-100");

        final Collection<GtidSet.UUIDSet> uuidSets = gtidSet.getUUIDSets();
        assertEquals(uuidSets.size(), 1);

        final GtidSet.UUIDSet uuidSet = uuidSets.iterator().next();
        assertEquals(uuidSet.getServerId().toString(), "24bc7850-2c16-11e6-a073-0242ac110002");
        assertEquals(uuidSet.getIntervals().size(), 1);
        assertEquals(uuidSet.getIntervals().get(0).getStart(), 1L);
        assertEquals(uuidSet.getIntervals().get(0).getEnd(), 100L);
    }

    @Test
    public void testParseTaggedGtidSetBackwardCompatibility() {
        // Verify that non-tagged GTIDs still work exactly as before
        final GtidSet legacySet = new GtidSet("24bc7850-2c16-11e6-a073-0242ac110002:1-5");
        final GtidSet taggedSet = new GtidSet("tag:24bc7850-2c16-11e6-a073-0242ac110002:1-5");

        // Both should have the same UUID and intervals
        final GtidSet.UUIDSet legacyUuidSet = legacySet.getUUIDSet("24bc7850-2c16-11e6-a073-0242ac110002");
        final GtidSet.UUIDSet taggedUuidSet = taggedSet.getUUIDSet("24bc7850-2c16-11e6-a073-0242ac110002");

        assertEquals(legacyUuidSet.getServerId(), taggedUuidSet.getServerId());
        assertEquals(legacyUuidSet.getIntervals(), taggedUuidSet.getIntervals());
    }

}
