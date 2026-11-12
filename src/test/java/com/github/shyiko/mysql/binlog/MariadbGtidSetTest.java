package com.github.shyiko.mysql.binlog;

import com.github.shyiko.mysql.binlog.MariadbGtidSet.MariaGtid;
import com.github.shyiko.mysql.binlog.event.MySqlGtid;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author <a href="mailto:winger2049@gmail.com">Winger</a>
 */
public class MariadbGtidSetTest {

    @Test
    public void testAdd() {
        MariadbGtidSet gtidSet = new MariadbGtidSet("0-102-7255");
        gtidSet.add("0-102-7256");
        gtidSet.add("0-102-7257");
        gtidSet.add("0-102-7259");
        gtidSet.add("1-102-7300");
        assertNotEquals(gtidSet.toString(), "1-102-7300");
        assertNotEquals(gtidSet.toString(), "0-102-7259");
        assertEquals(gtidSet.toString(), "0-102-7259,1-102-7300");
    }

    @Test
    public void testEmptySet() {
        assertEquals(new MariadbGtidSet("").toString(), "");
    }

    @Test
    public void testEquals() {
        assertEquals(new MariadbGtidSet(""), new MariadbGtidSet(null));
        assertEquals(new MariadbGtidSet(""), new MariadbGtidSet(""));
        assertEquals(new MariadbGtidSet("0-0-7404"), new MariadbGtidSet("0-0-7404"));
    }

    @Test
    public void testMatcher() {
        assertTrue(MariadbGtidSet.isMariaGtidSet("0-0-3323"));
        assertTrue(MariadbGtidSet.isMariaGtidSet("0-0-3323,4-33-12342134,444-33-13412341233"));
        assertTrue(MariadbGtidSet.isMariaGtidSet("0-0-3323, 4-33-12342134, 444-33-13412341233"));
        assertFalse(MariadbGtidSet.isMariaGtidSet("07212070-4330-3bc8-8a3a-01e34be47bc3:1-141692942,a0c4a949-fae8-30f3-a4d2-fee56a1a9307:1-1427643460,a16ef643-1d4a-3fd9-a86e-1adeb836eb2d:1-1411988930,b0d822f4-5a84-30d3-a929-61f64740d7ac:1-59364"));
    }

    @Test
    public void testAddStringGtid() {
        MariadbGtidSet gtidSet = new MariadbGtidSet();
        gtidSet.addGtid("1-2-3");
        assertEquals("1-2-3", gtidSet.toString());
    }

    @Test
    public void testAddMariadbGtid() {
        MariadbGtidSet gtidSet = new MariadbGtidSet();
        gtidSet.addGtid(MariaGtid.parse("1-2-3"));
        assertEquals("1-2-3", gtidSet.toString());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testAddAnotherObjectAsGtidFails() {
        MariadbGtidSet gtidSet = new MariadbGtidSet();
        gtidSet.addGtid(MySqlGtid.fromString("00000000-0000-0000-0000-000000000000:2"));
    }


}
