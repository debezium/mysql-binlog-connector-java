/*
 * Copyright Debezium Authors.
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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BinaryLogDatabaseVersionTest {

    @DataProvider(name = "heartbeatV2Versions")
    public Object[][] heartbeatV2Versions() {
        return new Object[][]{
            {"8.0.27", false},
            {"8.0.28", true},
            {"8.0.29", true},
            {"8.4.0", true},
            {"9.0.0", true},
            {"7.9.99", false},
            {"8.0.28-commercial", true},
            {"8.0.28-log", true},
            {"8.0.28+vendor", true}
        };
    }

    @Test(dataProvider = "heartbeatV2Versions")
    public void comparesPatchSpecificVersions(String serverVersion, boolean supported) {
        BinaryLogDatabaseVersion version = BinaryLogDatabaseVersion.parse(serverVersion);
        assertEquals(version.isGreaterThanOrEqualTo(8, 0, 28), supported);
        assertEquals(version.getServerVersion(), serverVersion);
        assertFalse(version.isMariaDb());
    }

    @DataProvider(name = "unknownPatchVersions")
    public Object[][] unknownPatchVersions() {
        return new Object[][]{
            {null},
            {""},
            {"not-a-version"},
            {"8"},
            {"8.0"},
            {"8a0.28"},
            {"8.0.bad"},
            {"8.0.28garbage"},
            {"999999999999999999999.0.28"},
            {"8.0.999999999999999999999"},
            {"8.4"}
        };
    }

    @Test(dataProvider = "unknownPatchVersions")
    public void unknownPatchVersionDoesNotEstablishFeatureSupport(String serverVersion) {
        assertFalse(BinaryLogDatabaseVersion.parse(serverVersion).isGreaterThanOrEqualTo(8, 0, 28));
    }

    @Test
    public void comparesMajorAndMinorBeforePatch() {
        assertTrue(BinaryLogDatabaseVersion.parse("9.0.0").isGreaterThanOrEqualTo(8, 99, 99));
        assertTrue(BinaryLogDatabaseVersion.parse("8.4.0").isGreaterThanOrEqualTo(8, 3, 99));
        assertFalse(BinaryLogDatabaseVersion.parse("8.0.28").isGreaterThanOrEqualTo(8, 1, 0));
        assertFalse(BinaryLogDatabaseVersion.parse("8.0.28").isGreaterThanOrEqualTo(9, 0, 0));
    }

    @Test
    public void preservesExistingConstructorsAndTwoComponentComparisons() {
        BinaryLogDatabaseVersion version = new BinaryLogDatabaseVersion(8, 0, "8.0.28-commercial");
        assertEquals(version.getMajor(), 8);
        assertEquals(version.getMinor(), 0);
        assertTrue(version.isEqualTo(8, 0));
        assertFalse(version.isGreaterThan(8, 0));
        assertTrue(version.isGreaterThan(7, 99));
        assertTrue(version.isGreaterThanOrEqualTo(8, 0));
        assertTrue(version.isGreaterThanOrEqualTo(8, 0, 28));
        assertFalse(version.isGreaterThanOrEqualTo(8, 0, 29));

        version = new BinaryLogDatabaseVersion("8.0.28");
        assertEquals(version.getMajor(), -1);
        assertEquals(version.getMinor(), -1);
        assertFalse(version.isGreaterThanOrEqualTo(8, 0));
        assertFalse(version.isGreaterThanOrEqualTo(8, 0, 28));
    }

    @Test
    public void retainsLegacyMajorAndMinorForIncompleteVersions() {
        BinaryLogDatabaseVersion version = BinaryLogDatabaseVersion.parse("8.4");
        assertEquals(version.getMajor(), 8);
        assertEquals(version.getMinor(), 4);
        assertTrue(version.isGreaterThanOrEqualTo(8, 0));
        assertFalse(version.isGreaterThanOrEqualTo(8, 0, 28));
    }

    @Test
    public void retainsTheMariaDbCompatibilityPrefixAndVendorIdentity() {
        String serverVersion = "5.5.5-10.11.9-MariaDB-log";
        BinaryLogDatabaseVersion version = BinaryLogDatabaseVersion.parse(serverVersion);
        assertTrue(version.isMariaDb());
        assertEquals(version.getMajor(), 5);
        assertEquals(version.getMinor(), 5);
        assertEquals(version.getServerVersion(), serverVersion);
        assertTrue(version.isGreaterThanOrEqualTo(5, 5, 5));
        assertFalse(version.isGreaterThanOrEqualTo(8, 0, 28));

        version = BinaryLogDatabaseVersion.parse("12.3.3-MariaDB-log");
        assertTrue(version.isMariaDb());
        assertTrue(version.isGreaterThanOrEqualTo(12, 3, 3));
    }
}
