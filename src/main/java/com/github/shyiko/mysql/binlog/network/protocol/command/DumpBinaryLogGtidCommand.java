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
package com.github.shyiko.mysql.binlog.network.protocol.command;

import com.github.shyiko.mysql.binlog.GtidSet;
import com.github.shyiko.mysql.binlog.io.ByteArrayOutputStream;

import java.io.IOException;
import java.util.Collection;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class DumpBinaryLogGtidCommand implements Command {

    private long serverId;
    private String binlogFilename;
    private long binlogPosition;
    private GtidSet gtidSet;

    public DumpBinaryLogGtidCommand(long serverId, String binlogFilename, long binlogPosition, GtidSet gtidSet) {
        this.serverId = serverId;
        this.binlogFilename = binlogFilename;
        this.binlogPosition = binlogPosition;
        this.gtidSet = gtidSet;
    }

    @Override
    public byte[] toByteArray() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.writeInteger(CommandType.BINLOG_DUMP_GTID.ordinal(), 1);
        buffer.writeInteger(0, 2); // flag
        buffer.writeLong(this.serverId, 4);
        buffer.writeInteger(this.binlogFilename.length(), 4);
        buffer.writeString(this.binlogFilename);
        buffer.writeLong(this.binlogPosition, 8);
        Collection<GtidSet.UUIDSet> uuidSets = gtidSet.getUUIDSets();
        boolean taggedFormat = containsTaggedGtid(uuidSets);
        int dataSize = 8 /* number of uuidSets */;
        for (GtidSet.UUIDSet uuidSet : uuidSets) {
            dataSize += 16 /* uuid */ + (taggedFormat ? 1 + getTagLength(uuidSet) : 0) + 8 /* number of intervals */ +
                uuidSet.getIntervals().size() /* number of intervals */ * 16 /* start-end */;
        }
        buffer.writeInteger(dataSize, 4);
        buffer.writeLong(getEncodedUuidSetCount(uuidSets.size(), taggedFormat), 8);
        for (GtidSet.UUIDSet uuidSet : uuidSets) {
            buffer.write(hexToByteArray(uuidSet.getUUID().replace("-", "")));
            if (taggedFormat) {
                String tag = uuidSet.getTag();
                buffer.writeInteger(getTagLength(uuidSet) << 1, 1);
                if (tag != null) {
                    buffer.writeString(tag);
                }
            }
            Collection<GtidSet.Interval> intervals = uuidSet.getIntervals();
            buffer.writeLong(intervals.size(), 8);
            for (GtidSet.Interval interval : intervals) {
                buffer.writeLong(interval.getStart(), 8);
                buffer.writeLong(interval.getEnd() + 1 /* right-open */, 8);
            }
        }
        return buffer.toByteArray();
    }

    private static boolean containsTaggedGtid(Collection<GtidSet.UUIDSet> uuidSets) {
        for (GtidSet.UUIDSet uuidSet : uuidSets) {
            if (uuidSet.getTag() != null && !uuidSet.getTag().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private static int getTagLength(GtidSet.UUIDSet uuidSet) {
        String tag = uuidSet.getTag();
        return tag == null ? 0 : tag.length();
    }

    private static long getEncodedUuidSetCount(int uuidSetCount, boolean taggedFormat) {
        if (!taggedFormat) {
            return uuidSetCount;
        }
        return (1L << 56) | ((long) uuidSetCount << 8) | 1L;
    }

    private static byte[] hexToByteArray(String uuid) {
        byte[] b = new byte[uuid.length() / 2];
        for (int i = 0, j = 0; j < uuid.length(); j += 2) {
            b[i++] = (byte) Integer.parseInt(uuid.charAt(j) + "" + uuid.charAt(j + 1), 16);
        }
        return b;
    }

}
