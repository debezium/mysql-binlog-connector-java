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

import com.github.shyiko.mysql.binlog.event.MySqlGtid;

import java.util.*;

/**
 * GTID set as described in <a href="https://dev.mysql.com/doc/refman/5.6/en/replication-gtids-concepts.html">GTID
 * Concepts</a> of MySQL 5.6 Reference Manual, with support for MySQL 8.3+ tagged GTID intervals.
 *
 * <pre>
 * gtid_set: uuid_set[,uuid_set]...
 * uuid_set: uuid[:tag]:interval[:interval]...
 * uuid: hhhhhhhh-hhhh-hhhh-hhhh-hhhhhhhhhhhh, h: [0-9|A-F]
 * interval: n[-n], (n &gt;= 1)
 * </pre>
 *
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class GtidSet {

    private final Map<Tsid, UUIDSet> map = new LinkedHashMap<Tsid, UUIDSet>();

    public static GtidSet parse(String gtidStr) {
        if ( MariadbGtidSet.isMariaGtidSet(gtidStr) ) {
            return new MariadbGtidSet(gtidStr);
        } else {
            return new GtidSet(gtidStr);
        }
    }
    /**
     * @param gtidSet gtid set comprised of closed intervals (like MySQL's executed_gtid_set).
     * Supports legacy UUID intervals (uuid:intervals), MySQL tagged GTID-set intervals
     * (uuid:tag:intervals), and the previous tag-first representation (tag:uuid:intervals).
     */
    public GtidSet(String gtidSet) {
        String[] uuidSets = (gtidSet == null || gtidSet.isEmpty()) ? new String[0] :
            gtidSet.replace("\n", "").split(",");
        for (String uuidSet : uuidSets) {
            final String[] parts = uuidSet.split(":");

            // MySQL tagged GTID sets identify intervals by TSID (uuid[:tag]).
            // Also accept the previous tag-first representation (tag:uuid:intervals).
            int uuidIndex = 0;
            int intervalsStartIndex = 1;

            // UUID format: 8-4-4-4-12 hex digits with dashes. If the first token is not a UUID,
            // treat it as a tag from the previous tag-first representation.
            if (parts.length >= 3 && !isValidUuidFormat(parts[0])) {
                // Previous tag-first format: tag:uuid:intervals...
                uuidIndex = 1;
                intervalsStartIndex = 2;
            }

            final UUID sourceId = UUID.fromString(parts[uuidIndex]);
            String tag = uuidIndex == 1 ? parts[0] : null;
            for (int i = intervalsStartIndex; i < parts.length; i++) {
                final String part = parts[i];
                if (!isInterval(part)) {
                    tag = part;
                    continue;
                }
                addInterval(sourceId, tag, parseInterval(part));
            }
        }
    }

    private void addInterval(UUID sourceId, String tag, Interval interval) {
        final Tsid tsid = new Tsid(sourceId, tag);
        final UUIDSet existing = map.get(tsid);
        final List<Interval> intervals = existing == null ? new ArrayList<Interval>() :
            new ArrayList<Interval>(existing.getIntervals());
        intervals.add(interval);
        map.put(tsid, new UUIDSet(sourceId, tag, intervals));
    }

    private static Interval parseInterval(final String interval) {
        final String[] is = interval.split("-");
        long[] split = new long[is.length];
        for (int j = 0, e = is.length; j < e; j++) {
            split[j] = Long.parseLong(is[j]);
        }
        if (split.length == 1) {
            split = new long[] {split[0], split[0]};
        }
        return new Interval(split[0], split[1]);
    }

    private static boolean isInterval(final String str) {
        return str.matches("[0-9]+(-[0-9]+)?");
    }

    /**
     * Checks if a string matches the UUID format (8-4-4-4-12 hex digits).
     *
     * @param str the string to check
     * @return true if the string is a valid UUID format, false otherwise
     */
    private static boolean isValidUuidFormat(final String str) {
        // UUID format: 8-4-4-4-12 hex digits with dashes
        return str.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
    }

    /**
     * Get an immutable collection of the {@link UUIDSet range of GTIDs for a single server}.
     * @return the {@link UUIDSet GTID ranges for each server}; never null
     */
    public Collection<UUIDSet> getUUIDSets() {
        return Collections.unmodifiableCollection(map.values());
    }

    /**
     * Find the untagged {@link UUIDSet} for the server with the specified UUID.
     * @param uuid the UUID of the server
     * @return the {@link UUIDSet} for the identified server, or {@code null} if there are no GTIDs from that server.
     */
    public UUIDSet getUUIDSet(String uuid) {
        return map.get(new Tsid(UUID.fromString(uuid), null));
    }

    /**
     * Find the {@link UUIDSet} for the server with the specified UUID and tag.
     * @param uuid the UUID of the server
     * @param tag the GTID tag, or {@code null} for untagged GTIDs
     * @return the {@link UUIDSet} for the identified TSID, or {@code null} if there are no GTIDs for that TSID.
     */
    public UUIDSet getUUIDSet(String uuid, String tag) {
        return map.get(new Tsid(UUID.fromString(uuid), tag));
    }

    /**
     * Add or replace the UUIDSet
     * @param uuidSet UUIDSet to be added
     * @return the old {@link UUIDSet} for the TSID given in uuidSet param,
     *         or {@code null} if there are no UUIDSet for the given TSID.
     */
    public UUIDSet putUUIDSet(UUIDSet uuidSet) {
        return map.put(uuidSet.getTsid(), uuidSet);
    }

    /**
     * @param gtid GTID ("source_id:transaction_id")
     * @return whether or not gtid was added to the set (false if it was already there)
     */
    public boolean add(String gtid) {
        return add(MySqlGtid.fromString(gtid));
    }

    public void addGtid(Object gtid) {
        if (gtid instanceof MySqlGtid) {
            add((MySqlGtid) gtid);
        } else if (gtid instanceof String) {
            add((String) gtid);
        } else {
            throw new IllegalArgumentException(gtid + " not supported");
        }
    }

    private boolean add(MySqlGtid mySqlGtid) {
        final Tsid tsid = new Tsid(mySqlGtid.getServerId(), mySqlGtid.getTag());
        UUIDSet uuidSet = map.get(tsid);
        if (uuidSet == null) {
            map.put(tsid, uuidSet = new UUIDSet(mySqlGtid.getServerId(), mySqlGtid.getTag(),
                new ArrayList<Interval>()));
        }
        return uuidSet.add(mySqlGtid.getTransactionId());
    }

    /**
     * Determine if the GTIDs represented by this object are contained completely within the supplied set of GTIDs.
     * Note that if two {@link GtidSet}s are equal, then they both are subsets of the other.
     * @param other the other set of GTIDs; may be null
     * @return {@code true} if all of the GTIDs in this set are equal to or completely contained within the supplied
     * set of GTIDs, or {@code false} otherwise
     */
    public boolean isContainedWithin(GtidSet other) {
        if (other == null) {
            return false;
        }
        if (this == other) {
            return true;
        }
        if (this.equals(other)) {
            return true;
        }
        for (UUIDSet uuidSet : map.values()) {
            UUIDSet thatSet = other.map.get(uuidSet.getTsid());
            if (!uuidSet.isContainedWithin(thatSet)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return map.keySet().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof GtidSet) {
            GtidSet that = (GtidSet) obj;
            return this.map.equals(that.map);
        }
        return false;
    }

    @Override
    public String toString() {
        List<String> gtids = new ArrayList<String>();
        for (Map.Entry<UUID, List<UUIDSet>> entry : getUUIDSetGroups().entrySet()) {
            StringBuilder sb = new StringBuilder();
            sb.append(entry.getKey()).append(':');
            Iterator<UUIDSet> iter = entry.getValue().iterator();
            if (iter.hasNext()) {
                appendTaggedIntervals(sb, iter.next());
            }
            while (iter.hasNext()) {
                sb.append(':');
                appendTaggedIntervals(sb, iter.next());
            }
            gtids.add(sb.toString());
        }
        return join(gtids, ",");
    }

    public String toSeenString() {
        return this.toString();
    }

    private static String join(Collection<?> o, String delimiter) {
        if (o.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Object o1 : o) {
            sb.append(o1).append(delimiter);
        }
        return sb.substring(0, sb.length() - delimiter.length());
    }

    private Map<UUID, List<UUIDSet>> getUUIDSetGroups() {
        Map<UUID, List<UUIDSet>> groups = new LinkedHashMap<UUID, List<UUIDSet>>();
        for (UUIDSet uuidSet : map.values()) {
            List<UUIDSet> uuidSets = groups.get(uuidSet.getServerId());
            if (uuidSets == null) {
                uuidSets = new ArrayList<UUIDSet>();
                groups.put(uuidSet.getServerId(), uuidSets);
            }
            uuidSets.add(uuidSet);
        }
        // Ensure untagged entry always appears first within each UUID group.
        groups.values().forEach(list ->
            list.sort(Comparator.comparing(u -> u.getTag() == null ? "" : u.getTag())));
        return groups;
    }

    private static void appendTaggedIntervals(StringBuilder sb, UUIDSet uuidSet) {
        if (uuidSet.getTag() != null) {
            sb.append(uuidSet.getTag()).append(':');
        }
        sb.append(join(uuidSet.intervals, ":"));
    }

    private static final class Tsid {

        private final UUID uuid;
        private final String tag;

        private Tsid(UUID uuid, String tag) {
            this.uuid = uuid;
            this.tag = tag == null || tag.isEmpty() ? null : tag;
        }

        @Override
        public int hashCode() {
            return 31 * uuid.hashCode() + (tag == null ? 0 : tag.hashCode());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof Tsid) {
                Tsid that = (Tsid) obj;
                return this.uuid.equals(that.uuid) && Objects.equals(this.tag, that.tag);
            }
            return false;
        }
    }

    /**
     * A range of GTIDs for a single server with a specific UUID.
     * @see GtidSet
     */
    public static final class UUIDSet {

        private final UUID uuid;
        private final String tag;
        private final List<Interval> intervals;

        public UUIDSet(String uuid, List<Interval> intervals) {
            this(UUID.fromString(uuid), intervals);
        }

        public UUIDSet(UUID uuid, List<Interval> intervals) {
            this(uuid, null, intervals);
        }

        public UUIDSet(String uuid, String tag, List<Interval> intervals) {
            this(UUID.fromString(uuid), tag, intervals);
        }

        public UUIDSet(UUID uuid, String tag, List<Interval> intervals) {
            this.uuid = uuid;
            this.tag = tag == null || tag.isEmpty() ? null : tag;
            this.intervals = intervals;
            if (intervals.size() > 1) {
                Collections.sort(intervals);
                joinAdjacentIntervals();
            }
        }

        private boolean add(long transactionId) {
            int index = findInterval(transactionId);
            boolean addedToExisting = false;
            if (index < intervals.size()) {
                Interval interval = intervals.get(index);
                if (interval.start == transactionId + 1) {
                    interval.start = transactionId;
                    addedToExisting = true;
                } else
                if (interval.end + 1 == transactionId) {
                    interval.end = transactionId;
                    addedToExisting = true;
                } else
                if (interval.start <= transactionId && transactionId <= interval.end) {
                    return false;
                }
            }
            if (!addedToExisting) {
                intervals.add(index, new Interval(transactionId, transactionId));
            }
            if (intervals.size() > 1) {
                joinAdjacentIntervals(index);
            }
            return true;
        }

        /**
         * Collapses adjacent or overlapping intervals near the supplied index.
         */
        private void joinAdjacentIntervals(int index) {
            for (int i = Math.min(index + 1, intervals.size() - 1), e = Math.max(index - 1, 0); i > e; i--) {
                Interval a = intervals.get(i - 1), b = intervals.get(i);
                if (a.end + 1 >= b.start) {
                    a.end = Math.max(a.end, b.end);
                    intervals.remove(i);
                }
            }
        }

        private void joinAdjacentIntervals() {
            for (int i = intervals.size() - 1; i > 0; i--) {
                Interval a = intervals.get(i - 1), b = intervals.get(i);
                if (a.end + 1 >= b.start) {
                    a.end = Math.max(a.end, b.end);
                    intervals.remove(i);
                }
            }
        }

        /**
         * @return index which is either a pointer to the interval containing v or a position at which v can be added
         */
        private int findInterval(long v) {
            int l = 0, p = 0, r = intervals.size();
            while (l < r) {
                p = (l + r) / 2;
                Interval i = intervals.get(p);
                if (i.end < v) {
                    l = p + 1;
                } else
                if (v < i.start) {
                    r = p;
                } else {
                    return p;
                }
            }
            if (!intervals.isEmpty() && intervals.get(p).end < v) {
                p++;
            }
            return p;
        }

        /**
         * Get the UUID for the server that generated the GTIDs.
         * @return the server's UUID; never null
         */
        @Deprecated
        public String getUUID() {
            return uuid.toString();
        }

        public UUID getServerId() {
            return uuid;
        }

        public String getTag() {
            return tag;
        }

        private Tsid getTsid() {
            return new Tsid(uuid, tag);
        }


        /**
         * Get the intervals of transaction numbers.
         * @return the immutable transaction intervals; never null
         */
        public List<Interval> getIntervals() {
            return Collections.unmodifiableList(intervals);
        }

        /**
         * Determine if the set of transaction numbers from this server is completely within the set of transaction
         * numbers from the set of transaction numbers in the supplied set.
         * @param other the set to compare with this set
         * @return {@code true} if this server's transaction numbers are equal to or a subset of the transaction
         * numbers of the supplied set, or false otherwise
         */
        public boolean isContainedWithin(UUIDSet other) {
            if (other == null) {
                return false;
            }
            if (!this.uuid.equals(other.uuid)) {
                // not even the same server ...
                return false;
            }
            if (!Objects.equals(this.tag, other.tag)) {
                return false;
            }
            if (this.intervals.isEmpty()) {
                return true;
            }
            if (other.intervals.isEmpty()) {
                return false;
            }
            // every interval in this must be within an interval of the other ...
            for (Interval thisInterval : this.intervals) {
                boolean found = false;
                for (Interval otherInterval : other.intervals) {
                    if (thisInterval.isContainedWithin(otherInterval)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return false; // didn't find a match
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return 31 * uuid.hashCode() + (tag == null ? 0 : tag.hashCode());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof UUIDSet) {
                UUIDSet that = (UUIDSet) obj;
                return this.uuid.equals(that.uuid) &&
                    Objects.equals(this.tag, that.tag) &&
                    this.getIntervals().equals(that.getIntervals());
            }
            return super.equals(obj);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (sb.length() != 0) {
                sb.append(',');
            }
            sb.append(uuid).append(':');
            if (tag != null) {
                sb.append(tag).append(':');
            }
            Iterator<Interval> iter = intervals.iterator();
            if (iter.hasNext()) {
                sb.append(iter.next());
            }
            while (iter.hasNext()) {
                sb.append(':');
                sb.append(iter.next());
            }
            return sb.toString();
        }
    }

    /**
     * An interval of contiguous transaction identifiers.
     * @see GtidSet
     */
    public static final class Interval implements Comparable<Interval> {

        private long start;
        private long end;

        public Interval(long start, long end) {
            this.start = start;
            this.end = end;
        }

        /**
         * Get the starting transaction number in this interval.
         * @return this interval's first transaction number
         */
        public long getStart() {
            return start;
        }

        /**
         * Get the ending transaction number in this interval.
         * @return this interval's last transaction number
         */
        public long getEnd() {
            return end;
        }

        /**
         * Determine if this interval is completely within the supplied interval.
         * @param other the interval to compare with
         * @return {@code true} if the {@link #getStart() start} is greater than or equal to the supplied interval's
         * {@link #getStart() start} and the {@link #getEnd() end} is less than or equal to the supplied
         * interval's {@link #getEnd() end}, or {@code false} otherwise
         */
        public boolean isContainedWithin(Interval other) {
            if (other == this) {
                return true;
            }
            if (other == null) {
                return false;
            }
            return this.getStart() >= other.getStart() && this.getEnd() <= other.getEnd();
        }

        @Override
        public int hashCode() {
            return (int) getStart();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof Interval) {
                Interval that = (Interval) obj;
                return this.getStart() == that.getStart() && this.getEnd() == that.getEnd();
            }
            return false;
        }

        @Override
        public String toString() {
            return start + "-" + end;
        }

        @Override
        public int compareTo(Interval o) {
            return saturatedCast(this.start - o.start);
        }

        private static int saturatedCast(long value) {
            if (value > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
            if (value < Integer.MIN_VALUE) {
                return Integer.MIN_VALUE;
            }
            return (int) value;
        }
    }

}
