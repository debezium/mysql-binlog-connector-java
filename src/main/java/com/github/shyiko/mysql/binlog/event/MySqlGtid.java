package com.github.shyiko.mysql.binlog.event;

import java.util.UUID;

/**
 * Represents a MySQL GTID (Global Transaction Identifier).
 * Supports both legacy format (uuid:transaction_id) and MySQL 8.3+ tagged format (tag:uuid:transaction_id).
 */
public class MySqlGtid {
    private final String tag;
    private final UUID serverId;
    private final long transactionId;

    /**
     * Creates a MySqlGtid without a tag (legacy format).
     *
     * @param serverId the server UUID
     * @param transactionId the transaction ID
     */
    public MySqlGtid(final UUID serverId, final long transactionId) {
        this(null, serverId, transactionId);
    }

    /**
     * Creates a MySqlGtid with an optional tag (MySQL 8.3+ format).
     *
     * @param tag the optional tag (null or empty for legacy format)
     * @param serverId the server UUID
     * @param transactionId the transaction ID
     */
    public MySqlGtid(final String tag, final UUID serverId, final long transactionId) {
        this.tag = tag;
        this.serverId = serverId;
        this.transactionId = transactionId;
    }

    /**
     * Parses a GTID string in either legacy or tagged format.
     * <p>
     * Supported formats:
     * <ul>
     *   <li>Legacy: uuid:transaction_id</li>
     *   <li>Tagged (MySQL 8.3+): tag:uuid:transaction_id</li>
     * </ul>
     *
     * @param gtid the GTID string to parse
     * @return the parsed MySqlGtid
     * @throws IllegalArgumentException if the format is invalid
     */
    public static MySqlGtid fromString(final String gtid) {
        final String[] split = gtid.split(":");

        if (split.length == 2) {
            // Legacy format: uuid:transaction_id
            final String sourceId = split[0];
            final long transactionId = Long.parseLong(split[1]);
            return new MySqlGtid(UUID.fromString(sourceId), transactionId);
        }
        else if (split.length == 3) {
            // Tagged format: tag:uuid:transaction_id
            final String tag = split[0];
            final String sourceId = split[1];
            final long transactionId = Long.parseLong(split[2]);
            return new MySqlGtid(tag, UUID.fromString(sourceId), transactionId);
        }
        else {
            throw new IllegalArgumentException(
                "Invalid GTID format: " + gtid +
                ". Expected format: 'uuid:transaction_id' or 'tag:uuid:transaction_id'"
            );
        }
    }

    @Override
    public String toString() {
        if (tag != null && !tag.isEmpty()) {
            return tag + ":" + serverId.toString() + ":" + transactionId;
        }
        return serverId.toString() + ":" + transactionId;
    }

    /**
     * Gets the optional tag (MySQL 8.3+).
     *
     * @return the tag, or null if not present
     */
    public String getTag() {
        return tag;
    }

    public UUID getServerId() {
        return serverId;
    }

    public long getTransactionId() {
        return transactionId;
    }
}
