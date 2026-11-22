/*
 * Copyright 2013 Stanley Shyiko
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

import com.github.shyiko.mysql.binlog.io.ByteArrayOutputStream;

import java.io.IOException;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class DumpBinaryLogCommand implements Command {

    /**
     * MariaDB flag requesting annotated row events. The same bit has a different meaning on MySQL.
     */
    public static final int BINLOG_SEND_ANNOTATE_ROWS_EVENT = 2;

    /**
     * MySQL flag requesting HEARTBEAT_LOG_EVENT_V2, supported since MySQL 8.0.28.
     * On MariaDB this bit requests annotated row events instead; callers must select flags for the server dialect.
     */
    public static final int USE_HEARTBEAT_EVENT_V2 = 0x02;

    private long serverId;
    private String binlogFilename;
    private long binlogPosition;
    private final int binlogFlags;

    public DumpBinaryLogCommand(long serverId, String binlogFilename, long binlogPosition) {
        this(serverId, binlogFilename, binlogPosition, 0);
    }

    /**
     * @param serverId replica server id
     * @param binlogFilename binary log filename
     * @param binlogPosition position to start reading
     * @param binlogFlags two-byte flags selected for the server dialect
     */
    public DumpBinaryLogCommand(long serverId, String binlogFilename, long binlogPosition, int binlogFlags) {
        this.serverId = serverId;
        this.binlogFilename = binlogFilename;
        this.binlogPosition = binlogPosition;
        this.binlogFlags = binlogFlags;
    }

    public DumpBinaryLogCommand(long serverId, String binlogFilename, long binlogPosition, boolean sendAnnotateRowsEvent) {
        this(serverId, binlogFilename, binlogPosition, sendAnnotateRowsEvent ? BINLOG_SEND_ANNOTATE_ROWS_EVENT : 0);
    }

    @Override
    public byte[] toByteArray() throws IOException {
        if (binlogPosition < 0 || binlogPosition > 0xffffffffL) {
            throw new BinlogPositionOutOfRangeException(binlogPosition);
        }
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.writeInteger(CommandType.BINLOG_DUMP.ordinal(), 1);
        buffer.writeLong(this.binlogPosition, 4);
        buffer.writeInteger(binlogFlags, 2); // flag
        buffer.writeLong(this.serverId, 4);
        buffer.writeString(this.binlogFilename);
        return buffer.toByteArray();
    }

}
