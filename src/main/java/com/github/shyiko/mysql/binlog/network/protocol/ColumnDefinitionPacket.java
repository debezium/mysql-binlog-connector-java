package com.github.shyiko.mysql.binlog.network.protocol;

import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Definition of a result set column (Protocol::ColumnDefinition41), of which only the name is read.
 *
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html">
 *     Column Definition</a>
 */
public class ColumnDefinitionPacket implements Packet {

    private final String name;

    public ColumnDefinitionPacket(byte[] bytes) throws IOException {
        ByteArrayInputStream buffer = new ByteArrayInputStream(bytes);
        buffer.readLengthEncodedString(); // catalog
        buffer.readLengthEncodedString(); // schema
        buffer.readLengthEncodedString(); // table
        buffer.readLengthEncodedString(); // org_table
        this.name = buffer.readLengthEncodedString();
    }

    /**
     * @return name of the column, as the query labels it
     */
    public String getName() {
        return name;
    }
}
